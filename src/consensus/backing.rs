//! Stored application records beneath an in-memory overlay: one table of a
//! pinned redb read snapshot. A stored value is its write's version, eight
//! little-endian bytes, followed by its JSON, packed (`super::packed`).
use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::fmt;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::ops::Bound;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::{Arc, Mutex};

use redb::{AccessGuard, ReadOnlyTable, ReadableTableMetadata};
use serde_json::Value;

use super::cache::VALUES;

pub(crate) type PlainTable = ReadOnlyTable<&'static [u8], &'static [u8]>;
pub(crate) type PairTable = ReadOnlyTable<(&'static [u8], &'static [u8]), &'static [u8]>;

/// Records served from disk. Reads cannot fail recoverably mid-evaluation:
/// a storage error is as fatal here as in the persistence task.
pub struct Backing {
    table: Table,
    // Counting a partition's slice reads it all, so only when asked.
    len: std::sync::OnceLock<usize>,
    // A database no store owns, kept open for as long as its snapshot.
    _database: Option<Arc<redb::Database>>,
}

enum Table {
    /// Nothing stored yet: records held in memory until their first write
    /// is persisted, whose deletions must still leave tombstones.
    Empty,
    Plain(PlainTable),
    /// One logical partition's slice of a table keyed by (partition, key),
    /// shared by every partition's backing.
    Pair(Arc<PairTable>, String),
    /// Records spooled in key order to a file.
    Run(Arc<Run>),
}

impl fmt::Debug for Backing {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Backing")
            .field("len", &self.len.get())
            .finish_non_exhaustive()
    }
}

/// The persistence batch of an in-memory write not yet assigned to one. A
/// rebase onto a backing that holds every batch drops it too.
pub(crate) const UNSEQUENCED: u64 = u64::MAX;

pub(crate) fn encode(version: u64, json: &[u8]) -> Vec<u8> {
    let body = super::packed::pack(json);
    let mut bytes = Vec::with_capacity(8 + body.len());
    bytes.extend_from_slice(&version.to_le_bytes());
    bytes.extend_from_slice(&body);
    bytes
}

/// A stored value's version, and its body: its JSON, packed.
fn split(bytes: &[u8]) -> (u64, &[u8]) {
    let (version, body) = bytes.split_at(8);
    (
        u64::from_le_bytes(version.try_into().expect("stored version")),
        body,
    )
}

fn unpack(body: &[u8]) -> Cow<'_, [u8]> {
    super::packed::unpack(body).expect("stored record")
}

/// A stored value: its version followed by its JSON.
enum Bytes {
    Table(AccessGuard<'static, &'static [u8]>),
    Run(Vec<u8>),
}

impl Bytes {
    fn value(&self) -> &[u8] {
        match self {
            Self::Table(guard) => guard.value(),
            Self::Run(bytes) => bytes,
        }
    }
}

/// One stored record found by a range read, parsed only on demand.
pub(crate) struct Stored {
    pub key: String,
    bytes: Bytes,
}

impl Stored {
    /// Its version followed by its JSON, as stored.
    pub(crate) fn encoded(&self) -> &[u8] {
        self.bytes.value()
    }

    pub(crate) fn version(&self) -> u64 {
        split(self.bytes.value()).0
    }

    pub(crate) fn json(&self) -> Cow<'_, [u8]> {
        unpack(split(self.bytes.value()).1)
    }

    pub(crate) fn value(&self) -> Arc<Value> {
        let (version, body) = split(self.bytes.value());
        VALUES.get_or_parse(version, || unpack(body))
    }
}

impl Backing {
    pub(crate) fn empty() -> Self {
        Self {
            table: Table::Empty,
            len: std::sync::OnceLock::from(0),
            _database: None,
        }
    }

    pub(crate) fn plain(table: PlainTable) -> Self {
        let len = table.len().expect("stored record count") as usize;
        Self {
            table: Table::Plain(table),
            len: std::sync::OnceLock::from(len),
            _database: None,
        }
    }

    pub(crate) fn pair(table: Arc<PairTable>, partition: String) -> Self {
        Self {
            table: Table::Pair(table, partition),
            len: std::sync::OnceLock::new(),
            _database: None,
        }
    }

    /// Stored records for tests: `records` written into a table of an
    /// in-memory database, with their versions.
    #[cfg(test)]
    pub(crate) fn in_memory<'a>(
        records: impl IntoIterator<Item = (&'a str, u64, Vec<u8>)>,
    ) -> Self {
        use redb::ReadableDatabase;
        const TABLE: redb::TableDefinition<&[u8], &[u8]> = redb::TableDefinition::new("data");
        let database = redb::Database::builder()
            .create_with_backend(redb::backends::InMemoryBackend::new())
            .expect("in-memory database");
        let transaction = database.begin_write().expect("write transaction");
        {
            let mut table = transaction.open_table(TABLE).expect("data table");
            for (key, version, json) in records {
                table
                    .insert(key.as_bytes(), encode(version, &json).as_slice())
                    .expect("stored record");
            }
        }
        transaction.commit().expect("commit");
        let table = database
            .begin_read()
            .expect("read transaction")
            .open_table(TABLE)
            .expect("data table");
        let mut backing = Self::plain(table);
        backing._database = Some(Arc::new(database));
        backing
    }

    pub(crate) fn len(&self) -> usize {
        *self.len.get_or_init(|| match &self.table {
            // redb keeps a table's length in its root.
            Table::Plain(table) => table.len().expect("stored record count") as usize,
            _ => self.range(Bound::Unbounded, Bound::Unbounded).count(),
        })
    }

    fn raw(&self, key: &str) -> Option<Bytes> {
        match &self.table {
            Table::Empty => None,
            Table::Plain(table) => table
                .get(key.as_bytes())
                .expect("stored record read")
                .map(Bytes::Table),
            Table::Pair(table, partition) => table
                .get((partition.as_bytes(), key.as_bytes()))
                .expect("stored record read")
                .map(Bytes::Table),
            Table::Run(run) => run.get(key).map(Bytes::Run),
        }
    }

    pub(crate) fn get(&self, key: &str) -> Option<(u64, Arc<Value>)> {
        self.raw(key).map(|bytes| {
            let (version, body) = split(bytes.value());
            (version, VALUES.get_or_parse(version, || unpack(body)))
        })
    }

    pub(crate) fn version(&self, key: &str) -> Option<u64> {
        self.raw(key).map(|bytes| split(bytes.value()).0)
    }

    pub(crate) fn contains(&self, key: &str) -> bool {
        self.raw(key).is_some()
    }

    /// Stored records within the bounds, in key order from either end.
    pub(crate) fn range(
        &self,
        lower: Bound<&str>,
        upper_bound: Bound<&str>,
    ) -> Box<dyn DoubleEndedIterator<Item = Stored> + Send + '_> {
        match &self.table {
            Table::Empty => Box::new(std::iter::empty()),
            Table::Plain(table) => Box::new(
                table
                    .range::<&[u8]>((lower.map(str::as_bytes), upper_bound.map(str::as_bytes)))
                    .expect("stored record range")
                    .map(|entry| {
                        let (key, bytes) = entry.expect("stored record range");
                        Stored {
                            key: stored_key(key.value()),
                            bytes: Bytes::Table(bytes),
                        }
                    }),
            ),
            Table::Pair(table, partition) => {
                let after = upper(partition);
                let partition = partition.as_bytes();
                let lower = match lower {
                    Bound::Included(key) => Bound::Included((partition, key.as_bytes())),
                    Bound::Excluded(key) => Bound::Excluded((partition, key.as_bytes())),
                    Bound::Unbounded => Bound::Included((partition, &b""[..])),
                };
                let upper_bound = match upper_bound {
                    Bound::Included(key) => Bound::Included((partition, key.as_bytes())),
                    Bound::Excluded(key) => Bound::Excluded((partition, key.as_bytes())),
                    Bound::Unbounded => Bound::Excluded((after.as_bytes(), &b""[..])),
                };
                Box::new(
                    table
                        .range::<(&[u8], &[u8])>((lower, upper_bound))
                        .expect("stored partition range")
                        .map(|entry| {
                            let (key, bytes) = entry.expect("stored partition range");
                            Stored {
                                key: stored_key(key.value().1),
                                bytes: Bytes::Table(bytes),
                            }
                        }),
                )
            }
            Table::Run(run) => Box::new(RunRange::new(run.clone(), lower, upper_bound)),
        }
    }
}

// Records per indexed block of a spooled run.
const RUN_STRIDE: usize = 64;

thread_local! {
    static SPOOL: RefCell<Option<Arc<Spool>>> = const { RefCell::new(None) };
}

/// While active on a thread, records and receipts decoded there are written
/// to a temporary file and served from it, so decoding a state larger than
/// memory holds one record at a time. Each map arrives in key order and is
/// appended as one run, with every `RUN_STRIDE`th key indexed in memory, which
/// costs far less than building a B-tree. The file is deleted once nothing
/// serves from it.
pub(crate) struct Spool {
    file: Arc<File>,
    out: Mutex<Appender>,
}

struct Appender {
    writer: BufWriter<File>,
    position: u64,
}

/// Ends spooling on this thread when dropped. Spooled records stay served.
pub(crate) struct Spooling(());

impl Drop for Spooling {
    fn drop(&mut self) {
        SPOOL.with(|spool| spool.borrow_mut().take());
    }
}

impl Spool {
    /// Spool what this thread decodes into a file in `directory`.
    pub(crate) fn activate(directory: &Path) -> anyhow::Result<Spooling> {
        let file = tempfile::tempfile_in(directory)?;
        let spool = Self {
            out: Mutex::new(Appender {
                writer: BufWriter::with_capacity(1 << 20, file.try_clone()?),
                position: 0,
            }),
            file: Arc::new(file),
        };
        SPOOL.with(|current| *current.borrow_mut() = Some(Arc::new(spool)));
        Ok(Spooling(()))
    }

    pub(crate) fn current() -> Option<Arc<Self>> {
        SPOOL.with(|spool| spool.borrow().clone())
    }

    /// A new run of this spool.
    pub(crate) fn writer(self: &Arc<Self>) -> SpoolWriter {
        SpoolWriter {
            spool: self.clone(),
            index: Vec::new(),
            len: 0,
            last: None,
        }
    }
}

pub(crate) struct SpoolWriter {
    spool: Arc<Spool>,
    index: Vec<(String, u64)>,
    len: usize,
    last: Option<String>,
}

impl SpoolWriter {
    /// Append a record, whose key must follow the previous one's.
    pub(crate) fn insert(&mut self, key: String, version: u64, json: &[u8]) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.last.as_ref().is_none_or(|last| *last < key),
            "snapshot records are not in strictly increasing key order"
        );
        let key_len = u32::try_from(key.len())?;
        let value = encode(version, json);
        let value_len = u32::try_from(value.len())?;
        let mut out = self.spool.out.lock().expect("spool lock");
        if self.len % RUN_STRIDE == 0 {
            self.index.push((key.clone(), out.position));
        }
        out.writer.write_all(&key_len.to_le_bytes())?;
        out.writer.write_all(key.as_bytes())?;
        out.writer.write_all(&value_len.to_le_bytes())?;
        out.writer.write_all(&value)?;
        out.position += 8 + u64::from(key_len) + u64::from(value_len);
        self.len += 1;
        self.last = Some(key);
        Ok(())
    }

    /// The spooled records, served from the spool.
    pub(crate) fn finish(self) -> anyhow::Result<Backing> {
        let mut out = self.spool.out.lock().expect("spool lock");
        out.writer.flush()?;
        let run = Run {
            file: self.spool.file.clone(),
            index: self.index,
            end: out.position,
        };
        Ok(Backing {
            table: Table::Run(Arc::new(run)),
            len: std::sync::OnceLock::from(self.len),
            _database: None,
        })
    }
}

/// Records appended in key order to a spool file, in blocks of
/// `RUN_STRIDE` whose first keys and offsets are indexed.
pub(crate) struct Run {
    file: Arc<File>,
    index: Vec<(String, u64)>,
    end: u64,
}

impl Run {
    /// The records of block `block`, in order.
    fn block(&self, block: usize) -> Vec<(String, Vec<u8>)> {
        let start = self.index[block].1;
        let stop = self
            .index
            .get(block + 1)
            .map_or(self.end, |(_, offset)| *offset);
        let mut bytes = vec![0; usize::try_from(stop - start).expect("spool block size")];
        self.file
            .read_exact_at(&mut bytes, start)
            .expect("spool read");
        let mut records = Vec::with_capacity(RUN_STRIDE);
        let mut rest = bytes.as_slice();
        let take = |rest: &mut &[u8]| {
            let (length, tail) = rest.split_at(4);
            let length = u32::from_le_bytes(length.try_into().expect("spool length")) as usize;
            let (field, tail) = tail.split_at(length);
            *rest = tail;
            field.to_vec()
        };
        while !rest.is_empty() {
            let key = String::from_utf8(take(&mut rest)).expect("spool key");
            records.push((key, take(&mut rest)));
        }
        records
    }

    /// The block that would hold `key`, if any.
    fn block_of(&self, key: &str) -> Option<usize> {
        self.index
            .partition_point(|(first, _)| first.as_str() <= key)
            .checked_sub(1)
    }

    fn get(&self, key: &str) -> Option<Vec<u8>> {
        let block = self.block_of(key)?;
        self.block(block)
            .into_iter()
            .find_map(|(found, bytes)| (found == key).then_some(bytes))
    }
}

/// A run's records within bounds, a block at a time from either end.
struct RunRange {
    run: Arc<Run>,
    lower: Bound<String>,
    upper: Bound<String>,
    // Blocks not yet read: [front, back).
    front: usize,
    back: usize,
    front_records: VecDeque<Stored>,
    back_records: VecDeque<Stored>,
}

impl RunRange {
    fn new(run: Arc<Run>, lower: Bound<&str>, upper: Bound<&str>) -> Self {
        let front = match lower {
            Bound::Included(key) | Bound::Excluded(key) => run.block_of(key).unwrap_or(0),
            Bound::Unbounded => 0,
        };
        let back = match upper {
            Bound::Included(key) => run
                .index
                .partition_point(|(first, _)| first.as_str() <= key),
            Bound::Excluded(key) => run.index.partition_point(|(first, _)| first.as_str() < key),
            Bound::Unbounded => run.index.len(),
        };
        Self {
            lower: lower.map(str::to_owned),
            upper: upper.map(str::to_owned),
            front,
            back: back.max(front),
            front_records: VecDeque::new(),
            back_records: VecDeque::new(),
            run,
        }
    }

    fn within(&self, key: &str) -> bool {
        (match &self.lower {
            Bound::Included(lower) => key >= lower.as_str(),
            Bound::Excluded(lower) => key > lower.as_str(),
            Bound::Unbounded => true,
        }) && (match &self.upper {
            Bound::Included(upper) => key <= upper.as_str(),
            Bound::Excluded(upper) => key < upper.as_str(),
            Bound::Unbounded => true,
        })
    }

    fn load(&self, block: usize) -> VecDeque<Stored> {
        self.run
            .block(block)
            .into_iter()
            .filter(|(key, _)| self.within(key))
            .map(|(key, bytes)| Stored {
                key,
                bytes: Bytes::Run(bytes),
            })
            .collect()
    }
}

impl Iterator for RunRange {
    type Item = Stored;

    fn next(&mut self) -> Option<Stored> {
        loop {
            if let Some(record) = self.front_records.pop_front() {
                return Some(record);
            }
            if self.front == self.back {
                return self.back_records.pop_front();
            }
            self.front_records = self.load(self.front);
            self.front += 1;
        }
    }
}

impl DoubleEndedIterator for RunRange {
    fn next_back(&mut self) -> Option<Stored> {
        loop {
            if let Some(record) = self.back_records.pop_back() {
                return Some(record);
            }
            if self.front == self.back {
                return self.front_records.pop_back();
            }
            self.back -= 1;
            self.back_records = self.load(self.back);
        }
    }
}

/// A stored key, checked as UTF-8 once it is read.
fn stored_key(bytes: &[u8]) -> String {
    String::from_utf8(bytes.to_vec()).expect("stored record key")
}

/// The least partition name after every key of `partition`.
fn upper(partition: &str) -> String {
    format!("{partition}\0")
}

/// An in-memory entry over a backing: a value, or the deletion of the
/// backing's, with the version of its write.
pub(crate) trait Slot {
    fn live(&self) -> bool;
    fn version(&self) -> u64;
}

/// A record in its stored form, to write into a table: encoded from memory,
/// or the backing's own bytes.
pub(crate) enum Encoded {
    Tree(String, Vec<u8>),
    Stored(Stored),
}

impl Encoded {
    pub(crate) fn key(&self) -> &str {
        match self {
            Self::Tree(key, _) => key,
            Self::Stored(stored) => &stored.key,
        }
    }

    pub(crate) fn bytes(&self) -> &[u8] {
        match self {
            Self::Tree(_, bytes) => bytes,
            Self::Stored(stored) => stored.encoded(),
        }
    }
}

/// A record found by a merged read: from the tree, or from the backing.
pub(crate) enum Found<'a, V> {
    Tree(&'a String, &'a V),
    Stored(Stored),
}

impl<V: Slot> Found<'_, V> {
    pub(crate) fn key(&self) -> &str {
        match self {
            Self::Tree(key, _) => key,
            Self::Stored(stored) => &stored.key,
        }
    }

    fn live(&self) -> bool {
        match self {
            Self::Tree(_, entry) => entry.live(),
            Self::Stored(_) => true,
        }
    }

    pub(crate) fn version(&self) -> u64 {
        match self {
            Self::Tree(_, entry) => entry.version(),
            Self::Stored(stored) => stored.version(),
        }
    }
}

/// Tree and backing records in key order, from either end, where the tree
/// shadows the backing: a deletion hides the backing's record.
pub(crate) struct Merge<'a, V, A, B> {
    tree: A,
    stored: B,
    tree_front: Option<(&'a String, &'a V)>,
    tree_back: Option<(&'a String, &'a V)>,
    stored_front: Option<Stored>,
    stored_back: Option<Stored>,
}

impl<'a, V, A, B> Merge<'a, V, A, B>
where
    A: DoubleEndedIterator<Item = (&'a String, &'a V)>,
    B: DoubleEndedIterator<Item = Stored>,
{
    pub(crate) fn new(tree: A, stored: B) -> Self {
        Self {
            tree,
            stored,
            tree_front: None,
            tree_back: None,
            stored_front: None,
            stored_back: None,
        }
    }
}

impl<'a, V: Slot, A, B> Iterator for Merge<'a, V, A, B>
where
    A: DoubleEndedIterator<Item = (&'a String, &'a V)>,
    B: DoubleEndedIterator<Item = Stored>,
{
    type Item = Found<'a, V>;

    fn next(&mut self) -> Option<Found<'a, V>> {
        loop {
            if self.tree_front.is_none() {
                self.tree_front = self.tree.next().or_else(|| self.tree_back.take());
            }
            if self.stored_front.is_none() {
                self.stored_front = self.stored.next().or_else(|| self.stored_back.take());
            }
            let found = match (&self.tree_front, &self.stored_front) {
                (None, None) => return None,
                (Some(_), None) => {
                    let (key, entry) = self.tree_front.take().expect("tree record");
                    Found::Tree(key, entry)
                }
                (None, Some(_)) => Found::Stored(self.stored_front.take().expect("stored record")),
                (Some((key, _)), Some(stored)) => match key.as_str().cmp(stored.key.as_str()) {
                    std::cmp::Ordering::Greater => {
                        Found::Stored(self.stored_front.take().expect("stored record"))
                    }
                    ordering => {
                        if ordering == std::cmp::Ordering::Equal {
                            self.stored_front = None;
                        }
                        let (key, entry) = self.tree_front.take().expect("tree record");
                        Found::Tree(key, entry)
                    }
                },
            };
            if found.live() {
                return Some(found);
            }
        }
    }
}

impl<'a, V: Slot, A, B> DoubleEndedIterator for Merge<'a, V, A, B>
where
    A: DoubleEndedIterator<Item = (&'a String, &'a V)>,
    B: DoubleEndedIterator<Item = Stored>,
{
    fn next_back(&mut self) -> Option<Found<'a, V>> {
        loop {
            if self.tree_back.is_none() {
                self.tree_back = self.tree.next_back().or_else(|| self.tree_front.take());
            }
            if self.stored_back.is_none() {
                self.stored_back = self.stored.next_back().or_else(|| self.stored_front.take());
            }
            let found = match (&self.tree_back, &self.stored_back) {
                (None, None) => return None,
                (Some(_), None) => {
                    let (key, entry) = self.tree_back.take().expect("tree record");
                    Found::Tree(key, entry)
                }
                (None, Some(_)) => Found::Stored(self.stored_back.take().expect("stored record")),
                (Some((key, _)), Some(stored)) => match key.as_str().cmp(stored.key.as_str()) {
                    std::cmp::Ordering::Less => {
                        Found::Stored(self.stored_back.take().expect("stored record"))
                    }
                    ordering => {
                        if ordering == std::cmp::Ordering::Equal {
                            self.stored_back = None;
                        }
                        let (key, entry) = self.tree_back.take().expect("tree record");
                        Found::Tree(key, entry)
                    }
                },
            };
            if found.live() {
                return Some(found);
            }
        }
    }
}
