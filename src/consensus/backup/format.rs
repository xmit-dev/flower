//! Backup objects and their names. Every object ends with the SHA-256 of
//! the bytes before it, so a torn or altered object is refused.
//!
//! A segment holds consecutive log entries, each as the JSON the log stores
//! (unpacked, so that compression spans entries): `FLOWERSEG1\n`, a JSON
//! header's length (u32 LE) and the header, then the entries in blocks as a
//! base has them (below). Uncompressed, each entry is its index (u64 LE), the
//! time it was applied (Unix ms, u64 LE), its length (u32 LE) and its JSON.
//!
//! A base is a whole state as a snapshot transfer encodes it (the JSON of
//! the stored state), in zstd blocks of up to 4 MiB: `FLOWERBASE1\n`, the
//! header as above, then per block its raw and compressed lengths (u32 LE
//! each) and the compressed bytes (one zstd frame), a block of lengths 0 and
//! 0, and the raw length in all (u64 LE).
use std::io::Write;

use anyhow::{Context, bail, ensure};
use aws_lc_rs::digest;
use openraft::{BasicNode, LogId, StoredMembership};
use serde::{Deserialize, Serialize};

const SEGMENT_MAGIC: &[u8] = b"FLOWERSEG1\n";
const BASE_MAGIC: &[u8] = b"FLOWERBASE1\n";
const HASH_BYTES: usize = 32;
// Raw bytes per block, and what compressing them may take at most (zstd's
// bound is below an extra 1/128).
const BLOCK_BYTES: usize = 4 << 20;
const MAX_COMPRESSED_BYTES: usize = BLOCK_BYTES + (BLOCK_BYTES >> 7) + 4096;
// zstd's default level: about LZ4's speed on JSON, at half the size or less.
const LEVEL: i32 = 3;
const MAX_HEADER_BYTES: usize = 1 << 20;

/// The compatibility contract of whoever wrote an object: what reading it
/// requires of a binary.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Contract {
    pub raft_wire: u32,
    pub state_machine: u32,
    pub snapshot_format: u32,
    pub value_format: u32,
}

impl Contract {
    pub fn current() -> Self {
        let current = crate::consensus::membership::compatibility();
        Self {
            raft_wire: current.raft_wire,
            state_machine: current.state_machine,
            snapshot_format: current.snapshot_format,
            value_format: current.value_format,
        }
    }

    /// Whether this binary can restore what `self` wrote: the same snapshot
    /// and value formats, and commands no newer than its own.
    pub fn check_readable(&self, what: &str) -> anyhow::Result<()> {
        let current = Self::current();
        ensure!(
            self.snapshot_format == current.snapshot_format
                && self.value_format == current.value_format
                && self.state_machine <= current.state_machine,
            "{what} was written under contract state{}-snapshot{}-value{}, which this binary \
             (state{}-snapshot{}-value{}) cannot restore; use a binary of that contract",
            self.state_machine,
            self.snapshot_format,
            self.value_format,
            current.state_machine,
            current.snapshot_format,
            current.value_format
        );
        Ok(())
    }
}

/// What starts a generation: `generations/{id}/generation.json`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Generation {
    pub id: String,
    pub created: u64,
    pub node: u64,
    /// The newest generation when this one began.
    pub previous: Option<String>,
    /// Why it began: `empty`, `new`, `continuity` (the last one could not
    /// be continued) or `restored`.
    pub reason: String,
    /// The backup point a restored node started from.
    #[serde(default)]
    pub restored_from: Option<serde_json::Value>,
    pub contract: Contract,
}

/// The newest shipped entry of a generation, written every few seconds:
/// `generations/{id}/tip.json`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Tip {
    pub generation: String,
    pub index: u64,
    pub at: u64,
    pub log_id: Option<LogId<u64>>,
    pub sha256: Option<String>,
    /// The segment holding it, if one does.
    pub segment: Option<String>,
    pub written: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct SegmentHeader {
    pub generation: String,
    pub first: u64,
    pub last: u64,
    pub first_at: u64,
    pub last_at: u64,
    /// The last entry's log ID and the SHA-256 of its JSON, which a later
    /// leader compares with its own log to continue the generation.
    pub last_log_id: LogId<u64>,
    pub last_sha256: String,
    pub contract: Contract,
    pub node: u64,
}

/// A log entry as a segment holds it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Record {
    pub index: u64,
    /// When it was applied (Unix ms).
    pub at: u64,
    /// Its JSON as the log stores it (packed).
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct BaseHeader {
    pub generation: String,
    pub last_log_id: Option<LogId<u64>>,
    pub membership: StoredMembership<u64, BasicNode>,
    /// When the state was captured (Unix ms): it holds every entry applied
    /// by then.
    pub at: u64,
    /// The SHA-256 of the JSON of the entry at `last_log_id`, when the log
    /// still held it.
    pub last_sha256: Option<String>,
    pub contract: Contract,
    pub node: u64,
}

pub(super) fn sha256_hex(bytes: &[u8]) -> String {
    super::hex(digest::digest(&digest::SHA256, bytes).as_ref())
}

/// A segment's object, and the size of its entries unpacked.
pub(super) fn encode_segment(
    header: &SegmentHeader,
    records: &[Record],
) -> anyhow::Result<(Vec<u8>, usize)> {
    let header_json = serde_json::to_vec(header)?;
    let mut raw = Vec::with_capacity(
        records
            .iter()
            .map(|record| 20 + record.bytes.len())
            .sum::<usize>(),
    );
    for record in records {
        raw.extend_from_slice(&record.index.to_le_bytes());
        raw.extend_from_slice(&record.at.to_le_bytes());
        raw.extend_from_slice(&u32::try_from(record.bytes.len())?.to_le_bytes());
        raw.extend_from_slice(&record.bytes);
    }
    let mut bytes = Vec::with_capacity(SEGMENT_MAGIC.len() + 4 + header_json.len() + raw.len() / 2);
    bytes.extend_from_slice(SEGMENT_MAGIC);
    bytes.extend_from_slice(&u32::try_from(header_json.len())?.to_le_bytes());
    bytes.extend_from_slice(&header_json);
    for block in raw.chunks(BLOCK_BYTES) {
        let compressed = zstd::bulk::compress(block, LEVEL).context("compress a backup segment")?;
        bytes.extend_from_slice(&(block.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&(compressed.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&compressed);
    }
    bytes.extend_from_slice(&[0; 8]);
    let hash = digest::digest(&digest::SHA256, &bytes);
    bytes.extend_from_slice(hash.as_ref());
    Ok((bytes, raw.len()))
}

pub(super) fn decode_segment(bytes: &[u8]) -> anyhow::Result<(SegmentHeader, Vec<Record>)> {
    let body = verified(bytes, SEGMENT_MAGIC, "segment")?;
    let mut reader = Reader(body);
    let header: SegmentHeader = reader.header()?;
    let mut raw = Vec::new();
    loop {
        let (length, compressed) = (reader.u32()? as usize, reader.u32()? as usize);
        if (length, compressed) == (0, 0) {
            break;
        }
        ensure!(
            length > 0
                && length <= BLOCK_BYTES
                && compressed > 0
                && compressed <= MAX_COMPRESSED_BYTES,
            "backup segment block has invalid lengths"
        );
        let block = zstd::bulk::decompress(reader.take(compressed)?, length)
            .context("backup segment block does not decompress")?;
        ensure!(
            block.len() == length,
            "backup segment block length mismatch"
        );
        raw.extend_from_slice(&block);
    }
    ensure!(
        reader.0.is_empty(),
        "backup segment has bytes after its end"
    );
    let mut reader = Reader(&raw);
    let mut records = Vec::new();
    while !reader.0.is_empty() {
        let index = reader.u64()?;
        let at = reader.u64()?;
        let length = reader.u32()? as usize;
        records.push(Record {
            index,
            at,
            bytes: reader.take(length)?.to_vec(),
        });
    }
    ensure!(
        header.last >= header.first
            && records.len() as u64 == header.last - header.first + 1
            && records
                .iter()
                .enumerate()
                .all(|(position, record)| record.index == header.first + position as u64),
        "segment {}-{} does not hold exactly those entries",
        header.first,
        header.last
    );
    Ok((header, records))
}

/// The bytes between the magic and the hash, once both are right.
fn verified<'a>(bytes: &'a [u8], magic: &[u8], what: &str) -> anyhow::Result<&'a [u8]> {
    ensure!(
        bytes.len() >= magic.len() + HASH_BYTES && bytes.starts_with(magic),
        "not a Flower backup {what}"
    );
    let (content, hash) = bytes.split_at(bytes.len() - HASH_BYTES);
    ensure!(
        digest::digest(&digest::SHA256, content).as_ref() == hash,
        "backup {what} is damaged: its SHA-256 does not match"
    );
    Ok(&content[magic.len()..])
}

struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn take(&mut self, length: usize) -> anyhow::Result<&'a [u8]> {
        let (taken, rest) = self
            .0
            .split_at_checked(length)
            .context("truncated backup object")?;
        self.0 = rest;
        Ok(taken)
    }

    fn u32(&mut self) -> anyhow::Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into()?))
    }

    fn u64(&mut self) -> anyhow::Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into()?))
    }

    fn header<T: serde::de::DeserializeOwned>(&mut self) -> anyhow::Result<T> {
        let length = self.u32()? as usize;
        ensure!(length <= MAX_HEADER_BYTES, "backup object header too large");
        serde_json::from_slice(self.take(length)?).context("decode backup object header")
    }
}

/// Writes a base: give it the state's JSON through `Write`, take what it has
/// encoded so far with `take`, and the rest with `finish`.
pub(super) struct BaseEncoder {
    block: Vec<u8>,
    output: Vec<u8>,
    hash: digest::Context,
    raw: u64,
}

impl BaseEncoder {
    pub fn new(header: &BaseHeader) -> anyhow::Result<Self> {
        let header_json = serde_json::to_vec(header)?;
        let mut encoder = Self {
            block: Vec::with_capacity(BLOCK_BYTES),
            output: Vec::new(),
            hash: digest::Context::new(&digest::SHA256),
            raw: 0,
        };
        encoder.emit(BASE_MAGIC);
        encoder.emit(&u32::try_from(header_json.len())?.to_le_bytes());
        encoder.emit(&header_json);
        Ok(encoder)
    }

    fn emit(&mut self, bytes: &[u8]) {
        self.hash.update(bytes);
        self.output.extend_from_slice(bytes);
    }

    fn seal_block(&mut self) -> std::io::Result<()> {
        if self.block.is_empty() {
            return Ok(());
        }
        let compressed = zstd::bulk::compress(&self.block, LEVEL)?;
        self.emit(&(self.block.len() as u32).to_le_bytes());
        self.emit(&(compressed.len() as u32).to_le_bytes());
        self.emit(&compressed);
        self.raw += self.block.len() as u64;
        self.block.clear();
        Ok(())
    }

    /// Encoded bytes not yet taken.
    pub fn pending(&self) -> usize {
        self.output.len()
    }

    pub fn take(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.output)
    }

    /// The raw bytes so far.
    pub fn raw(&self) -> u64 {
        self.raw + self.block.len() as u64
    }

    pub fn finish(mut self) -> std::io::Result<Vec<u8>> {
        self.seal_block()?;
        self.emit(&0u32.to_le_bytes());
        self.emit(&0u32.to_le_bytes());
        let raw = self.raw;
        self.emit(&raw.to_le_bytes());
        let hash = self.hash.clone().finish();
        self.output.extend_from_slice(hash.as_ref());
        Ok(self.output)
    }
}

impl Write for BaseEncoder {
    fn write(&mut self, mut data: &[u8]) -> std::io::Result<usize> {
        let written = data.len();
        while !data.is_empty() {
            let room = BLOCK_BYTES - self.block.len();
            let (now, later) = data.split_at(room.min(data.len()));
            self.block.extend_from_slice(now);
            if self.block.len() == BLOCK_BYTES {
                self.seal_block()?;
            }
            data = later;
        }
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Reads a base as it arrives: `feed` it the object's bytes and it writes
/// the state's JSON to `out`; `finish` checks that the object was whole.
pub(super) struct BaseDecoder {
    buffer: Vec<u8>,
    hash: digest::Context,
    stage: Stage,
    header: Option<BaseHeader>,
    raw: u64,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Stage {
    Magic,
    HeaderLength,
    Header(usize),
    BlockLengths,
    Block { raw: usize, compressed: usize },
    Total,
    Hash,
    Done,
}

impl Default for BaseDecoder {
    fn default() -> Self {
        Self {
            buffer: Vec::new(),
            hash: digest::Context::new(&digest::SHA256),
            stage: Stage::Magic,
            header: None,
            raw: 0,
        }
    }
}

impl BaseDecoder {
    pub fn header(&self) -> Option<&BaseHeader> {
        self.header.as_ref()
    }

    pub fn feed(&mut self, mut data: &[u8], out: &mut impl Write) -> anyhow::Result<()> {
        loop {
            let needed = match self.stage {
                Stage::Magic => BASE_MAGIC.len(),
                Stage::HeaderLength => 4,
                Stage::Header(length) => length,
                Stage::BlockLengths => 8,
                Stage::Block { compressed, .. } => compressed,
                Stage::Total => 8,
                Stage::Hash => HASH_BYTES,
                Stage::Done => {
                    ensure!(data.is_empty(), "backup base has bytes after its end");
                    return Ok(());
                }
            };
            // Take what completes the current stage, keeping it in the
            // buffer only when it arrives in pieces.
            let have = self.buffer.len();
            if have + data.len() < needed {
                self.buffer.extend_from_slice(data);
                return Ok(());
            }
            let (now, later) = data.split_at(needed - have);
            data = later;
            let piece: std::borrow::Cow<'_, [u8]> = if have == 0 {
                std::borrow::Cow::Borrowed(now)
            } else {
                self.buffer.extend_from_slice(now);
                std::borrow::Cow::Owned(std::mem::take(&mut self.buffer))
            };
            if self.stage != Stage::Hash {
                self.hash.update(&piece);
            }
            self.stage = match self.stage {
                Stage::Magic => {
                    ensure!(*piece == *BASE_MAGIC, "not a Flower backup base");
                    Stage::HeaderLength
                }
                Stage::HeaderLength => {
                    let length = u32::from_le_bytes(piece[..].try_into()?) as usize;
                    ensure!(length <= MAX_HEADER_BYTES, "backup base header too large");
                    Stage::Header(length)
                }
                Stage::Header(_) => {
                    self.header =
                        Some(serde_json::from_slice(&piece).context("decode backup base header")?);
                    Stage::BlockLengths
                }
                Stage::BlockLengths => {
                    let raw = u32::from_le_bytes(piece[..4].try_into()?) as usize;
                    let compressed = u32::from_le_bytes(piece[4..].try_into()?) as usize;
                    match (raw, compressed) {
                        (0, 0) => Stage::Total,
                        _ => {
                            ensure!(
                                raw > 0
                                    && raw <= BLOCK_BYTES
                                    && compressed > 0
                                    && compressed <= MAX_COMPRESSED_BYTES,
                                "backup base block has invalid lengths"
                            );
                            Stage::Block { raw, compressed }
                        }
                    }
                }
                Stage::Block { raw, .. } => {
                    let block = zstd::bulk::decompress(&piece, raw)
                        .context("backup base block does not decompress")?;
                    ensure!(block.len() == raw, "backup base block length mismatch");
                    out.write_all(&block)?;
                    self.raw += raw as u64;
                    Stage::BlockLengths
                }
                Stage::Total => {
                    let total = u64::from_le_bytes(piece[..].try_into()?);
                    ensure!(total == self.raw, "backup base is missing blocks");
                    Stage::Hash
                }
                Stage::Hash => {
                    ensure!(
                        self.hash.clone().finish().as_ref() == &piece[..],
                        "backup base is damaged: its SHA-256 does not match"
                    );
                    Stage::Done
                }
                Stage::Done => unreachable!("handled above"),
            };
        }
    }

    /// The header, once the whole base has been read and checked.
    pub fn finish(self) -> anyhow::Result<(BaseHeader, u64)> {
        if self.stage != Stage::Done {
            bail!("backup base is truncated");
        }
        Ok((
            self.header.context("backup base without a header")?,
            self.raw,
        ))
    }
}

/// `{created:013}-{random:016x}`: generations sort by when they began.
pub(super) fn generation_id(created: u64) -> String {
    let mut random = [0u8; 8];
    let _ = getrandom::fill(&mut random);
    format!("{created:013}-{}", super::hex(&random))
}

pub(super) fn generation_created(id: &str) -> Option<u64> {
    let (created, random) = id.split_once('-')?;
    (created.len() == 13 && random.len() == 16).then_some(())?;
    created.parse().ok()
}

pub(super) fn generation_key(generation: &str) -> String {
    format!("generations/{generation}/generation.json")
}

pub(super) fn tip_key(generation: &str) -> String {
    format!("generations/{generation}/tip.json")
}

pub(super) fn segments_prefix(generation: &str) -> String {
    format!("generations/{generation}/log/")
}

pub(super) fn bases_prefix(generation: &str) -> String {
    format!("generations/{generation}/bases/")
}

pub(super) fn segment_key(generation: &str, first: u64, last: u64, last_at: u64) -> String {
    format!(
        "{}{first:020}-{last:020}-{last_at:013}.seg",
        segments_prefix(generation)
    )
}

/// The key after which a listing starts at segments beginning at `first`.
pub(super) fn segments_from(generation: &str, first: u64) -> String {
    format!("{}{first:020}", segments_prefix(generation))
}

/// A segment key's first and last index and its last entry's time.
pub(super) fn parse_segment_key(key: &str) -> Option<(u64, u64, u64)> {
    let name = key.rsplit('/').next()?.strip_suffix(".seg")?;
    let mut parts = name.split('-');
    let first = parts.next()?.parse().ok()?;
    let last = parts.next()?.parse().ok()?;
    let at = parts.next()?.parse().ok()?;
    parts.next().is_none().then_some((first, last, at))
}

pub(super) fn base_key(generation: &str, index: u64, at: u64) -> String {
    format!("{}{index:020}-{at:013}.base", bases_prefix(generation))
}

/// A base key's index and capture time.
pub(super) fn parse_base_key(key: &str) -> Option<(u64, u64)> {
    let name = key.rsplit('/').next()?.strip_suffix(".base")?;
    let (index, at) = name.split_once('-')?;
    Some((index.parse().ok()?, at.parse().ok()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn log_id(term: u64, index: u64) -> LogId<u64> {
        LogId::new(openraft::CommittedLeaderId::new(term, 1), index)
    }

    #[test]
    fn segments_round_trip_and_refuse_damage() {
        let records: Vec<Record> = (5..8)
            .map(|index| Record {
                index,
                at: 1_000 + index,
                bytes: format!("{{\"entry\":{index}}}").into_bytes(),
            })
            .collect();
        let header = SegmentHeader {
            generation: "g".into(),
            first: 5,
            last: 7,
            first_at: 1_005,
            last_at: 1_007,
            last_log_id: log_id(2, 7),
            last_sha256: sha256_hex(b"{\"entry\":7}"),
            contract: Contract::current(),
            node: 1,
        };
        let (bytes, raw) = encode_segment(&header, &records).unwrap();
        assert_eq!(
            raw,
            records
                .iter()
                .map(|record| 20 + record.bytes.len())
                .sum::<usize>()
        );
        let (decoded, read) = decode_segment(&bytes).unwrap();
        assert_eq!(read, records);
        assert_eq!(decoded.last_log_id, log_id(2, 7));
        for damaged in [
            bytes[..bytes.len() - 1].to_vec(),
            {
                let mut flipped = bytes.clone();
                flipped[40] ^= 1;
                flipped
            },
            [bytes.as_slice(), b"x"].concat(),
        ] {
            assert!(decode_segment(&damaged).is_err());
        }
        // A header that names other entries than it holds is refused.
        let wrong = SegmentHeader {
            last: 8,
            ..header.clone()
        };
        assert!(decode_segment(&encode_segment(&wrong, &records).unwrap().0).is_err());

        // Entries span blocks, and JSON packs small.
        let records: Vec<Record> = (1..=12_000)
            .map(|index| Record {
                index,
                at: 1_000 + index,
                bytes: format!(
                    "{{\"entry\":{index},\"payload\":\"{}\"}}",
                    "flower ".repeat(100)
                )
                .into_bytes(),
            })
            .collect();
        let header = SegmentHeader {
            first: 1,
            last: 12_000,
            ..header
        };
        let (bytes, raw) = encode_segment(&header, &records).unwrap();
        assert!(raw > 2 * BLOCK_BYTES, "{raw}");
        assert!(bytes.len() * 10 < raw, "{} of {raw}", bytes.len());
        assert_eq!(decode_segment(&bytes).unwrap().1, records);
    }

    #[test]
    fn bases_round_trip_in_any_pieces_and_refuse_damage() {
        let header = BaseHeader {
            generation: "g".into(),
            last_log_id: Some(log_id(3, 40)),
            membership: StoredMembership::default(),
            at: 99,
            last_sha256: None,
            contract: Contract::current(),
            node: 2,
        };
        // Three and a half blocks of JSON-like text.
        let pattern = b"{\"k\":[1,2,3],\"v\":\"flower\"}";
        let raw: Vec<u8> = (0..(BLOCK_BYTES * 7 / 2))
            .map(|n| pattern[n % pattern.len()])
            .collect();
        let mut encoder = BaseEncoder::new(&header).unwrap();
        let mut object = Vec::new();
        for piece in raw.chunks(300_001) {
            encoder.write_all(piece).unwrap();
            if encoder.pending() > 1000 {
                object.extend(encoder.take());
            }
        }
        assert_eq!(encoder.raw(), raw.len() as u64);
        object.extend(encoder.finish().unwrap());
        assert!(object.len() < raw.len() / 10, "{} bytes", object.len());
        for piece in [1, 7, 4096, 1 << 20, object.len()] {
            let mut decoder = BaseDecoder::default();
            let mut out = Vec::new();
            for chunk in object.chunks(piece) {
                decoder.feed(chunk, &mut out).unwrap();
            }
            let (read, total) = decoder.finish().unwrap();
            assert_eq!(total, raw.len() as u64);
            assert_eq!(read.last_log_id, Some(log_id(3, 40)));
            assert!(out == raw);
        }
        let decode = |bytes: &[u8]| -> anyhow::Result<()> {
            let mut decoder = BaseDecoder::default();
            decoder.feed(bytes, &mut std::io::sink())?;
            decoder.finish().map(|_| ())
        };
        assert!(decode(&object[..object.len() - 1]).is_err());
        assert!(decode(&[object.as_slice(), b"x"].concat()).is_err());
        let mut flipped = object.clone();
        let middle = flipped.len() / 2;
        flipped[middle] ^= 0x10;
        assert!(decode(&flipped).is_err());
    }

    #[test]
    fn keys_sort_by_index_and_parse_back() {
        let generation = generation_id(1_790_000_000_000);
        assert_eq!(generation_created(&generation), Some(1_790_000_000_000));
        assert_eq!(generation.len(), 30);
        let segment = segment_key(&generation, 9, 12, 1_790_000_000_123);
        assert_eq!(
            parse_segment_key(&segment),
            Some((9, 12, 1_790_000_000_123))
        );
        assert!(segment.as_str() > segments_from(&generation, 9).as_str());
        assert!(
            segment_key(&generation, 8, 8, 1).as_str() < segments_from(&generation, 9).as_str()
        );
        assert!(segment < segment_key(&generation, 10, 10, 0));
        assert!(segment_key(&generation, 99, 99, 0) < segment_key(&generation, 100, 100, 0));
        let base = base_key(&generation, 40, 1_790_000_000_000);
        assert_eq!(parse_base_key(&base), Some((40, 1_790_000_000_000)));
        assert!(parse_segment_key("generations/x/log/1-2.seg").is_none());
    }
}
