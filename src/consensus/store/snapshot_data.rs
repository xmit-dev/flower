//! Raft transfer images are encoded only when a peer reads them, and read
//! as they encode: a sender that reads from the start streams the image
//! without a temporary file. Asking for its size materializes one, as does
//! seeking past what streaming can serve. Capturing an image only retains
//! immutable state roots; it does no encoding.
use super::{StoragePhase, StorageTrace, StoredState, snapshots::encode_state};
use std::collections::VecDeque;
use std::future::Future;
use std::io::{self, SeekFrom, Write};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, ready};
use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite, ReadBuf};
use tokio::sync::mpsc;

#[derive(Clone)]
pub(super) struct DeferredImage {
    pub state: Arc<StoredState>,
    pub directory: PathBuf,
    pub node: u64,
}

// Bytes per streamed block, and blocks the encoder may run ahead.
const STREAM_BLOCK_BYTES: usize = 256 * 1024;
const STREAM_AHEAD_BLOCKS: usize = 8;

enum Data {
    Deferred(Option<DeferredImage>),
    Streaming(Stream),
    Preparing(tokio::task::JoinHandle<io::Result<std::fs::File>>),
    File(tokio::fs::File),
    Failed(io::ErrorKind, String),
}

/// Raft snapshot transport with lazy encoding of immutable state. Received
/// snapshots are ordinary writable temporary files.
pub struct SnapshotData {
    data: Data,
    seek: Option<SeekFrom>,
    read_only: bool,
}

impl std::fmt::Debug for SnapshotData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never include application state or errors in consensus debug output.
        f.debug_struct("SnapshotData")
            .field("materialized", &matches!(self.data, Data::File(_)))
            .finish()
    }
}

/// An image being encoded by a blocking task, read as its blocks arrive.
/// It keeps the blocks from the latest seek on, so that a sender can resend
/// the segment it last read; seeking earlier encodes the image again.
struct Stream {
    image: DeferredImage,
    blocks: mpsc::Receiver<io::Result<Vec<u8>>>,
    // Received blocks still kept, from `kept`, and where the next one starts.
    history: VecDeque<Vec<u8>>,
    kept: u64,
    produced: u64,
    // Where the next read starts.
    cursor: u64,
    finished: bool,
}

impl Stream {
    fn start(image: DeferredImage) -> Self {
        let (sender, blocks) = mpsc::channel(STREAM_AHEAD_BLOCKS);
        let encoded = image.clone();
        tokio::task::spawn_blocking(move || {
            let mut profile = StorageTrace::new(encoded.node, "snapshot_transfer");
            let span = profile.span();
            let _entered = span.enter();
            profile.phase(StoragePhase::BlockingQueue);
            let mut writer = BlockWriter {
                sender,
                block: Vec::with_capacity(STREAM_BLOCK_BYTES),
            };
            let result = serde_json::to_writer(&mut writer, &*encoded.state)
                .map_err(io::Error::other)
                .and_then(|()| writer.flush());
            profile.phase(StoragePhase::Prepare);
            profile.report(result.is_ok());
            // A reader that went away needs no error.
            if let Err(error) = result {
                let _ = writer.sender.blocking_send(Err(error));
            }
        });
        Self {
            image,
            blocks,
            history: VecDeque::new(),
            kept: 0,
            produced: 0,
            cursor: 0,
            finished: false,
        }
    }

    /// Move the read position, or `None` if only encoding again reaches it.
    fn seek(&mut self, position: u64) -> Option<()> {
        if position < self.kept {
            return None;
        }
        // Blocks wholly before the new position are no longer needed.
        while let Some(block) = self.history.front()
            && self.kept + block.len() as u64 <= position.min(self.produced)
        {
            self.kept += block.len() as u64;
            self.history.pop_front();
        }
        self.cursor = position;
        Some(())
    }

    fn poll_read(&mut self, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        loop {
            if self.cursor < self.produced {
                let mut start = self.kept;
                for block in &self.history {
                    let end = start + block.len() as u64;
                    if self.cursor < end {
                        let from = (self.cursor - start) as usize;
                        let n = (block.len() - from).min(buf.remaining());
                        buf.put_slice(&block[from..from + n]);
                        self.cursor += n as u64;
                        return Poll::Ready(Ok(()));
                    }
                    start = end;
                }
                unreachable!("a position before the produced end is kept");
            }
            if self.finished {
                return Poll::Ready(Ok(()));
            }
            match ready!(self.blocks.poll_recv(cx)) {
                Some(Ok(block)) => {
                    self.produced += block.len() as u64;
                    self.history.push_back(block);
                    // Blocks the reader skipped past go right away.
                    let cursor = self.cursor;
                    self.seek(cursor).expect("a forward position");
                }
                Some(Err(error)) => return Poll::Ready(Err(error)),
                None => self.finished = true,
            }
        }
    }
}

struct BlockWriter {
    sender: mpsc::Sender<io::Result<Vec<u8>>>,
    block: Vec<u8>,
}

impl Write for BlockWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.block.extend_from_slice(bytes);
        if self.block.len() >= STREAM_BLOCK_BYTES {
            self.flush()?;
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.block.is_empty() {
            return Ok(());
        }
        let block = std::mem::replace(&mut self.block, Vec::with_capacity(STREAM_BLOCK_BYTES));
        self.sender
            .blocking_send(Ok(block))
            .map_err(|_| io::Error::other("snapshot reader went away"))
    }
}

impl SnapshotData {
    pub(super) fn deferred(image: DeferredImage) -> Self {
        Self {
            data: Data::Deferred(Some(image)),
            seek: None,
            read_only: true,
        }
    }

    pub(super) fn from_std(file: std::fs::File) -> Self {
        Self {
            data: Data::File(tokio::fs::File::from_std(file)),
            seek: None,
            read_only: false,
        }
    }

    /// Encode the image into a temporary file, for random access.
    fn materialize(&mut self) {
        let image = match &mut self.data {
            Data::Deferred(image) => image.take().expect("deferred snapshot image"),
            Data::Streaming(stream) => stream.image.clone(),
            _ => return,
        };
        self.data = Data::Preparing(tokio::task::spawn_blocking(move || {
            let mut profile = StorageTrace::new(image.node, "snapshot_transfer");
            let span = profile.span();
            let _entered = span.enter();
            profile.phase(StoragePhase::BlockingQueue);
            let result = encode_state(&image.directory, &image.state, &mut profile);
            profile.phase(StoragePhase::Prepare);
            profile.report(result.is_ok());
            result.map_err(io::Error::other)
        }));
    }

    fn poll_file(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<&mut tokio::fs::File>> {
        loop {
            match &mut self.data {
                Data::Deferred(_) | Data::Streaming(_) => self.materialize(),
                Data::Preparing(task) => {
                    let result = ready!(Pin::new(task).poll(cx))
                        .map_err(io::Error::other)
                        .and_then(|v| v);
                    self.data = match result {
                        Ok(file) => Data::File(tokio::fs::File::from_std(file)),
                        Err(error) => Data::Failed(error.kind(), error.to_string()),
                    };
                }
                Data::Failed(kind, message) => {
                    return Poll::Ready(Err(io::Error::new(*kind, message.clone())));
                }
                Data::File(_) => break,
            }
        }
        let Data::File(file) = &mut self.data else {
            unreachable!()
        };
        Poll::Ready(Ok(file))
    }

    /// Complete a pending seek, streaming where the position allows it.
    fn poll_position(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        // Tokio completes any earlier seek before starting one.
        match (&self.data, self.seek) {
            (Data::Deferred(_), None) => return Poll::Ready(Ok(0)),
            (Data::Streaming(stream), None) => return Poll::Ready(Ok(stream.cursor)),
            _ => {}
        }
        if let Some(target) = self.seek {
            let streamed = match (&mut self.data, target) {
                (Data::Deferred(image), SeekFrom::Start(position)) => {
                    let mut stream = Stream::start(image.take().expect("deferred snapshot image"));
                    stream.cursor = position;
                    self.data = Data::Streaming(stream);
                    Some(position)
                }
                (Data::Deferred(image), SeekFrom::Current(0)) => {
                    self.data = Data::Streaming(Stream::start(image.take().expect("deferred snapshot image")));
                    Some(0)
                }
                (Data::Streaming(stream), SeekFrom::Start(position)) => {
                    if stream.seek(position).is_none() {
                        // Before what it kept: encode from the start again.
                        let mut restarted = Stream::start(stream.image.clone());
                        restarted.cursor = position;
                        *stream = restarted;
                    }
                    Some(position)
                }
                (Data::Streaming(stream), SeekFrom::Current(0)) => Some(stream.cursor),
                _ => None,
            };
            if let Some(position) = streamed {
                self.seek = None;
                return Poll::Ready(Ok(position));
            }
        }
        let position = self.seek;
        let file = ready!(self.poll_file(cx))?;
        if let Some(position) = position {
            Pin::new(file).start_seek(position)?;
            self.seek = None;
        }
        let file = ready!(self.poll_file(cx))?;
        Pin::new(file).poll_complete(cx)
    }

    pub(super) async fn into_std(mut self) -> io::Result<std::fs::File> {
        std::future::poll_fn(|cx| self.poll_position(cx)).await?;
        std::future::poll_fn(|cx| self.poll_file(cx).map_ok(|_| ())).await?;
        let Data::File(file) = self.data else {
            unreachable!()
        };
        Ok(file.into_std().await)
    }
}

impl AsyncRead for SnapshotData {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.seek.is_some() {
            ready!(self.poll_position(cx))?;
        }
        if let Data::Deferred(image) = &mut self.data {
            self.data = Data::Streaming(Stream::start(image.take().expect("deferred snapshot image")));
        }
        if let Data::Streaming(stream) = &mut self.data {
            let result = ready!(stream.poll_read(cx, buf));
            if let Err(error) = &result {
                self.data = Data::Failed(error.kind(), error.to_string());
            }
            return Poll::Ready(result);
        }
        let file = ready!(self.poll_file(cx))?;
        Pin::new(file).poll_read(cx, buf)
    }
}

impl AsyncSeek for SnapshotData {
    fn start_seek(mut self: Pin<&mut Self>, position: SeekFrom) -> io::Result<()> {
        if self.seek.is_some() {
            return Err(io::Error::other("snapshot seek already in progress"));
        }
        self.seek = Some(position);
        Ok(())
    }
    fn poll_complete(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        self.poll_position(cx)
    }
}

impl AsyncWrite for SnapshotData {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.read_only {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "immutable snapshot image",
            )));
        }
        if self.seek.is_some() {
            ready!(self.poll_position(cx))?;
        }
        let file = ready!(self.poll_file(cx))?;
        Pin::new(file).poll_write(cx, bytes)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // An image nobody wrote to has nothing to flush.
        if self.read_only && !matches!(self.data, Data::File(_)) {
            return Poll::Ready(Ok(()));
        }
        let file = ready!(self.poll_file(cx))?;
        Pin::new(file).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.read_only && !matches!(self.data, Data::File(_)) {
            return Poll::Ready(Ok(()));
        }
        let file = ready!(self.poll_file(cx))?;
        Pin::new(file).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

    #[tokio::test]
    async fn deferred_stream_encodes_only_on_io_and_preserves_seek_and_independent_cursors() {
        let directory = tempfile::tempdir().unwrap();
        let mut state = StoredState::default();
        state
            .application
            .data
            .insert("value".into(), serde_json::json!("flower".repeat(100_000)));
        let bytes = serde_json::to_vec(&state).unwrap();
        let state = Arc::new(state);
        let make = || {
            SnapshotData::deferred(DeferredImage {
                state: state.clone(),
                directory: directory.path().into(),
                node: 1,
            })
        };
        let mut first = make();
        let mut second = make();
        assert!(matches!(first.data, Data::Deferred(_)));
        assert_eq!(
            first.write(b"no").await.unwrap_err().kind(),
            io::ErrorKind::PermissionDenied
        );
        assert!(matches!(first.data, Data::Deferred(_)));
        assert_eq!(
            first.seek(SeekFrom::End(0)).await.unwrap(),
            bytes.len() as u64
        );
        first.seek(SeekFrom::Start(7)).await.unwrap();
        let mut tail = Vec::new();
        first.read_to_end(&mut tail).await.unwrap();
        assert_eq!(tail, bytes[7..]);
        assert!(matches!(second.data, Data::Deferred(_)));
        let mut all = Vec::new();
        second.read_to_end(&mut all).await.unwrap();
        assert_eq!(all, bytes);
        first.rewind().await.unwrap();
        let mut prefix = [0; 7];
        first.read_exact(&mut prefix).await.unwrap();
        assert_eq!(prefix, bytes[..7]);
    }

    #[tokio::test]
    async fn failed_encoding_stays_an_error_and_debug_never_prints_state() {
        let directory = tempfile::tempdir().unwrap();
        let mut state = StoredState::default();
        state
            .application
            .data
            .insert("private-value".into(), serde_json::json!("secret-payload"));
        let mut data = SnapshotData::deferred(DeferredImage {
            state: Arc::new(state),
            directory: directory.path().join("missing"),
            node: 1,
        });
        assert!(!format!("{data:?}").contains("secret"));
        assert!(data.seek(SeekFrom::End(0)).await.is_err());
        assert!(data.read_u8().await.is_err());
        assert!(!format!("{data:?}").contains("secret"));
    }

    /// A sender reading segments from the start streams the image without a
    /// temporary file, resends the segment it last read, and can restart.
    #[tokio::test]
    async fn sequential_segments_stream_without_a_file_and_survive_resends() {
        let directory = tempfile::tempdir().unwrap();
        let mut state = StoredState::default();
        for n in 0..40 {
            state
                .application
                .data
                .insert(format!("value-{n:02}"), serde_json::json!(format!("{n}").repeat(40_000)));
        }
        let bytes = serde_json::to_vec(&state).unwrap();
        let mut data = SnapshotData::deferred(DeferredImage {
            state: Arc::new(state),
            directory: directory.path().into(),
            node: 1,
        });
        let segment = 100_003;
        let mut read = Vec::new();
        let mut offset = 0u64;
        let mut step = 0;
        loop {
            data.seek(SeekFrom::Start(offset)).await.unwrap();
            let mut buf = Vec::with_capacity(segment);
            while buf.len() < segment {
                if (&mut data).take((segment - buf.len()) as u64).read_to_end(&mut buf).await.unwrap() == 0 {
                    break;
                }
            }
            step += 1;
            if step % 3 == 0 {
                // A failed send reads the same segment again.
                continue;
            }
            if step == 10 {
                // A mismatch starts the transfer over.
                read.clear();
                offset = 0;
                continue;
            }
            assert_eq!(buf, bytes[offset as usize..offset as usize + buf.len()]);
            read.extend_from_slice(&buf);
            if buf.len() < segment {
                break;
            }
            offset += buf.len() as u64;
        }
        assert_eq!(read, bytes);
        assert!(matches!(data.data, Data::Streaming(_)));
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    }
}
