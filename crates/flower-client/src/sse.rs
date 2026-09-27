//! A bounded incremental SSE decoder: the twin of `readSse` in `sdk/watch.ts`.
//!
//! Fatal UTF-8 (a leading BOM is dropped like `TextDecoder` does), CR/LF/CRLF line endings including
//! a CRLF split across chunks, `:` comments, the event name defaulting to `message`, ids containing
//! NUL ignored, a bare CR charged as two bytes, and a byte budget per event covering comments and
//! blank lines too.

use std::collections::VecDeque;

/// One dispatched SSE event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SseFrame {
    pub event: String,
    pub data: String,
    pub id: Option<String>,
}

/// Why the stream is invalid; the watch reports it as `WATCH_PROTOCOL_ERROR`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SseError(pub &'static str);

pub const BYTE_LIMIT: &str = "Watch event exceeds its byte limit";
pub const INVALID_UTF8: &str = "Watch stream contains invalid UTF-8";
pub const ENDED_DURING_EVENT: &str = "Watch stream ended during an event";

const BOM: &[u8] = b"\xEF\xBB\xBF";

#[derive(Debug)]
pub struct SseParser {
    max_bytes: usize,
    line: Vec<u8>,
    event: String,
    id: Option<String>,
    data: Vec<String>,
    bytes: usize,
    after_cr: bool,
    /// Bytes of an incomplete UTF-8 character at the end of the last chunk.
    carry: Vec<u8>,
    /// The first bytes, held until we know whether they start with a BOM.
    head: Option<Vec<u8>>,
}

impl SseParser {
    pub fn new(max_bytes: usize) -> Self {
        SseParser {
            max_bytes,
            line: Vec::new(),
            event: String::new(),
            id: None,
            data: Vec::new(),
            bytes: 0,
            after_cr: false,
            carry: Vec::new(),
            head: Some(Vec::new()),
        }
    }

    /// Feed a chunk. Frames completed before an error are still appended to `frames`.
    pub fn push(&mut self, chunk: &[u8], frames: &mut VecDeque<SseFrame>) -> Result<(), SseError> {
        if let Some(mut head) = self.head.take() {
            head.extend_from_slice(chunk);
            if head.len() < BOM.len() && BOM.starts_with(&head) {
                self.head = Some(head);
                return Ok(());
            }
            let start = if head.starts_with(BOM) { BOM.len() } else { 0 };
            return self.feed(&head[start..], frames);
        }
        self.feed(chunk, frames)
    }

    /// The stream ended cleanly: fail if it stopped inside a character or an event.
    pub fn finish(&mut self) -> Result<(), SseError> {
        if let Some(head) = self.head.take() {
            // Fewer than three bytes, all a BOM prefix: TextDecoder reports them as invalid.
            if !head.is_empty() {
                return Err(SseError(INVALID_UTF8));
            }
        }
        if !self.carry.is_empty() {
            return Err(SseError(INVALID_UTF8));
        }
        if !self.line.is_empty() || !self.data.is_empty() || !self.event.is_empty() {
            return Err(SseError(ENDED_DURING_EVENT));
        }
        Ok(())
    }

    fn validate(&mut self, chunk: &[u8]) -> Result<(), SseError> {
        let mut start = 0;
        while !self.carry.is_empty() && start < chunk.len() {
            self.carry.push(chunk[start]);
            start += 1;
            match std::str::from_utf8(&self.carry) {
                Ok(_) => self.carry.clear(),
                Err(error) if error.error_len().is_none() => {}
                Err(_) => return Err(SseError(INVALID_UTF8)),
            }
        }
        if !self.carry.is_empty() {
            return Ok(());
        }
        match std::str::from_utf8(&chunk[start..]) {
            Ok(_) => Ok(()),
            Err(error) if error.error_len().is_none() => {
                self.carry
                    .extend_from_slice(&chunk[start + error.valid_up_to()..]);
                Ok(())
            }
            Err(_) => Err(SseError(INVALID_UTF8)),
        }
    }

    fn feed(&mut self, chunk: &[u8], frames: &mut VecDeque<SseFrame>) -> Result<(), SseError> {
        // Like the TS, decode (and so validate) 64 KiB at a time before splitting lines.
        for piece in chunk.chunks(65536) {
            self.validate(piece)?;
            let mut offset = 0;
            for (index, &byte) in piece.iter().enumerate() {
                if self.after_cr {
                    self.after_cr = false;
                    if byte == b'\n' {
                        offset = index + 1;
                        continue;
                    }
                }
                if byte != b'\r' && byte != b'\n' {
                    continue;
                }
                self.fragment(&piece[offset..index])?;
                offset = index + 1;
                self.after_cr = byte == b'\r';
                if let Some(frame) = self.end_line(self.after_cr)? {
                    frames.push_back(frame);
                }
            }
            self.fragment(&piece[offset..])?;
        }
        Ok(())
    }

    fn fragment(&mut self, text: &[u8]) -> Result<(), SseError> {
        self.bytes += text.len();
        if self.bytes > self.max_bytes {
            return Err(SseError(BYTE_LIMIT));
        }
        self.line.extend_from_slice(text);
        Ok(())
    }

    fn end_line(&mut self, carriage_return: bool) -> Result<Option<SseFrame>, SseError> {
        // Charge CR as CRLF even if a peer uses bare CR; never undercount wire bytes.
        self.bytes += if carriage_return { 2 } else { 1 };
        if self.bytes > self.max_bytes {
            return Err(SseError(BYTE_LIMIT));
        }
        let current = std::mem::take(&mut self.line);
        if current.is_empty() {
            let frame = (!self.data.is_empty()).then(|| SseFrame {
                event: if self.event.is_empty() {
                    "message".to_owned()
                } else {
                    std::mem::take(&mut self.event)
                },
                data: self.data.join("\n"),
                id: self.id.take(),
            });
            self.event.clear();
            self.id = None;
            self.data.clear();
            self.bytes = 0;
            return Ok(frame);
        }
        if current[0] == b':' {
            return Ok(None);
        }
        // Lines split on ASCII CR/LF of valid UTF-8 are valid UTF-8.
        let current = String::from_utf8(current).map_err(|_| SseError(INVALID_UTF8))?;
        let (field, value) = match current.find(':') {
            Some(colon) => (&current[..colon], &current[colon + 1..]),
            None => (current.as_str(), ""),
        };
        let value = value.strip_prefix(' ').unwrap_or(value);
        match field {
            "event" => self.event = value.to_owned(),
            "data" => self.data.push(value.to_owned()),
            "id" if !value.contains('\0') => self.id = Some(value.to_owned()),
            _ => {}
        }
        Ok(None)
    }
}
