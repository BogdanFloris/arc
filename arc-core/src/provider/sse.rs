//! Server-Sent Events framing: bytes in, `data:` payloads out.
//!
//! This is the backend-agnostic half of stream parsing. It knows line endings
//! and field names, and nothing about what a payload means — no JSON, no
//! provider vocabulary. The layer above it knows the opposite: what a payload
//! says, and nothing about how the bytes arrived.
//!
//! Frame boundaries have nothing to do with network chunk boundaries. A frame
//! routinely spans two reads, and one read routinely carries several frames.
//! [`FrameDecoder`] is the buffer that makes that difference invisible: push
//! whatever arrived, then take whatever is complete.
//!
//! # The subset it reads
//!
//! Of the SSE grammar (WHATWG HTML §9.2), the part backends actually send:
//! `data:` fields, a blank line ending each event, everything else ignored.
//!
//! - **Line endings.** CRLF and LF both end a line. The spec also allows a bare
//!   CR; nothing observed sends one, and honouring it would make a CR at the end
//!   of a chunk ambiguous — hold it back and a CR-framed stream stalls, dispatch
//!   on it and a CRLF stream splits every line in two. So a CR ends a line only
//!   when an LF follows it, and any other CR is content.
//! - **Ignored lines.** `event:`, `id:`, `retry:` and `:` comments are read and
//!   dropped. Nothing in ARC dispatches on an event name or resumes a stream by
//!   id, and a decoder that returns fields nobody reads is a wider interface for
//!   no gain. The day a backend needs one, it is added here rather than worked
//!   around above.
//! - **Multiple `data:` lines** in one event are joined with newlines, per spec,
//!   which is what makes a payload that contains a newline expressible at all.
//! - **Invalid UTF-8** becomes U+FFFD rather than an error. A split multi-byte
//!   character is not invalid — it is a line that has not finished arriving, and
//!   this decoder converts only complete lines — so anything left is genuine
//!   damage, and the layer above rejects the payload it cannot parse.
//!
//! An event still arriving when the connection ends is never dispatched: a
//! half-received frame is not a frame. That silence is what tells the caller a
//! stream was cut.

use std::collections::VecDeque;

/// The one field this decoder keeps.
const DATA: &str = "data";

/// Reassembles SSE frames from arbitrary byte chunks.
///
/// Push bytes as they arrive, then drain complete payloads:
///
/// ```
/// use arc_core::provider::sse::FrameDecoder;
///
/// let mut frames = FrameDecoder::new();
/// frames.push(b"data: one\r\n\r\ndata: two\r");
/// assert_eq!(frames.next_frame().as_deref(), Some("one"));
/// // The second frame has not finished arriving.
/// assert_eq!(frames.next_frame(), None);
/// frames.push(b"\n\r\n");
/// assert_eq!(frames.next_frame().as_deref(), Some("two"));
/// assert_eq!(frames.next_frame(), None);
/// ```
#[derive(Debug, Default)]
pub struct FrameDecoder {
    /// Bytes pushed but not yet consumed: whole lines still to be read, and the
    /// start of a line still arriving.
    buffer: VecDeque<u8>,

    /// `data:` values of the event being assembled, joined by newlines.
    data: String,

    /// Whether the event being assembled has seen a `data:` line. Distinct from
    /// `data` being empty: `data:` with nothing after it is a payload of "",
    /// and an event with no `data:` line at all is not dispatched.
    has_data: bool,
}

impl FrameDecoder {
    /// A decoder with an empty buffer.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds received bytes. Any chunking is fine, including one byte at a time.
    pub fn push(&mut self, chunk: &[u8]) {
        self.buffer.extend(chunk);
    }

    /// Removes and returns the next complete payload, if one has arrived.
    ///
    /// `None` means "not yet", never "never": pushing more bytes can make the
    /// next call succeed. Call it until it returns `None` after every push.
    pub fn next_frame(&mut self) -> Option<String> {
        while let Some(end) = self.buffer.iter().position(|&byte| byte == b'\n') {
            let line: Vec<u8> = self.buffer.drain(..=end).collect();
            // Drop the LF, then the CR of a CRLF pair.
            let line = &line[..end];
            let line = line.strip_suffix(b"\r").unwrap_or(line);
            let line = String::from_utf8_lossy(line);

            if line.is_empty() {
                if let Some(frame) = self.dispatch() {
                    return Some(frame);
                }
            } else {
                self.field(&line);
            }
        }
        None
    }

    /// Ends the current event, yielding its payload if it had one.
    fn dispatch(&mut self) -> Option<String> {
        if !self.has_data {
            // Blank lines between frames, and events built only from fields we
            // ignore. Neither is a payload.
            return None;
        }
        self.has_data = false;
        Some(std::mem::take(&mut self.data))
    }

    /// Reads one non-empty line into the event being assembled.
    fn field(&mut self, line: &str) {
        let (name, value) = match line.find(':') {
            // A leading colon is a comment, whatever follows it.
            Some(0) => return,
            Some(colon) => {
                let value = &line[colon + 1..];
                (&line[..colon], value.strip_prefix(' ').unwrap_or(value))
            }
            // A bare field name is that field with an empty value.
            None => (line, ""),
        };

        if name != DATA {
            return;
        }
        if self.has_data {
            self.data.push('\n');
        }
        self.data.push_str(value);
        self.has_data = true;
    }
}

#[cfg(test)]
mod tests {
    use super::FrameDecoder;

    /// Everything complete in the decoder, in order.
    fn drain(frames: &mut FrameDecoder) -> Vec<String> {
        std::iter::from_fn(|| frames.next_frame()).collect()
    }

    /// One push, then everything it completed.
    fn decode(bytes: &[u8]) -> Vec<String> {
        let mut frames = FrameDecoder::new();
        frames.push(bytes);
        drain(&mut frames)
    }

    #[test]
    fn crlf_framed_events_yield_their_payloads() {
        assert_eq!(
            decode(b"data: one\r\n\r\ndata: two\r\n\r\n"),
            ["one", "two"]
        );
    }

    #[test]
    fn lf_framed_events_yield_the_same_payloads() {
        assert_eq!(decode(b"data: one\n\ndata: two\n\n"), ["one", "two"]);
    }

    /// The value's optional leading space is part of the framing, not the data.
    #[test]
    fn one_leading_space_is_stripped_and_no_more() {
        assert_eq!(decode(b"data:tight\n\n"), ["tight"]);
        assert_eq!(decode(b"data: spaced\n\n"), ["spaced"]);
        assert_eq!(decode(b"data:  indented\n\n"), [" indented"]);
    }

    #[test]
    fn multiple_data_lines_join_with_newlines() {
        assert_eq!(decode(b"data: first\ndata: second\n\n"), ["first\nsecond"]);
    }

    #[test]
    fn a_data_field_with_no_value_is_an_empty_payload() {
        assert_eq!(decode(b"data:\n\n"), [""]);
        assert_eq!(decode(b"data\n\n"), [""]);
    }

    #[test]
    fn other_fields_and_comments_are_dropped() {
        assert_eq!(
            decode(b": keep-alive\nevent: message\nid: 7\nretry: 500\ndata: payload\n\n"),
            ["payload"]
        );
    }

    /// An event carrying no `data:` line has no payload to yield, and must not
    /// yield an empty one.
    #[test]
    fn an_event_without_data_dispatches_nothing() {
        assert_eq!(decode(b"event: ping\n\n\n\ndata: real\n\n"), ["real"]);
    }

    /// The property the network forces: chunk boundaries are arbitrary, and the
    /// decoder's output must not depend on them.
    #[test]
    fn payloads_are_the_same_however_the_bytes_are_split() {
        let stream = b": comment\r\ndata: alpha\r\n\r\ndata: beta\r\ndata: gamma\r\n\r\n";

        for split in 0..stream.len() {
            let mut frames = FrameDecoder::new();
            frames.push(&stream[..split]);
            let mut seen = drain(&mut frames);
            frames.push(&stream[split..]);
            seen.extend(drain(&mut frames));

            assert_eq!(seen, ["alpha", "beta\ngamma"], "split at {split}");
        }
    }

    /// Including the split that lands between a CR and its LF, which is the one
    /// a decoder that scans for CRLF gets wrong.
    #[test]
    fn a_split_between_cr_and_lf_is_still_one_line_ending() {
        let mut frames = FrameDecoder::new();
        frames.push(b"data: split\r");
        assert!(drain(&mut frames).is_empty());
        frames.push(b"\n\r");
        assert!(drain(&mut frames).is_empty());
        frames.push(b"\n");

        assert_eq!(drain(&mut frames), ["split"]);
    }

    #[test]
    fn one_byte_at_a_time_decodes_the_same_way() {
        let mut frames = FrameDecoder::new();
        let mut seen = Vec::new();
        for byte in b"data: dribbled\r\n\r\ndata: slowly\r\n\r\n" {
            frames.push(&[*byte]);
            seen.extend(drain(&mut frames));
        }

        assert_eq!(seen, ["dribbled", "slowly"]);
    }

    /// A cut connection leaves a half-built event. It is not a frame and is
    /// never reported as one.
    #[test]
    fn an_unterminated_event_is_never_dispatched() {
        assert_eq!(decode(b"data: complete\n\ndata: cut off"), ["complete"]);
    }

    /// A CR that is not part of a CRLF is content: this decoder does not treat
    /// a bare CR as a line ending.
    #[test]
    fn a_lone_carriage_return_stays_in_the_payload() {
        assert_eq!(decode(b"data: be\rfore\n\n"), ["be\rfore"]);
    }

    /// Multi-byte characters split across chunks are reassembled, because only
    /// complete lines are ever converted to text.
    #[test]
    fn a_character_split_across_chunks_survives() {
        let mut frames = FrameDecoder::new();
        let bytes = "data: héllo → ✓\n\n".as_bytes();
        for byte in bytes {
            frames.push(&[*byte]);
        }

        assert_eq!(drain(&mut frames), ["héllo → ✓"]);
    }

    /// Bytes that are not UTF-8 at all degrade, rather than deriving a framing
    /// error from a content problem.
    #[test]
    fn invalid_utf8_becomes_replacement_characters() {
        assert_eq!(decode(b"data: bad\xffbyte\n\n"), ["bad\u{fffd}byte"]);
    }
}
