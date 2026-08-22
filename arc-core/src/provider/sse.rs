use std::collections::VecDeque;

const DATA: &str = "data";

#[derive(Debug, Default)]
pub(crate) struct FrameDecoder {
    buffer: VecDeque<u8>,

    data: String,

    has_data: bool,
}

impl FrameDecoder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, chunk: &[u8]) {
        self.buffer.extend(chunk);
    }

    pub fn next_frame(&mut self) -> Option<String> {
        while let Some(end) = self.buffer.iter().position(|&byte| byte == b'\n') {
            let line: Vec<u8> = self.buffer.drain(..=end).collect();
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

    fn dispatch(&mut self) -> Option<String> {
        if !self.has_data {
            return None;
        }
        self.has_data = false;
        Some(std::mem::take(&mut self.data))
    }

    fn field(&mut self, line: &str) {
        let (name, value) = match line.find(':') {
            // a line starting with ':' is a comment, not a field
            Some(0) => return,
            Some(colon) => {
                let value = &line[colon + 1..];
                (&line[..colon], value.strip_prefix(' ').unwrap_or(value))
            }
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

    fn drain(frames: &mut FrameDecoder) -> Vec<String> {
        std::iter::from_fn(|| frames.next_frame()).collect()
    }

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

    #[test]
    fn an_event_without_data_dispatches_nothing() {
        assert_eq!(decode(b"event: ping\n\n\n\ndata: real\n\n"), ["real"]);
    }

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

    #[test]
    fn an_unterminated_event_is_never_dispatched() {
        assert_eq!(decode(b"data: complete\n\ndata: cut off"), ["complete"]);
    }

    #[test]
    fn a_lone_carriage_return_stays_in_the_payload() {
        assert_eq!(decode(b"data: be\rfore\n\n"), ["be\rfore"]);
    }

    #[test]
    fn a_character_split_across_chunks_survives() {
        let mut frames = FrameDecoder::new();
        let bytes = "data: héllo → ✓\n\n".as_bytes();
        for byte in bytes {
            frames.push(&[*byte]);
        }

        assert_eq!(drain(&mut frames), ["héllo → ✓"]);
    }

    #[test]
    fn invalid_utf8_becomes_replacement_characters() {
        assert_eq!(decode(b"data: bad\xffbyte\n\n"), ["bad\u{fffd}byte"]);
    }
}
