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
    fn multiple_data_lines_join_with_newlines() {
        assert_eq!(decode(b"data: first\ndata: second\n\n"), ["first\nsecond"]);
    }

    #[test]
    fn other_fields_and_comments_are_dropped() {
        assert_eq!(
            decode(b": keep-alive\nevent: message\nid: 7\nretry: 500\ndata: payload\n\n"),
            ["payload"]
        );
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
}
