use std::io::Read as _;
use std::path::Path;

use arc_proto::v1::ImageAttachment;

pub const MAX_ATTACHMENT_BYTES: usize = 12 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("could not read image {path}: {source}")]
    Read {
        path: String,
        #[source]
        source: std::io::Error,
    },

    #[error("image attachments may total at most 12 MiB")]
    TooLarge,

    #[error("unsupported image format; use PNG, JPEG, or WebP")]
    UnsupportedFormat,

    #[error("image attachment is empty")]
    Empty,
}

pub fn load(path: &Path) -> Result<ImageAttachment, Error> {
    let shown = path.display().to_string();
    let file = std::fs::File::open(path).map_err(|source| Error::Read {
        path: shown.clone(),
        source,
    })?;
    let mut data = Vec::new();
    file.take((MAX_ATTACHMENT_BYTES + 1) as u64)
        .read_to_end(&mut data)
        .map_err(|source| Error::Read {
            path: shown,
            source,
        })?;
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("image")
        .to_owned();
    let mut attachment = ImageAttachment {
        name,
        media_type: String::new(),
        data,
    };
    validate(std::slice::from_mut(&mut attachment))?;
    Ok(attachment)
}

pub fn validate(attachments: &mut [ImageAttachment]) -> Result<(), Error> {
    let total = attachments
        .iter()
        .try_fold(0usize, |total, attachment| {
            total.checked_add(attachment.data.len())
        })
        .ok_or(Error::TooLarge)?;
    if total > MAX_ATTACHMENT_BYTES {
        return Err(Error::TooLarge);
    }
    for attachment in attachments {
        if attachment.data.is_empty() {
            return Err(Error::Empty);
        }
        attachment.media_type = media_type(&attachment.data)
            .ok_or(Error::UnsupportedFormat)?
            .to_owned();
        attachment.name = attachment
            .name
            .rsplit(['/', '\\'])
            .find(|name| !name.is_empty())
            .unwrap_or("image")
            .to_owned();
    }
    Ok(())
}

pub fn display_text(content: &str, attachments: &[ImageAttachment]) -> String {
    let mut shown = attachments
        .iter()
        .map(|attachment| format!("[image: {}]", attachment.name))
        .collect::<Vec<_>>()
        .join("\n");
    if !content.is_empty() {
        if !shown.is_empty() {
            shown.push('\n');
        }
        shown.push_str(content);
    }
    shown
}

fn media_type(data: &[u8]) -> Option<&'static str> {
    if data.starts_with(b"\x89PNG\r\n\x1a\n") {
        Some("image/png")
    } else if data.starts_with(b"\xff\xd8\xff") {
        Some("image/jpeg")
    } else if data.len() >= 12 && data.starts_with(b"RIFF") && &data[8..12] == b"WEBP" {
        Some("image/webp")
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validation_derives_the_type_and_removes_the_original_path() {
        let mut attachments = vec![ImageAttachment {
            name: r"C:\private\screen.png".to_owned(),
            media_type: "text/plain".to_owned(),
            data: b"\x89PNG\r\n\x1a\nbody".to_vec(),
        }];

        validate(&mut attachments).expect("valid image");

        assert_eq!(attachments[0].name, "screen.png");
        assert_eq!(attachments[0].media_type, "image/png");
        assert_eq!(
            display_text("what is this?", &attachments),
            "[image: screen.png]\nwhat is this?"
        );
    }

    #[test]
    fn validation_rejects_unknown_bytes_and_an_oversized_total() {
        let mut unknown = vec![ImageAttachment {
            name: "image.svg".to_owned(),
            media_type: "image/svg+xml".to_owned(),
            data: b"<svg/>".to_vec(),
        }];
        assert!(matches!(
            validate(&mut unknown),
            Err(Error::UnsupportedFormat)
        ));

        let mut oversized = vec![ImageAttachment {
            name: "large.jpg".to_owned(),
            media_type: String::new(),
            data: vec![0; MAX_ATTACHMENT_BYTES + 1],
        }];
        assert!(matches!(validate(&mut oversized), Err(Error::TooLarge)));
    }
}
