//! Guest upload classification from observed sizes, headers, and UTF-16 strings.
//! Copyright (c) 2026 T3 Tools Inc. Distributed under the repository's MIT license.

pub const MAX_ATTACHMENTS: usize = 4;
pub const MAX_ATTACHMENT_BYTES: f64 = 10.0 * 1024.0 * 1024.0;

#[derive(Clone, Debug, PartialEq)]
pub struct InspectedUpload {
    pub name_units: Vec<u16>,
    /// The host supplies JavaScript's Unicode lowercase result. All remaining
    /// MIME and filename policy is evaluated here without Unicode conversion.
    pub content_type_lowercase_units: Vec<u16>,
    pub size_bytes: f64,
    pub header: Vec<u8>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AttachmentKind {
    Image,
    File,
}

impl AttachmentKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Image => "image",
            Self::File => "file",
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct AttachmentDraft {
    pub kind: AttachmentKind,
    pub name_units: Vec<u16>,
    pub mime_type_units: Vec<u16>,
    pub size_bytes: f64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UploadRejection {
    TooManyFiles,
    TooLarge,
    UnsupportedFile,
}

impl UploadRejection {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::TooManyFiles => "too_many_files",
            Self::TooLarge => "too_large",
            Self::UnsupportedFile => "unsupported_file",
        }
    }
}

fn js_whitespace(unit: u16) -> bool {
    matches!(
        unit,
        0x0009..=0x000d
            | 0x0020
            | 0x00a0
            | 0x1680
            | 0x2000..=0x200a
            | 0x2028
            | 0x2029
            | 0x202f
            | 0x205f
            | 0x3000
            | 0xfeff
    )
}

fn trim_units(units: &[u16]) -> &[u16] {
    let start = units
        .iter()
        .position(|unit| !js_whitespace(*unit))
        .unwrap_or(units.len());
    let end = units
        .iter()
        .rposition(|unit| !js_whitespace(*unit))
        .map_or(start, |index| index + 1);
    &units[start..end]
}

fn units(value: &str) -> Vec<u16> {
    value.encode_utf16().collect()
}

fn equals_ascii(value: &[u16], ascii: &str) -> bool {
    value.iter().copied().eq(ascii.bytes().map(u16::from))
}

fn starts_with_ascii(value: &[u16], ascii: &str) -> bool {
    value
        .get(..ascii.len())
        .is_some_and(|prefix| equals_ascii(prefix, ascii))
}

fn safe_name(name: &[u16], fallback: &str) -> Vec<u16> {
    // All Unicode Cc characters and the explicitly blocked direction controls
    // are BMP code points. Other units, including lone surrogates, stay intact.
    let replaced: Vec<u16> = name
        .iter()
        .map(|unit| {
            if matches!(
                *unit,
                0x0000..=0x001f
                    | 0x007f..=0x009f
                    | 0x002f
                    | 0x005c
                    | 0x061c
                    | 0x200e
                    | 0x200f
                    | 0x202a..=0x202e
                    | 0x2066..=0x2069
            ) {
                u16::from(b'_')
            } else {
                *unit
            }
        })
        .collect();
    let trimmed = trim_units(&replaced);
    if trimmed.is_empty() {
        units(fallback)
    } else {
        // JavaScript slice counts UTF-16 units and can split a surrogate pair.
        trimmed[..trimmed.len().min(255)].to_vec()
    }
}

fn matches_image_signature(mime_type: &[u16], header: &[u8]) -> bool {
    if equals_ascii(mime_type, "image/png") {
        header.starts_with(&[0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a])
    } else if equals_ascii(mime_type, "image/jpeg") {
        header.starts_with(&[0xff, 0xd8, 0xff])
    } else if equals_ascii(mime_type, "image/gif") {
        header.starts_with(b"GIF8")
    } else if equals_ascii(mime_type, "image/webp") {
        header.starts_with(b"RIFF")
            && header
                .get(8..)
                .is_some_and(|suffix| suffix.starts_with(b"WEBP"))
    } else {
        false
    }
}

/// The wire validates finite sizes. Preserve JavaScript's numerical comparisons
/// and left-to-right addition, including fractional and negative input values.
pub fn classify_uploads(
    uploads: &[InspectedUpload],
) -> Result<Vec<AttachmentDraft>, UploadRejection> {
    if uploads.len() > MAX_ATTACHMENTS {
        return Err(UploadRejection::TooManyFiles);
    }
    let total = uploads
        .iter()
        .fold(0.0, |total, upload| total + upload.size_bytes);
    if total > MAX_ATTACHMENT_BYTES {
        return Err(UploadRejection::TooLarge);
    }
    let mut attachments = Vec::with_capacity(uploads.len());
    for upload in uploads {
        let semicolon = upload
            .content_type_lowercase_units
            .iter()
            .position(|unit| *unit == u16::from(b';'))
            .unwrap_or(upload.content_type_lowercase_units.len());
        let mime_type = trim_units(&upload.content_type_lowercase_units[..semicolon]);
        let mut mime_type = if mime_type.is_empty() {
            units("application/octet-stream")
        } else {
            mime_type.to_vec()
        };
        if upload.size_bytes == 0.0 {
            return Err(UploadRejection::UnsupportedFile);
        }
        let kind = if starts_with_ascii(&mime_type, "image/") {
            if !matches_image_signature(&mime_type, &upload.header) {
                return Err(UploadRejection::UnsupportedFile);
            }
            AttachmentKind::Image
        } else {
            if mime_type.len() > 100 {
                mime_type = units("application/octet-stream");
            }
            AttachmentKind::File
        };
        attachments.push(AttachmentDraft {
            kind,
            name_units: safe_name(&upload.name_units, kind.as_str()),
            mime_type_units: mime_type,
            size_bytes: upload.size_bytes,
        });
    }
    Ok(attachments)
}

#[cfg(test)]
mod tests {
    use super::*;

    const PNG: &[u8] = &[0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a];

    fn upload(name: &str, mime: &str, size: f64, header: &[u8]) -> InspectedUpload {
        InspectedUpload {
            name_units: units(name),
            content_type_lowercase_units: units(mime),
            size_bytes: size,
            header: header.to_vec(),
        }
    }

    #[test]
    fn count_and_total_rejections_precede_individual_file_checks() {
        let invalid = upload("bad.svg", "image/svg+xml", MAX_ATTACHMENT_BYTES + 1.0, &[]);
        assert_eq!(
            classify_uploads(&vec![invalid.clone(); 5]),
            Err(UploadRejection::TooManyFiles)
        );
        assert_eq!(
            classify_uploads(&[invalid]),
            Err(UploadRejection::TooLarge)
        );
        let exact = upload("exact", "text/plain", MAX_ATTACHMENT_BYTES, &[]);
        assert_eq!(
            classify_uploads(&[exact]).unwrap()[0].size_bytes,
            MAX_ATTACHMENT_BYTES
        );
        for size in [0.0, -0.0] {
            assert_eq!(
                classify_uploads(&[upload("empty", "text/plain", size, &[])]),
                Err(UploadRejection::UnsupportedFile)
            );
        }
        assert_eq!(classify_uploads(&[]), Ok(vec![]));
    }

    #[test]
    fn image_types_require_the_upstream_signature_prefixes() {
        let signatures: &[(&str, &[u8])] = &[
            ("image/png", PNG),
            ("image/jpeg", &[0xff, 0xd8, 0xff]),
            ("image/gif", b"GIF8"),
            ("image/webp", b"RIFF\0\0\0\0WEBP"),
        ];
        for &(mime, header) in signatures {
            let source = upload("", mime, 1.0, header);
            let draft = classify_uploads(&[source.clone()]).unwrap().remove(0);
            assert_eq!(draft.kind, AttachmentKind::Image);
            assert_eq!(draft.name_units, units("image"));
            assert_eq!(draft.mime_type_units, units(mime));
            let mut truncated = source;
            truncated.header.pop();
            assert_eq!(
                classify_uploads(&[truncated]),
                Err(UploadRejection::UnsupportedFile)
            );
        }
        for mime in ["image/svg+xml", "image/heic", "image/jpg", "image/pngx"] {
            assert_eq!(
                classify_uploads(&[upload("renamed", mime, 1.0, PNG)]),
                Err(UploadRejection::UnsupportedFile)
            );
        }
        assert_eq!(
            classify_uploads(&[upload("wrong", "image/png", 1.0, b"<html>")]),
            Err(UploadRejection::UnsupportedFile)
        );
    }

    #[test]
    fn mime_parameters_js_whitespace_and_utf16_length_match_the_original_policy() {
        let image = upload("photo", "\u{feff} image/png\u{00a0};ignored", 1.0, PNG);
        assert_eq!(
            classify_uploads(&[image]).unwrap()[0].mime_type_units,
            units("image/png")
        );
        let blank = upload("\u{2003}\u{feff}", "\u{1680}\u{2029};image/png", 1.0, &[]);
        let draft = classify_uploads(&[blank]).unwrap().remove(0);
        assert_eq!(draft.name_units, units("file"));
        assert_eq!(draft.mime_type_units, units("application/octet-stream"));
        let mut source = upload("file", "", 1.0, &[]);
        source.content_type_lowercase_units = vec![0xd800; 100];
        assert_eq!(
            classify_uploads(&[source.clone()]).unwrap()[0].mime_type_units,
            vec![0xd800; 100]
        );
        source.content_type_lowercase_units.push(0xdc00);
        assert_eq!(
            classify_uploads(&[source]).unwrap()[0].mime_type_units,
            units("application/octet-stream")
        );
        let non_whitespace = upload("\u{180e}\u{200b}", "\u{0085}text/plain", 1.0, &[]);
        let draft = classify_uploads(&[non_whitespace]).unwrap().remove(0);
        assert_eq!(draft.name_units, units("\u{180e}\u{200b}"));
        assert_eq!(draft.mime_type_units, units("\u{0085}text/plain"));
    }

    #[test]
    fn names_replace_controls_before_trim_and_preserve_split_surrogates() {
        let blocked = [
            0x0000, 0x0009, 0x001f, 0x007f, 0x009f, 0x002f, 0x005c, 0x061c, 0x200e, 0x200f,
            0x202a, 0x202e, 0x2066, 0x2069,
        ];
        let mut source = upload("", "text/plain", 1.0, &[]);
        source.name_units = [vec![0x2003], blocked.to_vec(), vec![0x3000]].concat();
        assert_eq!(
            classify_uploads(&[source.clone()]).unwrap()[0].name_units,
            vec![u16::from(b'_'); blocked.len()]
        );
        source.name_units = vec![u16::from(b'x'); 254];
        source.name_units.extend([0xd83d, 0xde00, 0xdc00]);
        let name = classify_uploads(&[source.clone()])
            .unwrap()
            .remove(0)
            .name_units;
        assert_eq!(name.len(), 255);
        assert_eq!(name.last(), Some(&0xd83d));
        source.name_units = vec![0xd800, u16::from(b'a'), 0xdc00];
        assert_eq!(
            classify_uploads(&[source.clone()]).unwrap()[0].name_units,
            source.name_units
        );
    }

    #[test]
    fn finite_sizes_keep_input_order_and_javascript_addition_order() {
        let sources = [
            upload("first", "text/plain", 1.25, &[]),
            upload("second", "text/plain", -0.25, &[]),
        ];
        let drafts = classify_uploads(&sources).unwrap();
        assert_eq!(drafts[0].name_units, units("first"));
        assert_eq!(drafts[0].size_bytes, 1.25);
        assert_eq!(drafts[1].name_units, units("second"));
        assert_eq!(drafts[1].size_bytes, -0.25);
        let with_sizes = |sizes: &[f64]| {
            sizes
                .iter()
                .map(|size| upload("file", "text/plain", *size, &[]))
                .collect::<Vec<_>>()
        };
        assert!(classify_uploads(&with_sizes(&[1e30, MAX_ATTACHMENT_BYTES + 1.0, -1e30])).is_ok());
        assert_eq!(
            classify_uploads(&with_sizes(&[1e30, -1e30, MAX_ATTACHMENT_BYTES + 1.0])),
            Err(UploadRejection::TooLarge)
        );
        assert_eq!(
            classify_uploads(&with_sizes(&[f64::MAX, f64::MAX])),
            Err(UploadRejection::TooLarge)
        );
    }
}
