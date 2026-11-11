//! The shared assistant-citation plain-text contract, including JavaScript's
//! UTF-16 length limits, URLSearchParams decoding and strict path decoding.

const HREF_PREFIX: &str = "t3-citation://v1/";
const LINK_PREFIX: &str = "[Assistant quote](t3-citation://v1/";
const MAX_HREF: usize = 9 * (8_000 + 8_000 + 3 * 4_096 + 2 * 32) + 1_000;
const MAX_SAFE_INTEGER: u64 = (1 << 53) - 1;

fn js_space(character: char) -> bool {
    matches!(character, '\u{0009}'..='\u{000d}' | '\u{0020}' | '\u{00a0}' |
        '\u{1680}' | '\u{2000}'..='\u{200a}' | '\u{2028}' | '\u{2029}' |
        '\u{202f}' | '\u{205f}' | '\u{3000}' | '\u{feff}')
}

fn utf16_length(value: &str) -> usize {
    value.encode_utf16().count()
}

fn decode_path(value: &str) -> Option<String> {
    let mut bytes = Vec::with_capacity(value.len());
    let mut source = value.as_bytes().iter().copied();
    while let Some(byte) = source.next() {
        bytes.push(if byte == b'%' {
            let high = (source.next()? as char).to_digit(16)?;
            let low = (source.next()? as char).to_digit(16)?;
            ((high << 4) | low) as u8
        } else {
            byte
        });
    }
    String::from_utf8(bytes).ok()
}

fn offset(value: &str) -> Option<u64> {
    if value.is_empty() || value.len() > 16 || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    value
        .parse::<u64>()
        .ok()
        .filter(|value| *value <= MAX_SAFE_INTEGER)
}

fn plain_citation(href: &str) -> Option<String> {
    if !href.starts_with(HREF_PREFIX) || utf16_length(href) > MAX_HREF {
        return None;
    }
    let url = url::Url::parse(href).ok()?;
    if url.scheme() != "t3-citation"
        || url.host_str() != Some("v1")
        || !url.username().is_empty()
        || url.password().is_some_and(|value| !value.is_empty())
        || url.port().is_some()
        || url.fragment().is_some_and(|value| !value.is_empty())
    {
        return None;
    }
    let parts: Vec<_> = url.path().strip_prefix('/')?.split('/').collect();
    if parts.len() != 3 {
        return None;
    }
    for part in parts {
        let decoded = decode_path(part)?;
        let trimmed = decoded.trim_matches(js_space);
        if trimmed.is_empty() || utf16_length(trimmed) > 4_096 {
            return None;
        }
    }
    let mut fields = std::collections::BTreeMap::new();
    for (key, value) in url.query_pairs() {
        if !matches!(
            key.as_ref(),
            "text" | "start" | "end" | "prefix" | "suffix" | "comment"
        ) || fields
            .insert(key.into_owned(), value.into_owned())
            .is_some()
        {
            return None;
        }
    }
    let text = fields.get("text")?;
    let start = offset(fields.get("start")?)?;
    let end = offset(fields.get("end")?)?;
    if end <= start
        || utf16_length(text) > 8_000
        || text.trim_matches(js_space).is_empty()
        || utf16_length(fields.get("prefix")?) > 32
        || utf16_length(fields.get("suffix")?) > 32
        || fields
            .get("comment")
            .is_some_and(|value| utf16_length(value) > 8_000)
    {
        return None;
    }
    Some(match fields.get("comment") {
        Some(comment) => format!("{text}\nComment: {comment}"),
        None => text.clone(),
    })
}

/// Returns NULL's native equivalent when the stored text already reads plainly.
pub(crate) fn readable_search_text(text: &str) -> Option<String> {
    let mut output = String::new();
    let mut copied = 0;
    let mut cursor = 0;
    while let Some(relative) = text[cursor..].find(LINK_PREFIX) {
        let start = cursor + relative;
        let suffix_start = start + LINK_PREFIX.len();
        let mut suffix_units = 0;
        let mut closing = None;
        for (offset, character) in text[suffix_start..].char_indices() {
            if character == ')' {
                if suffix_units > 0 {
                    closing = Some(suffix_start + offset);
                }
                break;
            }
            suffix_units += character.len_utf16();
            if js_space(character) || suffix_units > MAX_HREF - HREF_PREFIX.len() {
                break;
            }
        }
        let Some(end) = closing else {
            // A failed regex match may still contain a later valid link.
            cursor = suffix_start;
            continue;
        };
        let href_start = suffix_start - HREF_PREFIX.len();
        if let Some(readable) = plain_citation(&text[href_start..end]) {
            output.push_str(&text[copied..start]);
            output.push_str(&readable);
            copied = end + 1;
        }
        // A syntactically matched invalid citation is consumed unchanged.
        cursor = end + 1;
    }
    if copied == 0 {
        return None;
    }
    output.push_str(&text[copied..]);
    (output != text).then_some(output)
}
