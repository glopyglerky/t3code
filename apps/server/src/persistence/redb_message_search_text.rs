//! Migration 81's citation-aware search text, also used for every live message.
use std::collections::BTreeMap;

const PREFIX: &str = "[Assistant quote](t3-citation://v1/";
const MAX_HREF: usize = 9 * (8_000 + 8_000 + 3 * 4_096 + 2 * 32) + 1_000;
fn whitespace(ch: char) -> bool {
    matches!(ch, '\u{0009}'..='\u{000d}' | '\u{0020}' | '\u{00a0}' | '\u{1680}'
        | '\u{2000}'..='\u{200a}' | '\u{2028}' | '\u{2029}' | '\u{202f}' | '\u{205f}' | '\u{3000}' | '\u{feff}')
}
fn length(text: &str) -> usize {
    text.encode_utf16().count()
}
fn percent_decode(text: &str, form: bool) -> Option<String> {
    let bytes = text.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut offset = 0;
    while offset < bytes.len() {
        match bytes[offset] {
            b'%' if offset + 2 < bytes.len() => {
                let high = (bytes[offset + 1] as char).to_digit(16);
                let low = (bytes[offset + 2] as char).to_digit(16);
                if let (Some(high), Some(low)) = (high, low) {
                    decoded.push((high * 16 + low) as u8);
                    offset += 3;
                    continue;
                }
                if !form {
                    return None;
                }
                decoded.push(b'%');
            }
            b'%' if !form => return None,
            b'+' if form => decoded.push(b' '),
            byte => decoded.push(byte),
        }
        offset += 1;
    }
    if form {
        Some(String::from_utf8_lossy(&decoded).into_owned())
    } else {
        String::from_utf8(decoded).ok()
    }
}
fn citation_text(suffix: &str) -> Option<String> {
    if suffix.chars().any(whitespace) {
        return None;
    }
    let suffix = match suffix.split_once('#') {
        Some((href, "")) => href,
        Some(_) => return None,
        None => suffix,
    };
    let (path, query) = suffix.split_once('?')?;
    let mut raw_parts = Vec::new();
    let mut segments = path.split('/').peekable();
    while let Some(part) = segments.next() {
        // URL path normalization happens before decodeURIComponent. A discarded
        // segment may contain malformed escapes without invalidating the IDs.
        match part.to_ascii_lowercase().as_str() {
            "." | "%2e" => {
                if segments.peek().is_none() {
                    raw_parts.push("");
                }
            }
            ".." | ".%2e" | "%2e." | "%2e%2e" => {
                raw_parts.pop();
                if segments.peek().is_none() {
                    raw_parts.push("");
                }
            }
            _ => raw_parts.push(part),
        }
    }
    let parts = raw_parts
        .into_iter()
        .map(|part| percent_decode(part, false))
        .collect::<Option<Vec<_>>>()?;
    if parts.len() != 3
        || parts.iter().any(|part| {
            let part = part.trim_matches(whitespace);
            part.is_empty() || length(part) > 4_096
        })
    {
        return None;
    }
    let mut fields = BTreeMap::new();
    for pair in query.split('&').filter(|part| !part.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        let key = percent_decode(key, true)?;
        let value = percent_decode(value, true)?;
        if fields.insert(key, value).is_some() {
            return None;
        }
    }
    if fields.len() != 5 + usize::from(fields.contains_key("comment")) {
        return None;
    }
    let text = fields.get("text")?;
    let prefix = fields.get("prefix")?;
    let suffix = fields.get("suffix")?;
    let offset = |key: &str| -> Option<u64> {
        let value = fields.get(key)?;
        if value.is_empty() || value.len() > 16 || !value.bytes().all(|byte| byte.is_ascii_digit())
        {
            return None;
        }
        value
            .parse::<u64>()
            .ok()
            .filter(|value| *value <= (1 << 53) - 1)
    };
    if offset("end")? <= offset("start")?
        || text.trim_matches(whitespace).is_empty()
        || length(text) > 8_000
        || length(prefix) > 32
        || length(suffix) > 32
    {
        return None;
    }
    match fields.get("comment") {
        Some(comment) if length(comment) > 8_000 => None,
        Some(comment) => Some(format!("{text}\nComment: {comment}")),
        None => Some(text.clone()),
    }
}

pub(crate) fn readable_search_text(text: &str) -> Option<String> {
    let mut output = String::new();
    let mut copied = 0;
    let mut cursor = 0;
    while let Some(offset) = text[cursor..].find(PREFIX) {
        let start = cursor + offset;
        let body = start + PREFIX.len();
        cursor = body;
        let Some(end) = text[body..].find(')').map(|offset| body + offset) else {
            break;
        };
        let suffix = &text[body..end];
        if length(suffix) + "t3-citation://v1/".len() > MAX_HREF {
            continue;
        }
        if let Some(readable) = citation_text(suffix) {
            output.push_str(&text[copied..start]);
            output.push_str(&readable);
            copied = end + 1;
            cursor = copied;
        }
    }
    if copied == 0 {
        None
    } else {
        output.push_str(&text[copied..]);
        (output != text).then_some(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn citation_parser_matches_current_upstream_url_and_schema_fixtures() {
        let fixtures: serde_json::Value =
            serde_json::from_str(include_str!("fixtures/message-search-text.json")).unwrap();
        for case in fixtures["cases"].as_array().unwrap() {
            let expected = case["expected"].as_str().map(str::to_owned);
            assert_eq!(
                readable_search_text(case["input"].as_str().unwrap()),
                expected,
                "{}",
                case["name"]
            );
        }
    }

    #[test]
    fn citations_decode_words_comments_and_utf16_offsets_without_indexing_urls() {
        let link = "[Assistant quote](t3-citation://v1/env/thread/msg?text=can%27t+lose+%F0%9F%8C%8D&start=0&end=13&prefix=&suffix=&comment=check+this)";
        assert_eq!(
            readable_search_text(&format!("Read {link} now")).as_deref(),
            Some("Read can't lose 🌍\nComment: check this now")
        );
        assert_eq!(readable_search_text("ordinary text"), None);
        for invalid in [
            link.replace("end=13", "end=0"),
            link.replace("prefix=", "start=4"),
            link.replace("env/", "%ZZ/"),
            link.replace("text=can", "extra=x&text=can"),
        ] {
            assert_eq!(readable_search_text(&invalid), None);
        }
    }
}
