//! Bounded JSON for provider wire messages, without a procedural-macro runtime.

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Value {
    Null,
    Bool(bool),
    Number(String),
    String(String),
    Array(Vec<Value>),
    Object(Vec<(String, Value)>),
}

impl Value {
    pub fn get(&self, key: &str) -> Option<&Value> {
        match self {
            Self::Object(entries) => entries
                .iter()
                .rev()
                .find(|entry| entry.0 == key)
                .map(|entry| &entry.1),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(value) => Some(value),
            _ => None,
        }
    }

    pub fn as_array(&self) -> Option<&[Value]> {
        match self {
            Self::Array(value) => Some(value),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(value) => Some(*value),
            _ => None,
        }
    }

    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Self::Number(value) => value.parse().ok(),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Number(value) => value.parse().ok(),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    pub max_bytes: usize,
    pub max_depth: usize,
    pub max_values: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_bytes: 128 * 1024 * 1024,
            max_depth: 128,
            max_values: 1_048_576,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Error {
    pub offset: usize,
    pub message: &'static str,
}

impl std::fmt::Display for Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{} at byte {}", self.message, self.offset)
    }
}

impl std::error::Error for Error {}

pub fn parse(input: &str) -> Result<Value, Error> {
    parse_with_limits(input, Limits::default())
}

pub fn parse_with_limits(input: &str, limits: Limits) -> Result<Value, Error> {
    if input.len() > limits.max_bytes {
        return Err(Error {
            offset: 0,
            message: "JSON exceeds byte limit",
        });
    }
    let mut parser = Parser {
        input,
        offset: 0,
        values: 0,
        limits,
    };
    let value = parser.value(0)?;
    parser.whitespace();
    if parser.offset != input.len() {
        return Err(parser.error("unexpected data after JSON value"));
    }
    Ok(value)
}

struct Parser<'a> {
    input: &'a str,
    offset: usize,
    values: usize,
    limits: Limits,
}

impl Parser<'_> {
    fn error(&self, message: &'static str) -> Error {
        Error {
            offset: self.offset,
            message,
        }
    }

    fn peek(&self) -> Option<u8> {
        self.input.as_bytes().get(self.offset).copied()
    }

    fn whitespace(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\t' | b'\r' | b'\n')) {
            self.offset += 1;
        }
    }

    fn literal(&mut self, text: &str, value: Value) -> Result<Value, Error> {
        if self.input[self.offset..].starts_with(text) {
            self.offset += text.len();
            Ok(value)
        } else {
            Err(self.error("invalid JSON literal"))
        }
    }

    fn value(&mut self, depth: usize) -> Result<Value, Error> {
        self.whitespace();
        if self.values >= self.limits.max_values {
            return Err(self.error("JSON exceeds value limit"));
        }
        self.values += 1;
        match self.peek() {
            Some(b'n') => self.literal("null", Value::Null),
            Some(b't') => self.literal("true", Value::Bool(true)),
            Some(b'f') => self.literal("false", Value::Bool(false)),
            Some(b'"') => self.string().map(Value::String),
            Some(b'-' | b'0'..=b'9') => self.number().map(Value::Number),
            Some(b'[' | b'{') => {
                if depth >= self.limits.max_depth {
                    return Err(self.error("JSON exceeds nesting limit"));
                }
                if self.peek() == Some(b'[') {
                    self.array(depth + 1)
                } else {
                    self.object(depth + 1)
                }
            }
            _ => Err(self.error("expected a JSON value")),
        }
    }

    fn number(&mut self) -> Result<String, Error> {
        let start = self.offset;
        if self.peek() == Some(b'-') {
            self.offset += 1;
        }
        match self.peek() {
            Some(b'0') => self.offset += 1,
            Some(b'1'..=b'9') => {
                while matches!(self.peek(), Some(b'0'..=b'9')) {
                    self.offset += 1;
                }
            }
            _ => return Err(self.error("expected a number")),
        }
        if self.peek() == Some(b'.') {
            self.offset += 1;
            self.digits()?;
        }
        if matches!(self.peek(), Some(b'e' | b'E')) {
            self.offset += 1;
            if matches!(self.peek(), Some(b'+' | b'-')) {
                self.offset += 1;
            }
            self.digits()?;
        }
        Ok(self.input[start..self.offset].to_owned())
    }

    fn digits(&mut self) -> Result<(), Error> {
        let start = self.offset;
        while matches!(self.peek(), Some(b'0'..=b'9')) {
            self.offset += 1;
        }
        if self.offset == start {
            Err(self.error("expected a decimal digit"))
        } else {
            Ok(())
        }
    }

    fn hex_quad(&mut self) -> Result<u32, Error> {
        let mut value = 0;
        for _ in 0..4 {
            let digit = match self.peek() {
                Some(b'0'..=b'9') => u32::from(self.input.as_bytes()[self.offset] - b'0'),
                Some(b'a'..=b'f') => u32::from(self.input.as_bytes()[self.offset] - b'a' + 10),
                Some(b'A'..=b'F') => u32::from(self.input.as_bytes()[self.offset] - b'A' + 10),
                _ => return Err(self.error("invalid Unicode escape")),
            };
            value = value * 16 + digit;
            self.offset += 1;
        }
        Ok(value)
    }

    fn escaped_unicode(&mut self) -> Result<char, Error> {
        let first = self.hex_quad()?;
        let codepoint = if (0xd800..=0xdbff).contains(&first) {
            if !self.input[self.offset..].starts_with("\\u") {
                return Err(self.error("unpaired Unicode surrogate"));
            }
            self.offset += 2;
            let second = self.hex_quad()?;
            if !(0xdc00..=0xdfff).contains(&second) {
                return Err(self.error("unpaired Unicode surrogate"));
            }
            0x10000 + ((first - 0xd800) << 10) + second - 0xdc00
        } else {
            first
        };
        char::from_u32(codepoint).ok_or(self.error("unpaired Unicode surrogate"))
    }

    fn string(&mut self) -> Result<String, Error> {
        self.offset += 1;
        let mut value = String::new();
        let mut start = self.offset;
        loop {
            match self.peek() {
                Some(b'"') => {
                    value.push_str(&self.input[start..self.offset]);
                    self.offset += 1;
                    return Ok(value);
                }
                Some(b'\\') => {
                    value.push_str(&self.input[start..self.offset]);
                    self.offset += 1;
                    let escape = self.peek().ok_or(self.error("unterminated escape"))?;
                    self.offset += 1;
                    let character = match escape {
                        b'"' => '"',
                        b'\\' => '\\',
                        b'/' => '/',
                        b'b' => '\u{0008}',
                        b'f' => '\u{000c}',
                        b'n' => '\n',
                        b'r' => '\r',
                        b't' => '\t',
                        b'u' => self.escaped_unicode()?,
                        _ => return Err(self.error("invalid string escape")),
                    };
                    value.push(character);
                    start = self.offset;
                }
                Some(0..=0x1f) => return Err(self.error("unescaped control character")),
                Some(_) => self.offset += 1,
                None => return Err(self.error("unterminated string")),
            }
        }
    }

    fn array(&mut self, depth: usize) -> Result<Value, Error> {
        self.offset += 1;
        self.whitespace();
        let mut values = Vec::new();
        if self.peek() == Some(b']') {
            self.offset += 1;
            return Ok(Value::Array(values));
        }
        loop {
            values.push(self.value(depth)?);
            self.whitespace();
            match self.peek() {
                Some(b']') => {
                    self.offset += 1;
                    return Ok(Value::Array(values));
                }
                Some(b',') => self.offset += 1,
                _ => return Err(self.error("expected comma or closing bracket")),
            }
        }
    }

    fn object(&mut self, depth: usize) -> Result<Value, Error> {
        self.offset += 1;
        self.whitespace();
        let mut values = Vec::new();
        if self.peek() == Some(b'}') {
            self.offset += 1;
            return Ok(Value::Object(values));
        }
        loop {
            self.whitespace();
            if self.peek() != Some(b'"') {
                return Err(self.error("expected an object key"));
            }
            let key = self.string()?;
            self.whitespace();
            if self.peek() != Some(b':') {
                return Err(self.error("expected colon after object key"));
            }
            self.offset += 1;
            let value = self.value(depth)?;
            values.push((key, value));
            self.whitespace();
            match self.peek() {
                Some(b'}') => {
                    self.offset += 1;
                    return Ok(Value::Object(values));
                }
                Some(b',') => self.offset += 1,
                _ => return Err(self.error("expected comma or closing brace")),
            }
        }
    }
}

fn quoted(value: &str, output: &mut String) {
    output.push('"');
    for character in value.chars() {
        match character {
            '"' => output.push_str("\\\""),
            '\\' => output.push_str("\\\\"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            '\u{0008}' => output.push_str("\\b"),
            '\u{000c}' => output.push_str("\\f"),
            '\u{0000}'..='\u{001f}' => output.push_str(&format!("\\u{:04x}", character as u32)),
            _ => output.push(character),
        }
    }
    output.push('"');
}

fn encode(value: &Value, output: &mut String) {
    match value {
        Value::Null => output.push_str("null"),
        Value::Bool(value) => output.push_str(if *value { "true" } else { "false" }),
        Value::Number(value) => {
            // Number text preserves provider IDs beyond the IEEE-754 integer range.
            assert!(
                matches!(parse(value), Ok(Value::Number(number)) if number == *value),
                "invalid JSON number"
            );
            output.push_str(value);
        }
        Value::String(value) => quoted(value, output),
        Value::Array(values) => {
            output.push('[');
            for (index, item) in values.iter().enumerate() {
                if index != 0 {
                    output.push(',');
                }
                encode(item, output);
            }
            output.push(']');
        }
        Value::Object(values) => {
            output.push('{');
            for (index, (key, item)) in values.iter().enumerate() {
                if index != 0 {
                    output.push(',');
                }
                quoted(key, output);
                output.push(':');
                encode(item, output);
            }
            output.push('}');
        }
    }
}

pub fn stringify(value: &Value) -> String {
    let mut output = String::new();
    encode(value, &mut output);
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_round_trip_preserves_unicode_control_text_and_large_identifiers() {
        let wire = r#"{"id":18446744073709551615,"text":"h\u00e9llo \ud83d\ude80\n\u0000","ok":true,"nested":[null,-0,1.25e+9,{}]}"#;
        let value = parse(wire).unwrap();
        assert_eq!(value.get("id").unwrap().as_u64(), Some(u64::MAX));
        assert_eq!(value.get("text").unwrap().as_str(), Some("héllo 🚀\n\0"));
        assert_eq!(parse(&stringify(&value)).unwrap(), value);
        assert_eq!(
            parse(r#"{"id":1,"id":2}"#)
                .unwrap()
                .get("id")
                .unwrap()
                .as_u64(),
            Some(2)
        );
    }

    #[test]
    fn malformed_messages_never_recover_into_different_requests() {
        for input in [
            "",
            "[1,]",
            "{\"a\":1,}",
            "{\"a\" 1}",
            "{a:1}",
            "[01]",
            "01",
            "-",
            "1.",
            "1e",
            "1e+",
            "+1",
            "NaN",
            "true false",
            "nullx",
            "[1 2]",
            "\"line\nfeed\"",
            "\"\\x41\"",
            "\"\\uD800\"",
            "\"\\uDC00\"",
            "\"\\uD800\\u0041\"",
            "\"\\u0x00\"",
            "\"unfinished",
            "[",
            "{",
        ] {
            assert!(parse(input).is_err(), "accepted {input}");
        }
    }

    #[test]
    fn limits_apply_before_unbounded_allocation_or_recursion() {
        let limits = Limits {
            max_bytes: 20,
            max_depth: 2,
            max_values: 3,
        };
        assert!(parse_with_limits("[[0]]", limits).is_ok());
        assert!(parse_with_limits("[[[]]]", limits).is_err());
        assert!(parse_with_limits("[0,1]", limits).is_ok());
        assert!(parse_with_limits("[0,1,2]", limits).is_err());
        assert!(parse_with_limits("\"01234567890123456789\"", limits).is_err());
        assert!(
            parse_with_limits(
                "{\"x\":0}",
                Limits {
                    max_values: 1,
                    ..limits
                }
            )
            .is_err()
        );
        assert_eq!(parse("[0,x]").unwrap_err().offset, 3);
    }
}
