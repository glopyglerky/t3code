use std::io::{self, BufRead, BufReader, Read, Write};
use t3_code_ls_core::json::{Limits, Value, parse_with_limits, stringify};

fn main() {
    let mut input = BufReader::new(io::stdin().lock());
    let mut output = io::stdout().lock();
    loop {
        let mut header = String::new();
        if input.read_line(&mut header).unwrap() == 0 {
            return;
        }
        let fields: Vec<&str> = header.trim_end().split('\t').collect();
        assert_eq!(fields.len(), 5);
        let size: usize = fields[1].parse().unwrap();
        assert!(size <= 16 * 1024 * 1024);
        let limits = Limits {
            max_bytes: fields[2].parse().unwrap(),
            max_depth: fields[3].parse().unwrap(),
            max_values: fields[4].parse().unwrap(),
        };
        let mut bytes = vec![0; size];
        input.read_exact(&mut bytes).unwrap();
        let text = String::from_utf8(bytes).unwrap();
        let value = if fields[0] == "number" {
            Ok(Value::Number(text))
        } else {
            parse_with_limits(&text, limits)
        };
        match value {
            Ok(value) => {
                let encoded = if fields[0] == "get" {
                    match value.get("probe") {
                        Some(value) => stringify(value),
                        None => {
                            output.write_all(b"missing\n").unwrap();
                            continue;
                        }
                    }
                } else {
                    assert!(fields[0] == "value" || fields[0] == "number");
                    stringify(&value)
                };
                writeln!(output, "ok\t{encoded}").unwrap();
            }
            Err(error) => writeln!(output, "error\t{}\t{}", error.offset, error.message).unwrap(),
        }
    }
}
