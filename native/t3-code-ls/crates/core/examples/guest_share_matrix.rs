use std::io::{self, BufRead};
use t3_code_ls_core::guest_share::Timestamp;
use t3_code_ls_core::guest_share_wire;
use t3_code_ls_core::json::{self, Value};

fn main() {
    for input in io::stdin().lock().lines() {
        let input = input.expect("read matrix input");
        let value = json::parse(&input).expect("parse matrix input");
        let output = if value.get("operation").and_then(Value::as_str) == Some("timestamp") {
            let time = value
                .get("now")
                .and_then(Value::as_i64)
                .and_then(Timestamp::from_epoch_millis)
                .expect("fixture time");
            Value::Object(vec![("iso".to_string(), Value::String(time.format_iso()))])
        } else {
            guest_share_wire::evaluate(&value).expect("evaluate matrix fixture")
        };
        println!("{}", json::stringify(&output));
    }
}
