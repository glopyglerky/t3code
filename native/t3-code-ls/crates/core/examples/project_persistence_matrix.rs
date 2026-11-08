use std::io::{self, BufRead};
use t3_code_ls_core::json;
use t3_code_ls_core::project_persistence_wire;

fn main() {
    for input in io::stdin().lock().lines() {
        let input = input.expect("read matrix input");
        let value = json::parse(&input).expect("parse matrix input");
        let output = project_persistence_wire::evaluate(&value).expect("evaluate matrix fixture");
        println!("{}", json::stringify(&output));
    }
}
