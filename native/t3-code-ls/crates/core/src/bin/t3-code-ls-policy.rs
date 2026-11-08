use std::io::{self, BufReader, Write};
use t3_code_ls_core::command_policy::{MAX_FRAME_BYTES, evaluate, read_frame, response};

fn run() -> Result<(), &'static str> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut output = io::stdout().lock();
    if !args.is_empty() {
        if args.len() > 8 || args.iter().map(String::len).sum::<usize>() > MAX_FRAME_BYTES {
            return Err("policy arguments exceed the protocol limit");
        }
        let result = evaluate(&args.join("\t"));
        output
            .write_all(response(result).as_bytes())
            .map_err(|_| "cannot write policy response")?;
        output.flush().map_err(|_| "cannot flush policy response")?;
        return result.map(|_| ());
    }
    let mut input = BufReader::new(io::stdin().lock());
    while let Some(frame) = read_frame(&mut input).map_err(|_| "invalid policy input frame")? {
        let result = evaluate(&frame);
        output
            .write_all(response(result).as_bytes())
            .map_err(|_| "cannot write policy response")?;
        output.flush().map_err(|_| "cannot flush policy response")?;
        result?;
    }
    Ok(())
}

fn main() {
    if let Err(error) = run() {
        eprintln!("t3-code-ls-policy: {error}");
        std::process::exit(2);
    }
}
