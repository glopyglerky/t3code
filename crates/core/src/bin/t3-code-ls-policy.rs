use std::io::{self, BufReader, Write};
use t3_code_ls_core::command_policy::{MAX_FRAME_BYTES, read_frame, response};
use t3_code_ls_core::{command_policy, dispatch};

fn answer(frame: &str) -> Result<String, &'static str> {
    if frame.starts_with("t3ls-dispatch-v1\t") || frame.starts_with("t3ls-goal-v1\t") {
        dispatch::evaluate(frame).map(|value| format!("ok\t{value}\n"))
    } else {
        command_policy::evaluate(frame).map(|decision| response(Ok(decision)))
    }
}

fn write_response(output: &mut impl Write, frame: &str) -> Result<(), &'static str> {
    let result = answer(frame);
    let text = match &result {
        Ok(text) => text.clone(),
        Err(error) => format!("invalid\t{error}\n"),
    };
    output
        .write_all(text.as_bytes())
        .map_err(|_| "cannot write policy response")?;
    output.flush().map_err(|_| "cannot flush policy response")?;
    result.map(|_| ())
}

fn run() -> Result<(), &'static str> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut output = io::stdout().lock();
    if !args.is_empty() {
        if args.len() > 12
            || args.iter().map(String::len).sum::<usize>() + args.len() - 1 > MAX_FRAME_BYTES
        {
            return Err("policy arguments exceed the protocol limit");
        }
        return write_response(&mut output, &args.join("\t"));
    }
    let mut input = BufReader::new(io::stdin().lock());
    while let Some(frame) = read_frame(&mut input).map_err(|_| "invalid policy input frame")? {
        write_response(&mut output, &frame)?;
    }
    Ok(())
}

fn main() {
    if let Err(error) = run() {
        eprintln!("t3-code-ls-policy: {error}");
        std::process::exit(2);
    }
}
