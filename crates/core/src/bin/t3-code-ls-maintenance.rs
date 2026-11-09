use std::io::{self, Read, Write};
use t3_code_ls_core::maintenance_wire::{Error, MAX_FRAME_BYTES, MAX_INPUT_BYTES, Session};

fn run() -> Result<(), Error> {
    if std::env::args().count() != 1 {
        return Err(Error::InvalidRequest);
    }
    let mut session = Session::new();
    let mut input = io::stdin().lock();
    let mut output = io::stdout().lock();
    let mut buffered = Vec::new();
    let mut chunk = [0u8; 8192];
    let mut total = 0;
    loop {
        let count = input.read(&mut chunk).map_err(|_| Error::Io)?;
        if count == 0 {
            return Err(Error::InvalidRequest);
        }
        total += count;
        if total > MAX_INPUT_BYTES {
            return Err(Error::InputLimit);
        }
        buffered.extend_from_slice(&chunk[..count]);
        while let Some(index) = buffered.iter().position(|byte| *byte == b'\n') {
            let length = index + 1;
            if length > MAX_FRAME_BYTES {
                return Err(Error::InputLimit);
            }
            let frame =
                std::str::from_utf8(&buffered[..length]).map_err(|_| Error::InvalidRequest)?;
            let (response, completed) = session.respond(frame)?;
            if completed && length != buffered.len() {
                return Err(Error::InvalidRequest);
            }
            output
                .write_all(response.as_bytes())
                .map_err(|_| Error::Io)?;
            output.flush().map_err(|_| Error::Io)?;
            buffered.drain(..length);
            if completed {
                return Ok(());
            }
        }
        if buffered.len() >= MAX_FRAME_BYTES {
            return Err(Error::InputLimit);
        }
    }
}

fn main() {
    if let Err(error) = run() {
        eprintln!("t3-code-ls-maintenance: {}", error.message());
        std::process::exit(2);
    }
}
