use std::io::{self, Read, Write};
use t3_code_ls_core::guest_share_wire::{self, Error, MAX_FRAME_BYTES};

fn run() -> Result<(), Error> {
    if std::env::args().count() != 1 {
        return Err(Error::InvalidRequest);
    }
    let mut bytes = Vec::new();
    let mut input = io::stdin().lock();
    let mut chunk = [0u8; 8192];
    loop {
        let count = input.read(&mut chunk).map_err(|_| Error::Io)?;
        if count == 0 {
            break;
        }
        if bytes.len() + count > MAX_FRAME_BYTES {
            return Err(Error::InputLimit);
        }
        bytes.extend_from_slice(&chunk[..count]);
    }
    let request = String::from_utf8(bytes).map_err(|_| Error::InvalidRequest)?;
    let response = guest_share_wire::respond(&request)?;
    let mut output = io::stdout().lock();
    output
        .write_all(response.as_bytes())
        .map_err(|_| Error::Io)?;
    output.flush().map_err(|_| Error::Io)
}

fn main() {
    if let Err(error) = run() {
        eprintln!("t3-code-ls-guest-share: {}", error.message());
        std::process::exit(2);
    }
}
