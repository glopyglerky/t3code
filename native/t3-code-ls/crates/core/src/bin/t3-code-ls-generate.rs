#[cfg(unix)]
mod app {
    use std::io::{self, Read, Write};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::time::{Duration, Instant};
    use t3_code_ls_core::codex_generation;
    use t3_code_ls_core::codex_generation_wire::{self, Error, MAX_REQUEST_BYTES};

    pub fn run() -> Result<i32, Error> {
        if std::env::args().count() != 1 {
            return Err(Error::InvalidRequest);
        }
        let interrupted = Arc::new(AtomicUsize::new(0));
        for signal in [signal_hook::consts::SIGINT, signal_hook::consts::SIGTERM] {
            signal_hook::flag::register_usize(signal, interrupted.clone(), signal as usize)
                .map_err(|_| Error::Io)?;
        }
        let (sender, receiver) = mpsc::sync_channel(1);
        std::thread::Builder::new()
            .name("t3ls-generation-input".to_owned())
            .spawn(move || {
                let result = (|| {
                    let mut input = io::stdin().lock();
                    let mut bytes = Vec::new();
                    let mut buffer = [0u8; 8192];
                    loop {
                        let count = input.read(&mut buffer).map_err(|_| Error::Io)?;
                        if count == 0 {
                            break;
                        }
                        if bytes.len() + count > MAX_REQUEST_BYTES {
                            return Err(Error::InputLimit);
                        }
                        bytes.extend_from_slice(&buffer[..count]);
                    }
                    String::from_utf8(bytes).map_err(|_| Error::InvalidRequest)
                })();
                let _ = sender.send(result);
            })
            .map_err(|_| Error::Io)?;
        let input_deadline = Instant::now() + Duration::from_secs(10);
        let frame = loop {
            let signal = interrupted.load(Ordering::SeqCst);
            if signal != 0 {
                return Ok(128 + signal as i32);
            }
            if Instant::now() >= input_deadline {
                return Err(Error::InvalidRequest);
            }
            match receiver.recv_timeout(Duration::from_millis(10)) {
                Ok(result) => break result?,
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => return Err(Error::Io),
            }
        };
        let request = codex_generation_wire::parse(&frame)?;
        let result = codex_generation::process::run(&request, &interrupted);
        let signal = interrupted.load(Ordering::SeqCst);
        if signal != 0 {
            return Ok(128 + signal as i32);
        }
        let response = codex_generation_wire::response(result)?;
        let mut output = io::stdout().lock();
        output
            .write_all(response.as_bytes())
            .map_err(|_| Error::Io)?;
        output.flush().map_err(|_| Error::Io)?;
        Ok(0)
    }
}

#[cfg(unix)]
fn main() {
    match app::run() {
        Ok(0) => {}
        Ok(code) => std::process::exit(code),
        Err(error) => {
            eprintln!("t3-code-ls-generate: {}", error.message());
            std::process::exit(2);
        }
    }
}

#[cfg(not(unix))]
fn main() {
    eprintln!("t3-code-ls-generate requires Unix process support on this target");
    std::process::exit(2);
}
