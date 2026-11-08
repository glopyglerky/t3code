#[cfg(unix)]
mod app {
    use std::io::{self, BufReader, Write};
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc::{self, TryRecvError, TrySendError};
    use std::time::{Duration, Instant};
    use t3_code_ls_core::json;
    use t3_code_ls_core::provider_process::{
        Config, Error, Event, Limits, ProviderProcess, decode_frame, error_response,
        is_blank_frame, read_frame,
    };

    const USAGE: &str = "Usage: t3-code-ls-provider --program /absolute/provider --cwd /workspace --home /isolated/home [--inherit-env] [--env KEY=VALUE] [--max-frame-bytes N] [--max-pending N] [--queue-size N] [--request-timeout-ms N] -- [provider arguments]\n\nReads and writes Codex app-server JSONL on stdin/stdout. Start Codex with provider argument app-server. By default the child receives only the explicit environment; --inherit-env forwards the wrapper process environment without putting secrets in argv. HOME always uses the selected home directory.";

    enum Input {
        Frame(String),
        Closed,
        Failed(Error),
    }

    fn config() -> Result<Option<Config>, String> {
        let args: Vec<String> = std::env::args().skip(1).collect();
        if args.len() == 1 && args[0] == "--help" {
            println!("{USAGE}");
            return Ok(None);
        }
        let mut program = None;
        let mut cwd = None;
        let mut home = None;
        let mut explicit_environment = Vec::new();
        let mut inherit_environment = false;
        let mut limits = Limits::default();
        let mut index = 0;
        while index < args.len() && args[index] != "--" {
            let key = &args[index];
            if key == "--inherit-env" {
                inherit_environment = true;
                index += 1;
                continue;
            }
            let value = args.get(index + 1).ok_or(USAGE.to_owned())?;
            match key.as_str() {
                "--program" => program = Some(PathBuf::from(value)),
                "--cwd" => cwd = Some(PathBuf::from(value)),
                "--home" => home = Some(PathBuf::from(value)),
                "--env" => {
                    let (key, value) = value.split_once('=').ok_or("--env requires KEY=VALUE")?;
                    explicit_environment.push((key.to_owned(), value.to_owned()));
                }
                "--max-frame-bytes" => {
                    limits.max_frame_bytes = value.parse().map_err(|_| "invalid frame limit")?
                }
                "--max-pending" => {
                    limits.max_pending_requests =
                        value.parse().map_err(|_| "invalid request limit")?
                }
                "--queue-size" => {
                    limits.max_queued_messages = value.parse().map_err(|_| "invalid queue limit")?
                }
                "--request-timeout-ms" => {
                    limits.request_timeout = Duration::from_millis(
                        value.parse().map_err(|_| "invalid request timeout")?,
                    );
                }
                _ => return Err(USAGE.to_owned()),
            }
            index += 2;
        }
        let args = if index < args.len() {
            args[index + 1..].to_vec()
        } else {
            Vec::new()
        };
        // Credentials stay in the wrapper environment, not in child argv.
        // HOME remains the explicitly selected directory in both modes.
        let mut environment: Vec<(String, String)> = if inherit_environment {
            std::env::vars().filter(|(key, _)| key != "HOME").collect()
        } else {
            Vec::new()
        };
        environment.extend(explicit_environment);
        Ok(Some(Config {
            program: program.ok_or(USAGE.to_owned())?,
            cwd: cwd.ok_or(USAGE.to_owned())?,
            home: home.ok_or(USAGE.to_owned())?,
            args,
            environment,
            limits,
        }))
    }

    pub fn run() -> Result<i32, String> {
        let Some(config) = config()? else {
            return Ok(0);
        };
        let interrupted = Arc::new(AtomicUsize::new(0));
        for signal in [signal_hook::consts::SIGINT, signal_hook::consts::SIGTERM] {
            signal_hook::flag::register_usize(signal, interrupted.clone(), signal as usize)
                .map_err(|_| "could not install provider shutdown handler")?;
        }
        let frame_limit = config.limits.max_frame_bytes;
        let queue_limit = config.limits.max_queued_messages;
        let mut process = ProviderProcess::spawn(config).map_err(|error| error.to_string())?;
        let (sender, receiver) = mpsc::sync_channel(queue_limit);
        // This reader lasts for the CLI process; provider-owned workers are joined by shutdown.
        std::thread::Builder::new()
            .name("t3ls-client-input".to_owned())
            .spawn(move || {
                let mut input = BufReader::new(io::stdin().lock());
                loop {
                    let message = match read_frame(&mut input, frame_limit) {
                        Ok(Some(frame)) if is_blank_frame(&frame) => continue,
                        Ok(Some(frame)) => Input::Frame(frame),
                        Ok(None) => {
                            let _ = sender.send(Input::Closed);
                            break;
                        }
                        Err(error) => {
                            let _ = sender.send(Input::Failed(error));
                            break;
                        }
                    };
                    if sender.send(message).is_err() {
                        break;
                    }
                }
            })
            .map_err(|_| "could not start client input reader")?;
        let (output_sender, output_receiver) = mpsc::sync_channel::<String>(queue_limit);
        let (output_done_sender, output_done) = mpsc::sync_channel(1);
        let (output_progress_sender, output_progress) = mpsc::sync_channel(1);
        let output_worker = std::thread::Builder::new()
            .name("t3ls-client-output".to_owned())
            .spawn(move || {
                let mut output = io::stdout().lock();
                while let Ok(frame) = output_receiver.recv() {
                    let _ = output_progress_sender.try_send(());
                    if output.write_all(frame.as_bytes()).is_err() || output.flush().is_err() {
                        let _ = output_done_sender.send(false);
                        return;
                    }
                }
                let _ = output_done_sender.send(true);
            })
            .map_err(|_| "could not start client output writer")?;
        let mut input_closed = false;
        let mut input_close_deadline = None;
        let mut session_failed = false;
        let mut pending_output = None;
        loop {
            let signal = interrupted.load(Ordering::SeqCst);
            if signal != 0 {
                process.shutdown().map_err(|error| error.to_string())?;
                return Ok(128 + signal as i32);
            }
            if output_done.try_recv().is_ok() {
                return Err("client output closed unexpectedly".to_owned());
            }
            if input_close_deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                return Err("provider did not exit after its input closed".to_owned());
            }
            if !input_closed {
                match receiver.try_recv() {
                    Ok(Input::Frame(frame)) => {
                        let message =
                            decode_frame(&frame, frame_limit).map_err(|error| error.to_string())?;
                        process
                            .send(message.value)
                            .map_err(|error| error.to_string())?;
                    }
                    Ok(Input::Closed) => {
                        input_closed = true;
                        input_close_deadline = Some(Instant::now() + Duration::from_secs(2));
                        process.close_input();
                    }
                    Ok(Input::Failed(error)) => return Err(error.to_string()),
                    Err(TryRecvError::Disconnected) => {
                        return Err("client input reader stopped unexpectedly".to_owned());
                    }
                    Err(TryRecvError::Empty) => {}
                }
            }
            if let Some(frame) = pending_output.take() {
                match output_sender.try_send(frame) {
                    Ok(()) => {}
                    Err(TrySendError::Full(frame)) => {
                        pending_output = Some(frame);
                        let _ = output_progress.recv_timeout(Duration::from_millis(10));
                        continue;
                    }
                    Err(TrySendError::Disconnected(_)) => {
                        return Err("client output is closed".to_owned());
                    }
                }
            }
            match process.recv_timeout(Duration::from_millis(10)) {
                Ok(Some(event)) => {
                    let value = match event {
                        Event::Message(message) => message.value,
                        Event::RequestFailed { id, error } => {
                            session_failed = true;
                            error_response(&id, -32000, &error.to_string())
                        }
                    };
                    let frame = format!("{}\n", json::stringify(&value));
                    pending_output = Some(frame);
                }
                Ok(None) => {}
                Err(Error::OutputClosed) => {
                    let incomplete = process.pending_requests() != 0;
                    let status = process
                        .wait_for_exit(Duration::from_millis(250))
                        .map_err(|error| error.to_string())?;
                    process.shutdown().map_err(|error| error.to_string())?;
                    drop(output_sender);
                    if output_done.recv_timeout(Duration::from_secs(2)) != Ok(true) {
                        return Err("client did not drain the response queue".to_owned());
                    }
                    output_worker
                        .join()
                        .map_err(|_| "client output writer failed")?;
                    return if !incomplete
                        && !session_failed
                        && status.is_some_and(|status| status.success())
                    {
                        Ok(0)
                    } else {
                        Err("provider exited before completing the session".to_owned())
                    };
                }
                Err(error) => {
                    process.shutdown().map_err(|failure| failure.to_string())?;
                    drop(output_sender);
                    if output_done.recv_timeout(Duration::from_secs(2)) != Ok(true) {
                        return Err("client did not drain the response queue".to_owned());
                    }
                    output_worker
                        .join()
                        .map_err(|_| "client output writer failed")?;
                    return Err(error.to_string());
                }
            }
        }
    }
}

#[cfg(unix)]
fn main() {
    match app::run() {
        Ok(0) => {}
        Ok(code) => std::process::exit(code),
        Err(error) => {
            eprintln!("t3-code-ls-provider: {error}");
            std::process::exit(2);
        }
    }
}

#[cfg(not(unix))]
fn main() {
    eprintln!("t3-code-ls-provider requires Unix socket stdio support on this target");
    std::process::exit(2);
}
