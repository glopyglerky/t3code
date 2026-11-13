//! Codex structured generation, ported from the upstream CodexTextGeneration runner.
//! Copyright (c) 2026 T3 Tools Inc. Distributed under the repository's MIT license.

use crate::json::{self, Value};

pub const MAX_PROMPT_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_SCHEMA_BYTES: usize = 1024 * 1024;
pub const MAX_OUTPUT_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_MCP_BYTES: usize = 1024 * 1024;
pub const MAX_STDOUT_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_STDERR_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Model {
    pub slug: String,
    pub custom: bool,
    pub default: bool,
    pub legacy: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Request {
    pub binary: String,
    pub cwd: String,
    pub prompt: String,
    pub output_schema: Value,
    pub launch_args: String,
    pub models: Vec<Model>,
    pub custom_models: Value,
    pub model: String,
    pub options: Vec<(String, Value)>,
    pub managed: bool,
    pub image_paths: Vec<String>,
    pub timeout_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Error {
    pub kind: &'static str,
    pub detail: String,
}

impl Error {
    pub fn new(kind: &'static str, detail: &str) -> Self {
        Self {
            kind,
            detail: detail.to_owned(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Selection {
    pub model: String,
    pub reasoning: String,
    pub service_tier: Option<String>,
    pub launch_args: Vec<String>,
    pub mcp_launch_args: Vec<String>,
}

fn js_space(character: char) -> bool {
    matches!(
        character,
        '\u{0009}'..='\u{000d}'
            | '\u{0020}'
            | '\u{00a0}'
            | '\u{1680}'
            | '\u{2000}'..='\u{200a}'
            | '\u{2028}'
            | '\u{2029}'
            | '\u{202f}'
            | '\u{205f}'
            | '\u{3000}'
            | '\u{feff}'
    )
}

pub fn js_trim(value: &str) -> &str {
    value.trim_matches(js_space)
}

pub fn tokenize_launch_args(input: &str) -> Vec<String> {
    let characters: Vec<char> = input.chars().collect();
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut quote = None;
    let mut quoted = false;
    let mut index = 0;
    while index < characters.len() {
        let character = characters[index];
        let next = characters.get(index + 1).copied();
        if let Some(open) = quote {
            if character == open {
                quote = None;
                quoted = true;
            } else if character == '\\'
                && open == '"'
                && next.is_some_and(|value| matches!(value, '"' | '\\' | '$' | '`'))
            {
                current.push(next.unwrap());
                index += 1;
            } else {
                current.push(character);
            }
        } else if matches!(character, '\'' | '"') {
            quote = Some(character);
            quoted = true;
        } else if js_space(character) {
            if !current.is_empty() || quoted {
                tokens.push(current);
                current = String::new();
                quoted = false;
            }
        } else if character == '\\'
            && next.is_some_and(|value| js_space(value) || matches!(value, '\'' | '"'))
        {
            current.push(next.unwrap());
            index += 1;
        } else {
            current.push(character);
        }
        index += 1;
    }
    if !current.is_empty() || quoted {
        tokens.push(current);
    }
    tokens
}

fn filter_launch_args(input: &str, mcp: bool) -> Vec<String> {
    let tokens = tokenize_launch_args(input);
    let mut result = Vec::new();
    let mut index = 0;
    while index < tokens.len() {
        let token = &tokens[index];
        let switch = !mcp && matches!(token.as_str(), "--strict-config" | "--oss");
        let value_flag = matches!(
            token.as_str(),
            "--config" | "-c" | "--enable" | "--disable" | "--profile" | "-p"
        ) || (!mcp && token == "--local-provider");
        let inline = [
            "--config=",
            "-c=",
            "--enable=",
            "--disable=",
            "--profile=",
            "-p=",
        ]
        .iter()
        .any(|prefix| token.starts_with(prefix))
            || (!mcp && token.starts_with("--local-provider="));
        if switch {
            result.push(token.clone());
        } else if value_flag {
            if let Some(value) = tokens.get(index + 1)
                && !value.starts_with('-')
            {
                result.push(token.clone());
                result.push(value.clone());
                index += 1;
            }
        } else if inline {
            result.push(token.clone());
        }
        index += 1;
    }
    result
}

pub fn exec_launch_args(input: &str) -> Vec<String> {
    filter_launch_args(input, false)
}

pub fn mcp_launch_args(input: &str) -> Vec<String> {
    filter_launch_args(input, true)
}

fn family(model: &str) -> &str {
    if model.starts_with("openai.gpt-") {
        &model[7..]
    } else {
        model
    }
}

fn custom_match(custom: &Value, requested: &str) -> bool {
    custom.as_array().is_some_and(|values| {
        values.iter().any(|value| {
            value
                .as_str()
                .or_else(|| value.get("slug").and_then(Value::as_str))
                .is_some_and(|slug| !js_trim(slug).is_empty() && js_trim(slug) == requested)
        })
    })
}

fn option<'a>(request: &'a Request, name: &str) -> Option<&'a Value> {
    request
        .options
        .iter()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value)
}

pub fn select(request: &Request, environment_launch_args: Option<&str>) -> Selection {
    let exact = request
        .models
        .iter()
        .find(|candidate| candidate.slug == request.model)
        .map(|candidate| candidate.slug.as_str());
    let custom =
        custom_match(&request.custom_models, &request.model).then_some(request.model.as_str());
    let family_match = request
        .models
        .iter()
        .find(|candidate| !candidate.custom && family(&candidate.slug) == family(&request.model))
        .map(|candidate| candidate.slug.as_str());
    let matched = exact.or(custom).or(family_match);
    let model = matched
        .or_else(|| {
            request
                .models
                .iter()
                .find(|candidate| !candidate.custom && candidate.default)
                .map(|candidate| candidate.slug.as_str())
        })
        .or_else(|| {
            request
                .models
                .iter()
                .find(|candidate| !candidate.custom && !candidate.legacy)
                .map(|candidate| candidate.slug.as_str())
        })
        .or_else(|| {
            request
                .models
                .iter()
                .find(|candidate| !candidate.custom)
                .map(|candidate| candidate.slug.as_str())
        })
        .unwrap_or(&request.model);
    let fallback = matched.is_none() && model != request.model;
    let reasoning = if fallback {
        None
    } else {
        option(request, "reasoningEffort").and_then(Value::as_str)
    }
    .unwrap_or("low")
    .to_owned();
    let service_tier = if fallback || request.managed {
        None
    } else {
        option(request, "serviceTier")
            .and_then(Value::as_str)
            .or_else(|| {
                (option(request, "fastMode").and_then(Value::as_bool) == Some(true))
                    .then_some("fast")
            })
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
    };
    let raw_args = environment_launch_args
        .map(js_trim)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| js_trim(&request.launch_args));
    Selection {
        model: model.to_owned(),
        reasoning,
        service_tier,
        launch_args: exec_launch_args(raw_args),
        mcp_launch_args: mcp_launch_args(raw_args),
    }
}

fn plugin_args(args: &mut Vec<String>) {
    for value in [
        "--config",
        "features.plugins=false",
        "--config",
        "features.apps=false",
    ] {
        args.push(value.to_owned());
    }
}

pub fn mcp_args(selection: &Selection) -> Vec<String> {
    let mut args = selection.mcp_launch_args.clone();
    plugin_args(&mut args);
    args.extend(["mcp".to_owned(), "list".to_owned(), "--json".to_owned()]);
    args
}

pub fn disabled_mcp_config(names: &[String]) -> Option<String> {
    if names.is_empty() {
        return None;
    }
    let entries: Vec<String> = names
        .iter()
        .map(|name| {
            format!(
                "{}={{enabled=false}}",
                json::stringify(&Value::String(name.clone()))
            )
        })
        .collect();
    Some(format!("mcp_servers={{{}}}", entries.join(",")))
}

pub fn exec_args(
    request: &Request,
    selection: &Selection,
    schema_path: &str,
    output_path: &str,
    disabled_mcp: Option<&str>,
) -> Vec<String> {
    let mut args = vec!["exec".to_owned()];
    args.extend(selection.launch_args.clone());
    plugin_args(&mut args);
    if let Some(config) = disabled_mcp {
        args.extend(["--config".to_owned(), config.to_owned()]);
    }
    for value in [
        "--ephemeral",
        "--skip-git-repo-check",
        "-s",
        "read-only",
        "--model",
    ] {
        args.push(value.to_owned());
    }
    args.push(selection.model.clone());
    args.push("--config".to_owned());
    args.push(format!(
        "model_reasoning_effort=\"{}\"",
        selection.reasoning
    ));
    if let Some(tier) = &selection.service_tier {
        args.extend(["--config".to_owned(), format!("service_tier=\"{tier}\"")]);
    }
    args.extend([
        "--output-schema".to_owned(),
        schema_path.to_owned(),
        "--output-last-message".to_owned(),
        output_path.to_owned(),
    ]);
    for path in &request.image_paths {
        args.extend(["--image".to_owned(), path.clone()]);
    }
    args.push("-".to_owned());
    args
}

pub fn enabled_mcp_names(bytes: &[u8]) -> Result<Option<Vec<String>>, Error> {
    let text = std::str::from_utf8(bytes)
        .map_err(|_| Error::new("output", "Codex MCP listing is not UTF-8."))?;
    let text = text.strip_prefix('\u{feff}').unwrap_or(text);
    let listing = match json::parse_with_limits(
        text,
        json::Limits {
            max_bytes: MAX_MCP_BYTES,
            max_depth: 128,
            max_values: 131_072,
        },
    ) {
        Ok(value) => value,
        Err(error) => {
            if error.message.contains("limit") || error.message.contains("surrogate") {
                return Err(Error::new(
                    "limit",
                    "Codex MCP listing exceeds native JSON constraints.",
                ));
            }
            return Ok(None);
        }
    };
    let Some(entries) = listing.as_array() else {
        return Ok(None);
    };
    let mut names = Vec::new();
    for entry in entries {
        let Some(name) = entry.get("name").and_then(Value::as_str) else {
            return Ok(None);
        };
        let Some(enabled) = entry.get("enabled").and_then(Value::as_bool) else {
            return Ok(None);
        };
        if enabled {
            names.push(name.to_owned());
        }
    }
    Ok(Some(names))
}

#[cfg(unix)]
pub mod process {
    use super::{
        Error, MAX_MCP_BYTES, MAX_OUTPUT_BYTES, MAX_STDERR_BYTES, MAX_STDOUT_BYTES, Request,
    };
    use std::fs::{self, DirBuilder, File, OpenOptions};
    use std::io::{self, Read, Write};
    use std::net::Shutdown;
    use std::os::fd::OwnedFd;
    use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
    use std::os::unix::net::UnixStream;
    use std::path::PathBuf;
    use std::process::{Child, Command, ExitStatus, Stdio};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    // Open without following links or blocking on a provider-replaced FIFO.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    const OUTPUT_OPEN_FLAGS: i32 = 0x20000 | 0x800;
    #[cfg(any(
        target_os = "macos",
        target_os = "ios",
        target_os = "freebsd",
        target_os = "openbsd",
        target_os = "netbsd",
        target_os = "dragonfly"
    ))]
    const OUTPUT_OPEN_FLAGS: i32 = 0x100 | 0x4;

    struct Temporary {
        directory: PathBuf,
        schema: PathBuf,
        output: PathBuf,
    }

    impl Temporary {
        fn create(schema: &str) -> Result<Self, Error> {
            let mut random = [0u8; 16];
            let mut source = File::open("/dev/urandom")
                .map_err(|_| Error::new("io", "Failed to create private generation files."))?;
            source
                .read_exact(&mut random)
                .map_err(|_| Error::new("io", "Failed to create private generation files."))?;
            let suffix: String = random.iter().map(|byte| format!("{byte:02x}")).collect();
            let directory = std::env::temp_dir()
                .join(format!("t3ls-generation-{}-{suffix}", std::process::id()));
            DirBuilder::new()
                .mode(0o700)
                .create(&directory)
                .map_err(|_| Error::new("io", "Failed to write temp file"))?;
            let temporary = Self {
                schema: directory.join("schema.json"),
                output: directory.join("output.json"),
                directory,
            };
            for (path, content) in [(&temporary.schema, schema), (&temporary.output, "")] {
                let mut file = OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .mode(0o600)
                    .open(path)
                    .map_err(|_| Error::new("io", "Failed to write temp file"))?;
                file.write_all(content.as_bytes())
                    .map_err(|_| Error::new("io", "Failed to write temp file"))?;
            }
            Ok(temporary)
        }

        fn output(&self) -> Result<String, Error> {
            let observed = fs::symlink_metadata(&self.output)
                .map_err(|_| Error::new("output", "Failed to read Codex output file."))?;
            if !observed.is_file() {
                return Err(Error::new("output", "Codex output is not a regular file."));
            }
            #[cfg(any(
                target_os = "linux",
                target_os = "android",
                target_os = "macos",
                target_os = "ios",
                target_os = "freebsd",
                target_os = "openbsd",
                target_os = "netbsd",
                target_os = "dragonfly"
            ))]
            let mut file = OpenOptions::new()
                .read(true)
                .custom_flags(OUTPUT_OPEN_FLAGS)
                .open(&self.output)
                .map_err(|_| Error::new("output", "Failed to read Codex output file."))?;
            #[cfg(not(any(
                target_os = "linux",
                target_os = "android",
                target_os = "macos",
                target_os = "ios",
                target_os = "freebsd",
                target_os = "openbsd",
                target_os = "netbsd",
                target_os = "dragonfly"
            )))]
            let mut file: File = return Err(Error::new(
                "output",
                "Native structured output is unsupported on this platform.",
            ));
            let metadata = file
                .metadata()
                .map_err(|_| Error::new("output", "Failed to read Codex output file."))?;
            if !metadata.is_file() {
                return Err(Error::new("output", "Codex output is not a regular file."));
            }
            if metadata.len() > MAX_OUTPUT_BYTES as u64 {
                return Err(Error::new(
                    "limit",
                    "Codex structured output exceeds the byte limit.",
                ));
            }
            let mut output = Vec::new();
            let mut buffer = [0u8; 8192];
            loop {
                let count = file
                    .read(&mut buffer)
                    .map_err(|_| Error::new("output", "Failed to read Codex output file."))?;
                if count == 0 {
                    break;
                }
                if output.len() + count > MAX_OUTPUT_BYTES {
                    return Err(Error::new(
                        "limit",
                        "Codex structured output exceeds the byte limit.",
                    ));
                }
                output.extend_from_slice(&buffer[..count]);
            }
            let output = String::from_utf8(output)
                .map_err(|_| Error::new("output", "Codex structured output is not UTF-8."))?;
            Ok(output
                .strip_prefix('\u{feff}')
                .unwrap_or(&output)
                .to_owned())
        }
    }

    impl Drop for Temporary {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.directory);
        }
    }

    struct Running {
        child: Child,
        input: UnixStream,
        output: UnixStream,
        errors: UnixStream,
    }

    impl Drop for Running {
        fn drop(&mut self) {
            for socket in [&self.input, &self.output, &self.errors] {
                let _ = socket.shutdown(Shutdown::Both);
            }
            if !matches!(self.child.try_wait(), Ok(Some(_))) {
                let _ = self.child.kill();
                let _ = self.child.wait();
            }
        }
    }

    struct Captured {
        status: ExitStatus,
        stdout: Vec<u8>,
        stderr: Vec<u8>,
    }

    fn collect(
        socket: &mut UnixStream,
        bytes: &mut Vec<u8>,
        maximum: usize,
    ) -> Result<bool, Error> {
        let mut buffer = [0u8; 8192];
        for _ in 0..32 {
            match socket.read(&mut buffer) {
                Ok(0) => return Ok(true),
                Ok(count) => {
                    if bytes.len() + count > maximum {
                        return Err(Error::new(
                            "limit",
                            "Codex process output exceeds the byte limit.",
                        ));
                    }
                    bytes.extend_from_slice(&buffer[..count]);
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(false),
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(_) => return Err(Error::new("io", "Failed to collect process output")),
            }
        }
        Ok(false)
    }

    fn spawn(request: &Request, args: &[String]) -> Result<Running, Error> {
        let (input, child_input) = UnixStream::pair()
            .map_err(|_| Error::new("spawn", "Failed to spawn Codex CLI process"))?;
        let (output, child_output) = UnixStream::pair()
            .map_err(|_| Error::new("spawn", "Failed to spawn Codex CLI process"))?;
        let (errors, child_errors) = UnixStream::pair()
            .map_err(|_| Error::new("spawn", "Failed to spawn Codex CLI process"))?;
        for socket in [&input, &output, &errors] {
            socket
                .set_nonblocking(true)
                .map_err(|_| Error::new("spawn", "Failed to spawn Codex CLI process"))?;
        }
        let mut command = Command::new(if request.binary.is_empty() {
            "codex"
        } else {
            &request.binary
        });
        command
            .args(args)
            .current_dir(&request.cwd)
            .stdin(Stdio::from(OwnedFd::from(child_input)))
            .stdout(Stdio::from(OwnedFd::from(child_output)))
            .stderr(Stdio::from(OwnedFd::from(child_errors)));
        let child = command.spawn().map_err(|error| {
            Error::new(
                "spawn",
                if error.kind() == io::ErrorKind::NotFound {
                    "Codex CLI (`codex`) is required but not available on PATH."
                } else {
                    "Failed to spawn Codex CLI process"
                },
            )
        })?;
        drop(command);
        Ok(Running {
            child,
            input,
            output,
            errors,
        })
    }

    fn capture(
        request: &Request,
        args: &[String],
        input: &[u8],
        deadline: Instant,
        max_stdout: usize,
        interrupted: &AtomicUsize,
    ) -> Result<Captured, Error> {
        if interrupted.load(Ordering::SeqCst) != 0 {
            return Err(Error::new("io", "Codex generation was interrupted."));
        }
        if Instant::now() >= deadline {
            return Err(Error::new("timeout", "Codex CLI request timed out."));
        }
        let mut process = spawn(request, args)?;
        let mut written = 0;
        let mut closed_input = false;
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let mut output_done = false;
        let mut errors_done = false;
        let mut status = None;
        let mut drain_deadline = None;
        loop {
            if interrupted.load(Ordering::SeqCst) != 0 {
                return Err(Error::new("io", "Codex generation was interrupted."));
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(Error::new("timeout", "Codex CLI request timed out."));
            }
            if !closed_input {
                if written == input.len() {
                    let _ = process.input.shutdown(Shutdown::Write);
                    closed_input = true;
                } else {
                    match process.input.write(&input[written..]) {
                        Ok(0) => closed_input = true,
                        Ok(count) => written += count,
                        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                        Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                        Err(error) if error.kind() == io::ErrorKind::BrokenPipe => {
                            closed_input = true
                        }
                        Err(_) => return Err(Error::new("io", "Failed to write Codex input.")),
                    }
                }
            }
            if !output_done {
                output_done = collect(&mut process.output, &mut stdout, max_stdout)?;
            }
            if !errors_done {
                errors_done = collect(&mut process.errors, &mut stderr, MAX_STDERR_BYTES)?;
            }
            if status.is_none() {
                status = process
                    .child
                    .try_wait()
                    .map_err(|_| Error::new("io", "Failed to read Codex CLI exit code"))?;
                if status.is_some() {
                    drain_deadline = Some(now + Duration::from_millis(250));
                }
            }
            if let Some(exit) = status
                && (output_done && errors_done || drain_deadline.is_some_and(|at| now >= at))
            {
                return Ok(Captured {
                    status: exit,
                    stdout,
                    stderr,
                });
            }
            std::thread::sleep(Duration::from_millis(2));
        }
    }

    pub fn run(request: &Request, interrupted: &AtomicUsize) -> Result<String, Error> {
        if interrupted.load(Ordering::SeqCst) != 0 {
            return Err(Error::new("io", "Codex generation was interrupted."));
        }
        let schema = crate::json::stringify(&request.output_schema);
        let temporary = Temporary::create(&schema)?;
        let deadline = Instant::now() + Duration::from_millis(request.timeout_ms);
        let environment = match std::env::var("T3CODE_CODEX_LAUNCH_ARGS") {
            Ok(value) if value.len() <= 65_536 => Some(value),
            Ok(_) => {
                return Err(Error::new(
                    "limit",
                    "Codex launch arguments exceed the byte limit.",
                ));
            }
            Err(std::env::VarError::NotPresent) => None,
            Err(std::env::VarError::NotUnicode(_)) => {
                return Err(Error::new("io", "Codex launch arguments are not UTF-8."));
            }
        };
        let selection = super::select(request, environment.as_deref());
        let mcp_deadline = (Instant::now() + Duration::from_secs(10)).min(deadline);
        let mcp = capture(
            request,
            &super::mcp_args(&selection),
            &[],
            mcp_deadline,
            MAX_MCP_BYTES,
            interrupted,
        );
        if interrupted.load(Ordering::SeqCst) != 0 {
            return Err(Error::new("io", "Codex generation was interrupted."));
        }
        let names = match mcp {
            Ok(result) if result.status.success() => {
                super::enabled_mcp_names(&result.stdout)?.unwrap_or_default()
            }
            Err(error) if error.kind == "limit" => return Err(error),
            _ => Vec::new(),
        };
        if Instant::now() >= deadline {
            return Err(Error::new("timeout", "Codex CLI request timed out."));
        }
        let schema_path = temporary
            .schema
            .to_str()
            .ok_or_else(|| Error::new("io", "Generation temporary path is not UTF-8."))?;
        let output_path = temporary
            .output
            .to_str()
            .ok_or_else(|| Error::new("io", "Generation temporary path is not UTF-8."))?;
        let disabled = super::disabled_mcp_config(&names);
        let args = super::exec_args(
            request,
            &selection,
            schema_path,
            output_path,
            disabled.as_deref(),
        );
        let result = capture(
            request,
            &args,
            request.prompt.as_bytes(),
            deadline,
            MAX_STDOUT_BYTES,
            interrupted,
        )?;
        if result.status.code().is_none() {
            return Err(Error::new("io", "Failed to read Codex CLI exit code"));
        }
        if !result.status.success() {
            let stderr = String::from_utf8_lossy(&result.stderr);
            let stdout = String::from_utf8_lossy(&result.stdout);
            let detail = if super::js_trim(&stderr).is_empty() {
                super::js_trim(&stdout)
            } else {
                super::js_trim(&stderr)
            };
            return Err(Error {
                kind: "exit",
                detail: if detail.is_empty() {
                    format!(
                        "Codex CLI command failed with code {}.",
                        result.status.code().unwrap_or(1)
                    )
                } else {
                    format!("Codex CLI command failed: {detail}")
                },
            });
        }
        temporary.output()
    }
}
