#![cfg(unix)]

use std::io;
use std::net::SocketAddr;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::Path;
use std::process::{Output, Stdio};
use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStderr, ChildStdout, Command};
use tokio::time::timeout;

const DEADLINE: Duration = Duration::from_secs(20);
const OUTPUT_LIMIT: u64 = 1024 * 1024;

fn check(condition: bool, message: &str) -> io::Result<()> {
    if condition {
        Ok(())
    } else {
        Err(io::Error::other(message))
    }
}

fn cli(directory: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_t3-code-ls-app"));
    command
        .current_dir(directory)
        .env_clear()
        .env("HOME", directory)
        .env("XDG_CONFIG_HOME", directory.join("config"))
        .env("XDG_CACHE_HOME", directory.join("cache"))
        .stdin(Stdio::null());
    command
}

async fn read_bounded(reader: &mut (impl AsyncRead + Unpin)) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    reader
        .take(OUTPUT_LIMIT + 1)
        .read_to_end(&mut bytes)
        .await?;
    check(
        bytes.len() as u64 <= OUTPUT_LIMIT,
        "child output exceeded the test bound",
    )?;
    Ok(bytes)
}

struct OwnedProcess {
    child: Child,
    stdout: BufReader<ChildStdout>,
    stderr: ChildStderr,
}

impl OwnedProcess {
    fn spawn(mut command: Command) -> io::Result<Self> {
        command
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = command.spawn()?;
        let stdout = child.stdout.take().expect("stdout was explicitly piped");
        let stderr = child.stderr.take().expect("stderr was explicitly piped");
        Ok(Self {
            child,
            stdout: BufReader::new(stdout),
            stderr,
        })
    }

    /// Every failure path kills and waits on this exact owned child.
    async fn reap(&mut self) -> io::Result<()> {
        if self.child.try_wait()?.is_none() {
            let _ = self.child.start_kill();
        }
        timeout(DEADLINE, self.child.wait())
            .await
            .map_err(|_| io::Error::other("owned child did not exit after cleanup"))??;
        Ok(())
    }

    async fn output(&mut self, input: Option<&[u8]>) -> io::Result<Output> {
        let result = timeout(DEADLINE, async {
            if let Some(input) = input {
                let mut stdin = self
                    .child
                    .stdin
                    .take()
                    .ok_or_else(|| io::Error::other("request stdin is unavailable"))?;
                stdin.write_all(input).await?;
                stdin.shutdown().await?;
            }
            drop(self.child.stdin.take());
            let (stdout, stderr, status) = tokio::try_join!(
                read_bounded(&mut self.stdout),
                read_bounded(&mut self.stderr),
                self.child.wait()
            )?;
            Ok(Output {
                status,
                stdout,
                stderr,
            })
        })
        .await
        .unwrap_or_else(|_| {
            Err(io::Error::other(
                "CLI operation did not finish before its deadline",
            ))
        });
        if result.is_err() {
            self.reap().await?;
        }
        result
    }

    async fn listen_address(&mut self) -> io::Result<SocketAddr> {
        let mut line = String::new();
        let read = timeout(DEADLINE, (&mut self.stdout).take(4096).read_line(&mut line))
            .await
            .map_err(|_| io::Error::other("server did not announce its listening address"))??;
        check(
            read > 0 && line.ends_with('\n'),
            "server exited without a complete listening announcement",
        )?;
        let value: Value = serde_json::from_str(&line).map_err(io::Error::other)?;
        check(
            value["protocolVersion"] == 1,
            "unexpected public CLI protocol version",
        )?;
        let address: SocketAddr = value["listen"]
            .as_str()
            .ok_or_else(|| io::Error::other("listen address is absent"))?
            .parse()
            .map_err(io::Error::other)?;
        check(
            address.ip().is_loopback() && address.port() > 0,
            "server did not bind an ephemeral loopback port",
        )?;
        Ok(address)
    }

    async fn terminate(&mut self) -> io::Result<()> {
        check(
            self.child.try_wait()?.is_none(),
            "server exited before SIGTERM",
        )?;
        let pid = self
            .child
            .id()
            .ok_or_else(|| io::Error::other("owned server has no process ID"))?;
        let mut signal = Command::new("/bin/kill");
        signal
            .arg("-TERM")
            .arg(pid.to_string())
            .env_clear()
            .stdin(Stdio::null());
        let output = OwnedProcess::spawn(signal)?.output(None).await?;
        check(
            output.status.success(),
            "SIGTERM could not be sent to the owned server",
        )?;
        let output = self.output(None).await?;
        check(
            output.status.success(),
            &format!(
                "server did not shut down successfully: {}",
                String::from_utf8_lossy(&output.stderr)
            ),
        )
    }
}

async fn successful(mut command: Command, input: Option<&[u8]>) -> io::Result<Value> {
    if input.is_some() {
        command.stdin(Stdio::piped());
    }
    let output = OwnedProcess::spawn(command)?.output(input).await?;
    check(
        output.status.success(),
        &format!(
            "CLI command failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ),
    )?;
    serde_json::from_slice(&output.stdout).map_err(io::Error::other)
}

async fn request(
    directory: &Path,
    state: &Path,
    address: SocketAddr,
    method: &str,
    params: Value,
) -> io::Result<Value> {
    let mut command = cli(directory);
    command
        .arg("request")
        .arg("--address")
        .arg(address.to_string())
        .arg("--token-file")
        .arg(state.join("api-token"));
    let bytes = serde_json::to_vec(
        &json!({"jsonrpc":"2.0","id":"cli-test","method":method,"params":params}),
    )
    .map_err(io::Error::other)?;
    let value = successful(command, Some(&bytes)).await?;
    check(
        value["jsonrpc"] == "2.0" && value["id"] == "cli-test" && value.get("error").is_none(),
        "CLI request did not preserve a successful RPC envelope",
    )?;
    value
        .get("result")
        .cloned()
        .ok_or_else(|| io::Error::other("CLI result is absent"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_cli_initializes_owns_stops_and_restarts_persisted_state() {
    let directory = tempfile::tempdir().unwrap();
    let state = directory.path().join("application-state");
    let workspace = directory.path().join("project");
    let mut servers = Vec::<OwnedProcess>::new();
    let result: io::Result<()> = async {
        std::fs::create_dir(&workspace)?;
        let mut initialize = cli(directory.path());
        initialize.arg("initialize").arg("--state").arg(&state);
        let initialized = successful(initialize, None).await?;
        let canonical = state.canonicalize()?;
        check(initialized["state"] == json!(canonical), "initialize did not return its canonical state directory")?;
        check(initialized["tokenFile"] == json!(canonical.join("api-token")), "initialize did not return its private credential path")?;
        let state_metadata = std::fs::symlink_metadata(&state)?;
        check(state_metadata.is_dir() && state_metadata.permissions().mode() & 0o077 == 0, "state directory is not private")?;
        for name in ["api-token", "state.redb"] {
            let metadata = std::fs::symlink_metadata(state.join(name))?;
            check(metadata.is_file() && metadata.nlink() == 1 && metadata.permissions().mode() & 0o077 == 0, "application state file is not a private regular file")?;
        }
        let credential = std::fs::read(state.join("api-token"))?;
        let secret = credential.strip_suffix(b"\n").unwrap_or(&credential);
        check(secret.len() == 64 && secret.iter().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte)), "initialize did not create a valid API credential")?;

        let mut serve = cli(directory.path());
        serve.arg("serve").arg("--state").arg(&state);
        servers.push(OwnedProcess::spawn(serve)?);
        let first_address = servers[0].listen_address().await?;
        let capabilities = request(directory.path(), &state, first_address, "application.capabilities", json!({})).await?;
        check(capabilities["codexSessions"] == false, "CLI started an unconfigured provider")?;
        let created = request(directory.path(), &state, first_address, "command.execute", json!({"command":{
            "type":"project.create","commandId":"project-from-cli","projectId":"cli-project","title":" CLI project ","workspaceRoot":workspace,
        }})).await?;
        check(created["replayed"] == false && created["receipt"]["result"]["ok"]["project"]["title"] == "CLI project", "CLI command did not create the durable project")?;
        let receipt = request(directory.path(), &state, first_address, "receipt.get", json!({"commandId":"project-from-cli"})).await?;
        check(receipt == created["receipt"], "CLI receipt did not match the committed command")?;
        let snapshot = request(directory.path(), &state, first_address, "state.snapshot", json!({})).await?;
        check(snapshot["projections"].as_array().is_some_and(|rows| rows.len() == 1 && rows[0]["key"] == "project/cli-project"), "created project is missing from the public snapshot")?;

        let mut competing = cli(directory.path());
        competing.arg("serve").arg("--state").arg(&state);
        let rejected = OwnedProcess::spawn(competing)?.output(None).await?;
        check(!rejected.status.success(), "a second server acquired the same application state")?;
        check(String::from_utf8_lossy(&rejected.stderr).contains("another native server owns this state directory"), "second server did not fail at the ownership lock")?;
        let after_rejection = request(directory.path(), &state, first_address, "receipt.get", json!({"commandId":"project-from-cli"})).await?;
        check(after_rejection == receipt, "the competing server disrupted the original owner")?;

        servers[0].terminate().await?;
        let mut restart = cli(directory.path());
        restart.arg("serve").arg("--state").arg(&state);
        servers.push(OwnedProcess::spawn(restart)?);
        let second_address = servers[1].listen_address().await?;
        let reopened_receipt = request(directory.path(), &state, second_address, "receipt.get", json!({"commandId":"project-from-cli"})).await?;
        check(reopened_receipt == receipt, "receipt changed after SIGTERM and restart")?;
        let reopened_snapshot = request(directory.path(), &state, second_address, "state.snapshot", json!({})).await?;
        check(reopened_snapshot == snapshot, "state changed after SIGTERM and restart")?;
        check(std::fs::read(state.join("api-token"))? == credential, "restart rotated the existing API credential")?;
        servers[1].terminate().await?;
        Ok(())
    }.await;
    let mut cleanup_error = None;
    for server in &mut servers {
        if let Err(error) = server.reap().await {
            cleanup_error.get_or_insert(error);
        }
    }
    result.unwrap_or_else(|error| panic!("public CLI lifecycle failed: {error}"));
    if let Some(error) = cleanup_error {
        panic!("owned CLI child cleanup failed: {error}");
    }
}
