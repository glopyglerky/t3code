use std::fs::{self, File, OpenOptions};
use std::io::{self, Read};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use clap::{Parser, Subcommand};
use serde_json::json;
use t3_code_ls_app::{MAX_RPC_BYTES, auth::Token, decode_json, http, service::Application};
use t3_code_ls_storage::Store;
use tokio::net::TcpListener;

#[cfg(unix)]
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};

#[derive(Parser)]
#[command(
    version,
    about = "Native T3 Code LS application service and command-line client"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Create an isolated native application database and its private API credential.
    Initialize {
        #[arg(long)]
        state: PathBuf,
    },
    /// Start the authenticated native API. Remote clients can use Iroh forwarding.
    Serve {
        #[arg(long)]
        state: PathBuf,
        #[arg(long, default_value = "127.0.0.1:0")]
        listen: SocketAddr,
        /// Explicit trusted Codex executable. Omit to disable provider execution.
        #[arg(long)]
        provider: Option<PathBuf>,
        /// Provider launch argument. Defaults to app-server when none are supplied.
        #[arg(long, allow_hyphen_values = true)]
        provider_arg: Vec<String>,
    },
    /// Read one JSON-RPC request from stdin and print the complete server response.
    Request {
        #[arg(long)]
        address: SocketAddr,
        #[arg(long)]
        token_file: PathBuf,
    },
}

fn state_directory(path: &Path, create: bool) -> io::Result<PathBuf> {
    if !path.is_absolute() {
        return Err(io::Error::other("state directory must be absolute"));
    }
    if create && !path.exists() {
        let mut builder = fs::DirBuilder::new();
        #[cfg(unix)]
        builder.mode(0o700);
        builder.create(path)?;
    }
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_dir() {
        return Err(io::Error::other("state directory must be a real directory"));
    }
    #[cfg(unix)]
    if metadata.permissions().mode() & 0o077 != 0 {
        return Err(io::Error::other(
            "state directory must be accessible only to its owner",
        ));
    }
    path.canonicalize()
}

fn server_lock(state: &Path) -> io::Result<File> {
    let path = state.join("server.lock");
    if fs::symlink_metadata(&path).is_ok_and(|metadata| !metadata.is_file()) {
        return Err(io::Error::other("server lock must be a regular file"));
    }
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    options.mode(0o600);
    let lock = options.open(path)?;
    lock.try_lock()
        .map_err(|_| io::Error::other("another native server owns this state directory"))?;
    Ok(lock)
}

fn database_path(state: &Path, create: bool) -> io::Result<PathBuf> {
    let path = state.join("state.redb");
    match fs::symlink_metadata(&path) {
        Ok(metadata) => {
            if !metadata.is_file() {
                return Err(io::Error::other("database must be a regular file"));
            }
            #[cfg(unix)]
            if metadata.nlink() != 1 || metadata.permissions().mode() & 0o077 != 0 {
                return Err(io::Error::other(
                    "database must be private and have no hard links",
                ));
            }
        }
        Err(error) if create && error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    Ok(path)
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut terminate) => tokio::select! {
                _ = tokio::signal::ctrl_c() => {},
                _ = terminate.recv() => {},
            },
            Err(error) => eprintln!("could not register service shutdown signal: {error}"),
        }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    match Cli::parse().command {
        Command::Initialize { state } => {
            let state = state_directory(&state, true)?;
            let _lock = server_lock(&state)?;
            let _token = Token::create(&state.join("api-token"))?;
            Store::initialize_local(database_path(&state, true)?)
                .await?
                .shutdown()
                .await?;
            println!(
                "{}",
                json!({"state":state, "tokenFile":state.join("api-token")})
            );
        }
        Command::Serve {
            state,
            listen,
            provider,
            provider_arg,
        } => {
            if !listen.ip().is_loopback() {
                return Err("server listen address must be loopback".into());
            }
            let state = state_directory(&state, false)?;
            let _lock = server_lock(&state)?;
            let token = Token::load(&state.join("api-token"))?;
            let store = Arc::new(Store::open_local(database_path(&state, false)?).await?);
            let listener = TcpListener::bind(listen).await?;
            if provider.is_none() && !provider_arg.is_empty() {
                return Err("provider arguments require --provider".into());
            }
            let mut app = Application::new(store.clone());
            #[cfg(unix)]
            if let Some(program) = provider {
                use t3_code_ls_app::runtime::{ProviderConfig, ProviderHub};
                if !program.is_absolute() || !program.is_file() {
                    return Err("provider must be an absolute executable file".into());
                }
                let args = if provider_arg.is_empty() {
                    vec!["app-server".into()]
                } else {
                    provider_arg
                };
                let hub = ProviderHub::new(store.clone(), ProviderConfig { program, args });
                hub.recover().await?;
                app = app.with_providers(hub);
            }
            #[cfg(not(unix))]
            if provider.is_some() || !provider_arg.is_empty() {
                return Err(
                    "provider process execution is not implemented on this platform".into(),
                );
            }
            let app = Arc::new(app);
            println!(
                "{}",
                json!({"listen":listener.local_addr()?, "protocolVersion":1})
            );
            let served = http::serve(listener, app.clone(), token, shutdown_signal()).await;
            let providers_stopped = app.shutdown().await;
            drop(app);
            Arc::try_unwrap(store)
                .map_err(|_| io::Error::other("application still owns database operations"))?
                .shutdown()
                .await?;
            providers_stopped.map_err(|error| io::Error::other(error.message))?;
            served?;
        }
        Command::Request {
            address,
            token_file,
        } => {
            let token = Token::load(&token_file)?;
            let mut bytes = Vec::new();
            io::stdin()
                .lock()
                .take(MAX_RPC_BYTES as u64 + 1)
                .read_to_end(&mut bytes)?;
            let request = decode_json(&bytes).map_err(io::Error::other)?;
            let response = http::request(address, &token, request).await?;
            println!("{}", serde_json::to_string(&response)?);
            if response.get("error").is_some() {
                return Err("native request was rejected".into());
            }
        }
    }
    Ok(())
}

#[tokio::main(worker_threads = 2)]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("t3-code-ls-app: {error}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_second_server_cannot_reconcile_an_active_owners_sessions() {
        let directory = tempfile::tempdir().unwrap();
        let first = server_lock(directory.path()).unwrap();
        assert!(server_lock(directory.path()).is_err());
        drop(first);
        assert!(server_lock(directory.path()).is_ok());
    }
}
