use std::{net::SocketAddr, path::PathBuf, time::Duration};

use clap::{Args, Parser, Subcommand};
use t3_code_ls_transport::{
    Admission, EndpointAddr, EndpointId, Forwarder, Host, HostConfig, Limits, Network, RelayUrl,
    load_or_create_identity,
};

#[derive(Parser)]
#[command(version, about = "Authenticated Iroh byte transport for T3 Code LS")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Args)]
struct Routing {
    /// Bind Iroh UDP only on localhost, for isolated local validation.
    #[arg(long, conflicts_with_all = ["relay", "public_discovery"])]
    loopback: bool,
    /// Use explicitly configured relays without public address publication.
    #[arg(long, conflicts_with = "public_discovery")]
    relay: Vec<RelayUrl>,
    /// Opt in to Number 0 public relays and public endpoint discovery.
    #[arg(long)]
    public_discovery: bool,
}

impl Routing {
    fn network(&self) -> Network {
        if self.loopback {
            Network::Loopback
        } else if self.public_discovery {
            Network::Public
        } else if self.relay.is_empty() {
            Network::Direct
        } else {
            Network::Relays(self.relay.clone())
        }
    }
    fn uses_relay(&self) -> bool {
        self.public_discovery || !self.relay.is_empty()
    }
}

#[derive(Subcommand)]
enum Command {
    /// Create or read a persistent identity; prints only its public endpoint ID.
    Identity {
        #[arg(long)]
        identity: PathBuf,
    },
    /// Expose one fixed loopback service to explicitly granted endpoint IDs.
    Serve {
        #[arg(long)]
        identity: PathBuf,
        /// Explicit loopback IP:port of an authenticated T3 server.
        #[arg(long)]
        target: SocketAddr,
        /// Endpoint ID to grant. Repeat for each client; empty means deny everyone.
        #[arg(long)]
        allow: Vec<EndpointId>,
        #[command(flatten)]
        routing: Routing,
    },
    /// Bind a local loopback port and forward bytes to one pinned remote endpoint.
    Forward {
        #[arg(long)]
        identity: PathBuf,
        #[arg(long)]
        remote: EndpointId,
        /// Known direct UDP address of the remote Iroh endpoint. Repeat if needed.
        #[arg(long)]
        address: Vec<SocketAddr>,
        /// Known remote Iroh relay URL. Repeat if needed.
        #[arg(long)]
        remote_relay: Vec<RelayUrl>,
        #[arg(long, default_value = "127.0.0.1:0")]
        listen: SocketAddr,
        #[command(flatten)]
        routing: Routing,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    match Cli::parse().command {
        Command::Identity { identity } => {
            let key = load_or_create_identity(&identity)?;
            println!("{}", key.public());
        }
        Command::Serve {
            identity,
            target,
            allow,
            routing,
        } => {
            let key = load_or_create_identity(&identity)?;
            let admission = Admission::new();
            for peer in allow {
                admission.grant(peer);
            }
            let host =
                Host::bind(key, routing.network(), HostConfig::new(target, admission)).await?;
            if routing.uses_relay() {
                host.online(Duration::from_secs(30)).await?;
            }
            println!(
                "{}",
                serde_json::json!({"endpoint": host.addr(), "target": target.to_string()})
            );
            tokio::signal::ctrl_c().await?;
            host.shutdown().await?;
        }
        Command::Forward {
            identity,
            remote,
            address,
            remote_relay,
            listen,
            routing,
        } => {
            let key = load_or_create_identity(&identity)?;
            let mut remote = EndpointAddr::new(remote);
            for address in address {
                remote = remote.with_ip_addr(address);
            }
            for relay in remote_relay {
                remote = remote.with_relay_url(relay);
            }
            let mut forwarder =
                Forwarder::bind(key, routing.network(), remote, listen, Limits::default()).await?;
            println!(
                "{}",
                serde_json::json!({"endpoint_id": forwarder.id().to_string(), "listen": forwarder.local_addr().to_string()})
            );
            tokio::select! {
                result = forwarder.wait() => result?,
                result = tokio::signal::ctrl_c() => result?,
            }
            forwarder.shutdown().await?;
        }
    }
    Ok(())
}
