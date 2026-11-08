//! Operational peer allowlist; a file is configuration, never client authorization.
use std::fs::{self, OpenOptions};
use std::io::{self, Read};
use std::path::PathBuf;

use crate::{Admission, EndpointId};

const MAX_BYTES: usize = 64 * 1024;
const MAX_PEERS: usize = 1024;

#[derive(Clone)]
pub struct AdmissionFile {
    path: PathBuf,
    admission: Admission,
}

impl AdmissionFile {
    /// Fail startup if the initial allowlist cannot be safely read.
    pub fn load(path: PathBuf, admission: Admission) -> io::Result<Self> {
        let config = Self { path, admission };
        config.reload()?;
        Ok(config)
    }

    /// Validate the entire file before changing a single active grant.
    pub fn reload(&self) -> io::Result<bool> {
        if fs::symlink_metadata(&self.path)?.file_type().is_symlink() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "peer file cannot be a symlink",
            ));
        }
        let mut options = OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
        }
        let file = options.open(&self.path)?;
        let meta = file.metadata()?;
        if !meta.is_file() || meta.len() > MAX_BYTES as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "peer file must be a regular file no larger than 64 KiB",
            ));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if meta.mode() & 0o022 != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "peer file must not be group/world writable",
                ));
            }
        }
        let mut bytes = Vec::new();
        file.take(MAX_BYTES as u64 + 1).read_to_end(&mut bytes)?;
        if bytes.len() > MAX_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "peer file exceeded 64 KiB",
            ));
        }
        let source = String::from_utf8(bytes)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "peer file must be UTF-8"))?;
        let mut peers = Vec::new();
        for line in source.lines() {
            let candidate = line.split('#').next().unwrap_or("").trim();
            if candidate.is_empty() {
                continue;
            }
            if peers.len() >= MAX_PEERS {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "peer file exceeds 1024 endpoint IDs",
                ));
            }
            let peer = candidate.parse::<EndpointId>().map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid endpoint ID in peer file",
                )
            })?;
            if peers.contains(&peer) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "duplicate endpoint in peer file",
                ));
            }
            peers.push(peer);
        }
        self.admission
            .reconcile(&peers)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
    }
}
