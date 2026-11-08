use iroh::SecretKey;
use std::{
    fs::{self, OpenOptions},
    io::{self, Read, Write},
    path::Path,
};

/// Loads a raw 32-byte key. A missing file is created without replacing existing state.
pub fn load_or_create_identity(path: &Path) -> io::Result<SecretKey> {
    let key = SecretKey::generate();
    let mut create = OpenOptions::new();
    create.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        create.mode(0o600);
    }
    match create.open(path) {
        Ok(mut file) => {
            file.write_all(&key.to_bytes())?;
            file.sync_all()?;
            return Ok(key);
        }
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error),
    }
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    if fs::symlink_metadata(path)?.file_type().is_symlink() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "identity must not be a symlink",
        ));
    }
    let mut file = options.open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() != 32 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "identity must contain exactly 32 bytes",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.mode() & 0o077 != 0 || metadata.nlink() != 1 {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "identity must have owner-only permissions and one link",
            ));
        }
    }
    let mut bytes = [0; 32];
    file.read_exact(&mut bytes)?;
    Ok(SecretKey::from_bytes(&bytes))
}
