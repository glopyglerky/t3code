use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::Path;

#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};

#[derive(Clone)]
pub struct Token([u8; 64]);

impl Token {
    pub fn parse(value: &[u8]) -> io::Result<Self> {
        let value = value.strip_suffix(b"\n").unwrap_or(value);
        let bytes: [u8; 64] = value.try_into().map_err(|_| invalid())?;
        if !bytes
            .iter()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
        {
            return Err(invalid());
        }
        Ok(Self(bytes))
    }

    pub fn matches_header(&self, value: &[u8]) -> bool {
        let Some(value) = value.strip_prefix(b"Bearer ") else {
            return false;
        };
        if value.len() != self.0.len() {
            return false;
        }
        self.0
            .iter()
            .zip(value)
            .fold(0u8, |difference, (left, right)| difference | (left ^ right))
            == 0
    }

    pub fn header(&self) -> String {
        format!(
            "Bearer {}",
            std::str::from_utf8(&self.0).expect("token is ASCII")
        )
    }

    pub fn load(path: &Path) -> io::Result<Self> {
        let before = fs::symlink_metadata(path)?;
        if !before.is_file() {
            return Err(invalid());
        }
        #[cfg(unix)]
        if before.permissions().mode() & 0o077 != 0 || before.nlink() != 1 {
            return Err(invalid());
        }
        let mut file = File::open(path)?;
        let opened = file.metadata()?;
        if !opened.is_file() {
            return Err(invalid());
        }
        #[cfg(unix)]
        if opened.dev() != before.dev()
            || opened.ino() != before.ino()
            || opened.permissions().mode() & 0o077 != 0
            || opened.nlink() != 1
        {
            return Err(invalid());
        }
        let mut value = Vec::new();
        Read::by_ref(&mut file).take(66).read_to_end(&mut value)?;
        Self::parse(&value)
    }

    /// Create once; opening an existing token validates it without rotating a live credential.
    pub fn create(path: &Path) -> io::Result<Self> {
        let mut random = [0u8; 32];
        getrandom::fill(&mut random).map_err(io::Error::other)?;
        let value: String = random.iter().map(|byte| format!("{byte:02x}")).collect();
        let token = Self::parse(value.as_bytes())?;
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options.mode(0o600);
        match options.open(path) {
            Ok(mut file) => {
                file.write_all(value.as_bytes())?;
                file.write_all(b"\n")?;
                file.sync_all()?;
                #[cfg(unix)]
                File::open(
                    path.parent()
                        .filter(|parent| !parent.as_os_str().is_empty())
                        .unwrap_or(Path::new(".")),
                )?
                .sync_all()?;
                Ok(token)
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => Self::load(path),
            Err(error) => Err(error),
        }
    }
}

fn invalid() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        "token must be a private regular file containing 64 lowercase hexadecimal characters",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credentials_survive_restart_and_reject_ambiguous_headers() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("token");
        let token = Token::create(&path).unwrap();
        let reloaded = Token::create(&path).unwrap();
        assert!(reloaded.matches_header(token.header().as_bytes()));
        for header in ["", "bearer token", "Bearer token", &(token.header() + " ")] {
            assert!(!token.matches_header(header.as_bytes()));
        }
        assert!(Token::parse(&vec![b'a'; 65]).is_err());
        fs::write(&path, "bad credential").unwrap();
        assert!(Token::create(&path).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn rejects_links_and_shared_permissions_without_changing_files() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("token");
        Token::create(&path).unwrap();
        let link = directory.path().join("link");
        std::os::unix::fs::symlink(&path, &link).unwrap();
        assert!(Token::load(&link).is_err());
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(Token::load(&path).is_err());
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        fs::hard_link(&path, directory.path().join("other")).unwrap();
        assert!(Token::load(&path).is_err());
    }
}
