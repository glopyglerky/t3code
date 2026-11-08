use t3_code_ls_transport::load_or_create_identity;

#[test]
fn identity_is_persistent_and_malformed_keys_are_not_replaced() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("endpoint.key");
    let first = load_or_create_identity(&path).unwrap();
    let second = load_or_create_identity(&path).unwrap();
    assert_eq!(first.public(), second.public());
    std::fs::write(&path, b"not a key").unwrap();
    assert!(load_or_create_identity(&path).is_err());
    assert_eq!(std::fs::read(path).unwrap(), b"not a key");
}

#[cfg(unix)]
#[test]
fn identities_refuse_shared_permissions_symlinks_and_hard_links() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("endpoint.key");
    load_or_create_identity(&path).unwrap();
    assert_eq!(
        std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let linked = dir.path().join("linked.key");
    symlink(&path, &linked).unwrap();
    assert!(load_or_create_identity(&linked).is_err());
    std::fs::remove_file(&linked).unwrap();
    std::fs::hard_link(&path, &linked).unwrap();
    assert!(load_or_create_identity(&path).is_err());
    std::fs::remove_file(&linked).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
    assert!(load_or_create_identity(&path).is_err());
}

#[cfg(unix)]
#[test]
fn identity_rejects_fifo_without_waiting_for_a_writer() {
    use std::{os::unix::fs::OpenOptionsExt, sync::mpsc, time::Duration};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("endpoint.key");
    assert!(
        std::process::Command::new("mkfifo")
            .arg(&path)
            .status()
            .unwrap()
            .success()
    );
    let worker_path = path.clone();
    let (sender, receiver) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        sender.send(load_or_create_identity(&worker_path)).unwrap();
    });
    let result = receiver.recv_timeout(Duration::from_secs(2));
    if result.is_err() {
        // Release the old blocking implementation so a failing test leaves no blocked thread.
        let _writer = std::fs::OpenOptions::new()
            .write(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(&path);
        worker.join().unwrap();
        panic!("identity validation waited for a FIFO writer");
    }
    worker.join().unwrap();
    assert_eq!(
        result.unwrap().unwrap_err().kind(),
        std::io::ErrorKind::InvalidData
    );
}
