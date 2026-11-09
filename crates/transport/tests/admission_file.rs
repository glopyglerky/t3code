use std::fs;
use std::io::ErrorKind;

use t3_code_ls_transport::{Admission, AdmissionFile, SecretKey};

fn key() -> t3_code_ls_transport::EndpointId {
    SecretKey::generate().public()
}

#[test]
fn reload_is_atomic_and_preserves_survivor_generations() {
    let temporary = tempfile::tempdir().unwrap();
    let path = temporary.path().join("allowlist");
    let alice = key();
    let bob = key();
    let charlie = key();
    fs::write(&path, format!("# initial\n{alice}\n{bob}\n")).unwrap();
    let admission = Admission::new();
    let source = AdmissionFile::load(path.clone(), admission.clone()).unwrap();
    let generation = admission.generation(alice).unwrap();
    let removed_generation = admission.generation(bob).unwrap();
    assert!(!source.reload().unwrap());
    fs::write(
        &path,
        format!("# replacement\n{alice}\n{charlie} # second device\n"),
    )
    .unwrap();
    assert!(source.reload().unwrap());
    assert_eq!(admission.generation(alice), Some(generation));
    assert!(!admission.permits(bob));
    assert!(admission.permits(charlie));
    fs::write(&path, format!("{alice}\n{bob}\n")).unwrap();
    assert!(source.reload().unwrap());
    assert_ne!(admission.generation(bob), Some(removed_generation));
    assert!(!admission.permits(charlie));
}

#[test]
fn malformed_or_missing_file_cannot_partially_change_grants() {
    let temporary = tempfile::tempdir().unwrap();
    let path = temporary.path().join("allowlist");
    let alice = key();
    let bob = key();
    fs::write(&path, format!("{alice}\n")).unwrap();
    let admission = Admission::new();
    let source = AdmissionFile::load(path.clone(), admission.clone()).unwrap();
    let original = admission.generation(alice);
    for bad in [
        format!("{bob}\nnot-an-endpoint\n"),
        format!("{bob}\n{bob}\n"),
        format!("{bob}\n{}", "x".repeat(65536)),
        String::from("x\n").repeat(1025),
    ] {
        fs::write(&path, bad).unwrap();
        assert!(source.reload().is_err());
        assert_eq!(admission.generation(alice), original);
        assert!(!admission.permits(bob));
    }
    fs::remove_file(&path).unwrap();
    assert_eq!(source.reload().unwrap_err().kind(), ErrorKind::NotFound);
    assert_eq!(admission.generation(alice), original);
}

#[cfg(unix)]
#[test]
fn symlinks_and_group_writable_allowlists_are_rejected() {
    use std::os::unix::fs::{PermissionsExt, symlink};

    let temporary = tempfile::tempdir().unwrap();
    let path = temporary.path().join("allowlist");
    let link = temporary.path().join("allowlink");
    let alice = key();
    fs::write(&path, format!("{alice}\n")).unwrap();
    let admission = Admission::new();
    let source = AdmissionFile::load(path.clone(), admission.clone()).unwrap();
    let original = admission.generation(alice);
    symlink(&path, &link).unwrap();
    assert_eq!(
        AdmissionFile::load(link, admission.clone())
            .err()
            .unwrap()
            .kind(),
        ErrorKind::PermissionDenied
    );
    fs::set_permissions(&path, fs::Permissions::from_mode(0o666)).unwrap();
    assert_eq!(
        source.reload().unwrap_err().kind(),
        ErrorKind::PermissionDenied
    );
    assert_eq!(admission.generation(alice), original);
}
