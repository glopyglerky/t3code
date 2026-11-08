use std::io::{BufRead, BufReader};
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use surrealdb::engine::any::connect;
use surrealdb::opt::auth::Root;
use tempfile::TempDir;

pub struct Daemon {
    pub endpoint: String,
    pub token: String,
    child: Option<Child>,
    data: TempDir,
    executable: PathBuf,
    address: String,
    password: String,
}

impl Daemon {
    pub async fn start() -> Self {
        let executable = std::env::var_os("T3_CODE_LS_SURREAL_EXECUTABLE")
            .map(PathBuf::from)
            .expect("T3_CODE_LS_SURREAL_EXECUTABLE must name the pinned SurrealDB 4.0.0 binary");
        assert!(executable.is_absolute());
        let temp_root = std::env::var_os("T3_CODE_LS_TEST_TMPDIR")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let data = tempfile::Builder::new()
            .prefix("t3-surreal-test-")
            .tempdir_in(temp_root)
            .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        drop(listener);
        let mut random = [0_u8; 32];
        getrandom::fill(&mut random).unwrap();
        let password = random.iter().map(|byte| format!("{byte:02x}")).collect();
        let mut daemon = Self {
            endpoint: format!("ws://{address}"),
            token: String::new(),
            child: None,
            data,
            executable,
            address,
            password,
        };
        daemon.launch().await;
        daemon
    }

    async fn launch(&mut self) {
        let data_endpoint = format!("surrealkv://{}?sync=every", self.data.path().display());
        let mut child = Command::new(&self.executable)
            .args([
                "start",
                "--bind",
                &self.address,
                "--deny-net",
                "--deny-scripting",
                "--log",
                "info",
                "--log-format",
                "json",
                &data_endpoint,
            ])
            .env_clear()
            .env("SURREAL_USER", "storage_test")
            .env("SURREAL_PASS", &self.password)
            .env("SURREAL_SURREALKV_BLOCK_CACHE_CAPACITY", "33554432")
            .env("SURREAL_SURREALKV_MAX_MEMTABLE_SIZE", "16777216")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("start pinned SurrealDB daemon");
        let stdout = child.stdout.take().unwrap();
        let mut stderr = child.stderr.take().unwrap();
        self.child = Some(child);
        std::thread::spawn(move || {
            let _ = std::io::copy(&mut stderr, &mut std::io::sink());
        });
        let (ready, waiting) = tokio::sync::oneshot::channel();
        let expected = format!("Started web server on {}", self.address);
        std::thread::spawn(move || {
            let mut reader = BufReader::new(stdout);
            let mut observed = 0;
            loop {
                let mut line = String::new();
                match reader.read_line(&mut line) {
                    Ok(0) | Err(_) => {
                        let _ = ready.send(false);
                        return;
                    }
                    Ok(count) => {
                        observed += count;
                    }
                }
                if observed > 65_536 {
                    let _ = ready.send(false);
                    return;
                }
                if serde_json::from_str::<serde_json::Value>(&line)
                    .ok()
                    .and_then(|value| {
                        value
                            .get("fields")?
                            .get("message")?
                            .as_str()
                            .map(str::to_owned)
                    })
                    .is_some_and(|message| message == expected)
                {
                    let _ = ready.send(true);
                    let _ = std::io::copy(&mut reader, &mut std::io::sink());
                    return;
                }
            }
        });
        assert!(
            tokio::time::timeout(Duration::from_secs(15), waiting)
                .await
                .expect("database listener readiness deadline")
                .expect("database readiness channel"),
            "database exited before listener readiness"
        );
        let db = connect(self.endpoint.as_str()).await.unwrap();
        let token = db
            .signin(Root {
                username: "storage_test".into(),
                password: self.password.clone(),
            })
            .await
            .unwrap();
        self.token = token.access.into_insecure_token();
        db.shutdown().await.unwrap();
    }

    pub async fn crash_and_restart(&mut self) {
        if let Some(mut child) = self.child.take() {
            child.kill().unwrap();
            child.wait().unwrap();
        }
        self.launch().await;
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
