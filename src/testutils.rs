#[cfg(any(test, feature = "pg_test"))]
pub mod minio_test {
    use std::net::{Ipv4Addr, SocketAddrV4, TcpListener};
    use std::os::unix::fs::PermissionsExt;
    use std::sync::OnceLock;
    use std::{
        fs, io,
        path::{Path, PathBuf},
        process::{Child, Command, Stdio},
        time::Duration,
    };
    use tempfile::TempDir;

    pub struct MinioServer {
        child: Child,
        _data: TempDir,
    }

    impl Drop for MinioServer {
        fn drop(&mut self) {
            // Best-effort stop; ignore errors (tests are exiting anyway)
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }

    impl MinioServer {
        /// Start MinIO listening on a random port
        pub fn start() -> anyhow::Result<Self> {
            static MINIO_BIN: OnceLock<PathBuf> = OnceLock::new();
            let bin = MINIO_BIN.get_or_init(|| ensure_minio_binary().unwrap());
            let data = tempfile::tempdir()?;

            let api_port = free_port()?;
            let console_port = free_port()?;
            let endpoint = format!("http://127.0.0.1:{api_port}");

            // Credentials for tests
            let access_key = "minio";
            let secret_key = "minio12345";

            // Spawn minio
            let mut cmd = Command::new(bin);
            cmd.arg("server")
                .arg("--address")
                .arg(format!("127.0.0.1:{api_port}"))
                .arg("--console-address")
                .arg(format!("127.0.0.1:{console_port}"))
                .arg(data.path())
                .env("MINIO_ROOT_USER", access_key)
                .env("MINIO_ROOT_PASSWORD", secret_key)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null());
            let child = cmd
                .spawn()
                .map_err(|e| anyhow::anyhow!("spawn minio: {e}"))?;

            // Wait for health
            wait_healthy(api_port, Duration::from_secs(30))?;

            // Expose env so the Postgres backend sees them inside this process
            std::env::set_var("AWS_ACCESS_KEY_ID", access_key);
            std::env::set_var("AWS_SECRET_ACCESS_KEY", secret_key);
            std::env::set_var("S3_ENDPOINT_URL", &endpoint);
            // std::env::set_var("AWS_EC2_METADATA_DISABLED", "true");s

            Ok(Self { child, _data: data })
        }
    }

    /// Grap a free port from the OS
    fn free_port() -> anyhow::Result<u16> {
        // Using port number 0 asks the OS to provide a free port
        let sock = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))?;
        let port = sock.local_addr()?.port();
        drop(sock);
        Ok(port)
    }

    /// Poll MinIO health endpoint until MinIO ready
    fn wait_healthy(port: u16, timeout: Duration) -> anyhow::Result<()> {
        let start = std::time::Instant::now();
        let url = format!("http://127.0.0.1:{port}/minio/health/live");
        while start.elapsed() < timeout {
            if let Ok(resp) = reqwest::blocking::get(&url) {
                if resp.status().is_success() {
                    return Ok(());
                }
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        Err(anyhow::anyhow!("MinIO did not become healthy on {url}"))
    }

    /// Download MinIO binary if not existing
    pub fn ensure_minio_binary() -> anyhow::Result<PathBuf> {
        let (os, arch) = (std::env::consts::OS, std::env::consts::ARCH);
        let (subdir, url) = match (os, arch) {
            ("linux", "x86_64") => (
                "linux-amd64",
                "https://dl.min.io/server/minio/release/linux-amd64/minio",
            ),
            ("linux", "aarch64") => (
                "linux-arm64",
                "https://dl.min.io/server/minio/release/linux-arm64/minio",
            ),
            ("macos", "x86_64") => (
                "darwin-amd64",
                "https://dl.min.io/server/minio/release/darwin-amd64/minio",
            ),
            ("macos", "aarch64") => (
                "darwin-arm64",
                "https://dl.min.io/server/minio/release/darwin-arm64/minio",
            ),
            other => return Err(anyhow::anyhow!("unsupported platform: {other:?}")),
        };

        // Put the binary under target/… and make the path absolute
        let target_dir = std::env::current_dir()?
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("minio")
            .join(subdir);
        fs::create_dir_all(&target_dir)?;
        let bin = target_dir.join("minio");

        if !bin.exists() {
            download_file(url, &bin)?;
        }

        // Always ensure executable (even if it already existed)
        #[cfg(unix)]
        {
            let mut perms = fs::metadata(&bin)?.permissions();
            let mode = perms.mode();
            if mode & 0o111 == 0 {
                perms.set_mode(mode | 0o755);
                fs::set_permissions(&bin, perms)?;
            }
        }

        Ok(bin)
    }

    /// Download file from url to file on dest
    fn download_file(url: &str, dest: &Path) -> anyhow::Result<()> {
        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(60))
            .build()?;
        let resp = client.get(url).send()?.error_for_status()?;
        let bytes = resp.bytes()?;
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut f = fs::File::create(dest)?;
        io::copy(&mut io::Cursor::new(bytes), &mut f)?;

        Ok(())
    }
}
