use std::process::Stdio;
use std::time::Instant;
use tokio::process::Command;
use tokio::io::{AsyncBufReadExt, BufReader};

#[derive(Debug, Clone)]
pub struct ExecutionRequest {
    pub run_id: String,
    pub language: String,
    pub code: String,
}

#[derive(Debug, Clone)]
pub struct ExecutionResult {
    pub run_id: String,
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
    pub duration_ms: u64,
}

pub async fn execute(req: ExecutionRequest) -> ExecutionResult {
    let run_id = req.run_id.clone();
    let start = Instant::now();

    let dir = std::env::temp_dir().join(&run_id);
    tokio::fs::create_dir_all(&dir).await.unwrap();

    let (src_path, compile_cmd, run_cmd) = match req.language.as_str() {
        "rust" => {
            let src = dir.join("main.rs");
            let bin = dir.join("main");
            (
                src.clone(),
                Some(("rustc", vec![
                    src.to_str().unwrap().to_string(),
                    "-o".into(),
                    bin.to_str().unwrap().to_string(),
                ])),
                vec![bin.to_str().unwrap().to_string()],
            )
        }
        "python" => {
            let src = dir.join("main.py");
            (src.clone(), None, vec![
                "python3".into(),
                src.to_str().unwrap().to_string(),
            ])
        }
        _ => {
            return ExecutionResult {
                run_id,
                stdout: String::new(),
                stderr: format!("unsupported language: {}", req.language),
                exit_code: 1,
                duration_ms: 0,
            };
        }
    };

    tokio::fs::write(&src_path, &req.code).await.unwrap();

    // compile step (Rust only)
    if let Some((cmd, args)) = compile_cmd {
        let output = Command::new(cmd)
            .args(&args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .await
            .unwrap();

        if !output.status.success() {
            let _ = tokio::fs::remove_dir_all(&dir).await;
            return ExecutionResult {
                run_id,
                stdout: String::new(),
                stderr: String::from_utf8_lossy(&output.stderr).to_string(),
                exit_code: output.status.code().unwrap_or(1),
                duration_ms: start.elapsed().as_millis() as u64,
            };
        }
    }

    // run
    let mut child = Command::new(&run_cmd[0])
        .args(&run_cmd[1..])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    let stdout = child.stdout.take().unwrap();
    let stderr = child.stderr.take().unwrap();

    let mut stdout_lines = BufReader::new(stdout).lines();
    let mut stderr_lines = BufReader::new(stderr).lines();

    let stdout_handle = tokio::spawn(async move {
        let mut buf = String::new();
        while let Ok(Some(line)) = stdout_lines.next_line().await {
            buf.push_str(&line);
            buf.push('\n');
        }
        buf
    });

    let stderr_handle = tokio::spawn(async move {
        let mut buf = String::new();
        while let Ok(Some(line)) = stderr_lines.next_line().await {
            buf.push_str(&line);
            buf.push('\n');
        }
        buf
    });

    let (stdout_buf, stderr_buf) = tokio::join!(stdout_handle, stderr_handle);
    let stdout_buf = stdout_buf.unwrap_or_default();
    let stderr_buf = stderr_buf.unwrap_or_default();

    let status = child.wait().await.unwrap();
    let _ = tokio::fs::remove_dir_all(&dir).await;

    ExecutionResult {
        run_id,
        stdout: stdout_buf,
        stderr: stderr_buf,
        exit_code: status.code().unwrap_or(1),
        duration_ms: start.elapsed().as_millis() as u64,
    }
}
