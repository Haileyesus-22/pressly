use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::sync::Semaphore;
use tokio::time::Instant;

const MAX_CONCURRENT: usize = 4;
const EXEC_TIMEOUT: Duration = Duration::from_secs(30);
const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
pub struct ExecutionRequest {
    pub run_id: String,
    pub language: String,
    pub code: String,
    pub previous_output: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ExecutionResult {
    pub run_id: String,
    pub stdout: String,
    pub stderr: String,
    pub exit_code: Option<i32>,
    pub duration_ms: u64,
    pub timed_out: bool,
    pub diff: Vec<crate::ws::DiffHunk>,
}

static EXEC_SEM: std::sync::OnceLock<Arc<Semaphore>> = std::sync::OnceLock::new();

fn exec_semaphore() -> Arc<Semaphore> {
    EXEC_SEM
        .get_or_init(|| Arc::new(Semaphore::new(MAX_CONCURRENT)))
        .clone()
}

fn sanitize_id(raw: &str) -> String {
    raw.chars()
        .filter(|c| c.is_alphanumeric() || *c == '-' || *c == '_')
        .take(64)
        .collect()
}

pub async fn execute(req: ExecutionRequest) -> ExecutionResult {
    let run_id = req.run_id.clone();
    let safe_id = sanitize_id(&run_id);
    let start = Instant::now();

    let sem = exec_semaphore();
    let _permit = match sem.acquire().await {
        Ok(p) => p,
        Err(_) => {
            return result_err(&run_id, "execution semaphore closed", 0, false);
        }
    };

    let dir = std::env::temp_dir().join("pressly").join(&safe_id);

    if tokio::fs::create_dir_all(&dir).await.is_err() {
        return result_err(&run_id, "failed to create temp directory", 0, false);
    }

    let (src_path, compile_cmd, run_cmd) = match req.language.as_str() {
        "rust" => {
            let src = dir.join("main.rs");
            let bin = dir.join("main");
            let src_str = src.to_string_lossy().to_string();
            let bin_str = bin.to_string_lossy().to_string();
            (src, Some(("rustc", vec![src_str, "-o".into(), bin_str.clone()])), vec![bin_str])
        }
        "python" => {
            let src = dir.join("main.py");
            let src_str = src.to_string_lossy().to_string();
            (src, None, vec!["python3".into(), src_str])
        }
        _ => {
            return ExecutionResult {
                run_id,
                stdout: String::new(),
                stderr: format!("unsupported language: {}", req.language),
                exit_code: None,
                duration_ms: 0,
                timed_out: false,
                diff: vec![],
            };
        }
    };

    if tokio::fs::write(&src_path, &req.code).await.is_err() {
        let _ = tokio::fs::remove_dir_all(&dir).await;
        return result_err(&run_id, "failed to write source file", elapsed(&start), false);
    }

    if let Some((cmd, args)) = compile_cmd {
        let output = tokio::time::timeout(EXEC_TIMEOUT, async {
            Command::new(cmd)
                .args(&args)
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .output()
                .await
        })
        .await;

        let output = match output {
            Ok(Ok(o)) => o,
            Ok(Err(_)) => {
                let _ = tokio::fs::remove_dir_all(&dir).await;
                return result_err(&run_id, "failed to spawn compiler", elapsed(&start), false);
            }
            Err(_) => {
                let _ = tokio::fs::remove_dir_all(&dir).await;
                return result_err(&run_id, "compilation timed out", elapsed(&start), true);
            }
        };

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).to_string();
            let _ = tokio::fs::remove_dir_all(&dir).await;
            return ExecutionResult {
                run_id,
                stdout: String::new(),
                stderr,
                exit_code: output.status.code(),
                duration_ms: elapsed(&start),
                timed_out: false,
                diff: vec![],
            };
        }
    }

    let mut child = match Command::new(&run_cmd[0])
        .args(&run_cmd[1..])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(c) => c,
        Err(_) => {
            let _ = tokio::fs::remove_dir_all(&dir).await;
            return result_err(&run_id, "failed to spawn process", elapsed(&start), false);
        }
    };

    let stdout_pipe = child.stdout.take();
    let stderr_pipe = child.stderr.take();

    let stdout_task = tokio::spawn(async move {
        let mut buf = Vec::new();
        if let Some(mut r) = stdout_pipe {
            let _ = tokio::time::timeout(DRAIN_TIMEOUT, r.read_to_end(&mut buf)).await;
        }
        String::from_utf8_lossy(&buf).to_string()
    });

    let stderr_task = tokio::spawn(async move {
        let mut buf = Vec::new();
        if let Some(mut r) = stderr_pipe {
            let _ = tokio::time::timeout(DRAIN_TIMEOUT, r.read_to_end(&mut buf)).await;
        }
        String::from_utf8_lossy(&buf).to_string()
    });

    let wait_result = tokio::time::timeout(EXEC_TIMEOUT, child.wait()).await;

    let timed_out = wait_result.is_err();
    if timed_out {
        let _ = child.kill().await;
    }

    let status = match wait_result {
        Ok(Ok(s)) => Some(s),
        _ => None,
    };

    let (stdout_buf, stderr_buf) = tokio::join!(stdout_task, stderr_task);
    let _ = tokio::fs::remove_dir_all(&dir).await;

    let stdout = stdout_buf.unwrap_or_default();
    let stderr = stderr_buf.unwrap_or_default();

    let exit_code = status.and_then(|s| s.code());

    let diff = if let Some(prev) = req.previous_output {
        crate::diff::diff_output(&prev, &stdout)
    } else {
        vec![]
    };

    ExecutionResult {
        run_id,
        stdout,
        stderr: if timed_out && stderr.is_empty() {
            "execution timed out".into()
        } else {
            stderr
        },
        exit_code,
        duration_ms: elapsed(&start),
        timed_out,
        diff,
    }
}

fn result_err(run_id: &str, msg: &str, duration_ms: u64, timed_out: bool) -> ExecutionResult {
    ExecutionResult {
        run_id: run_id.to_string(),
        stdout: String::new(),
        stderr: msg.to_string(),
        exit_code: None,
        duration_ms,
        timed_out,
        diff: vec![],
    }
}

fn elapsed(start: &Instant) -> u64 {
    start.elapsed().as_millis() as u64
}
