# Pressly 🦀

> Ephemeral, encrypted collaborative code scratchpad. No accounts. Rooms vanish after 2hrs. Server never sees your code.

![Rust](https://img.shields.io/badge/rust-1.95-orange?logo=rust)
![Build](https://img.shields.io/badge/build-passing-brightgreen)
![License](https://img.shields.io/badge/license-MIT-blue)

## What is it?

Pressly is a real-time collaborative code runner built for developers who want to share, run, and review code instantly — without signing up, without leaving traces.

Think pastebin × CoderPad, but:

- **No accounts** — just create a room and share the link
- **Ephemeral** — rooms self-destruct after 2 hours
- **E2E encrypted** — the server never sees plaintext code *(Phase 4)*
- **Diff output** — every run shows what changed vs the last run
- **Snapshot sync** — new peers joining get the current editor state instantly
- **Witness mode** — read-only spectator link for demos and interviews

---

## Features

| Feature | Status |
|---|---|
| Room creation with TTL | ✅ Phase 1 |
| WebSocket real-time sync | ✅ Phase 1 |
| Peer join/leave events | ✅ Phase 1 |
| Editor snapshot for late joiners | ✅ Phase 1 |
| Sandboxed code execution (Rust, Python) | ✅ Phase 2 |
| Stdout/stderr streaming | ✅ Phase 2 |
| Output diff between runs | ✅ Phase 3 |
| E2E encryption (X25519 + ChaCha20) | 🔨 Phase 4 |
| Witness mode | 🔨 Phase 5 |
| Dead drop (one-time result link) | 🔨 Phase 6 |
| Replay mode | 🔨 Phase 7 |

---

## Stack

**Backend** — Rust
- `axum` — HTTP + WebSocket server
- `tokio` — async runtime
- `dashmap` — concurrent room registry
- `serde_json` — message serialization

**Frontend** *(coming soon)*
- Next.js + Monaco Editor + Tailwind CSS

---

## Getting started

### Prerequisites
- Rust 1.75+
- `rustc` in PATH (for Rust code execution)
- `python3` in PATH (for Python code execution)

### Run locally

```bash
git clone https://github.com/Haileyesus-22/pressly
cd pressly
cargo run
```

Server starts on `http://localhost:3001`

### Environment

```bash
RUST_LOG=pressly=debug cargo run
```

---

## API

### Create a room
```bash
curl -X POST http://localhost:3001/rooms \
  -H "Content-Type: application/json" \
  -d '{"language": "rust", "mode": "collab", "ttl_minutes": 120}'
```

```json
{
  "room_id": "swift-a3f2b1",
  "expires_at": "2026-05-25T04:00:00Z",
  "mode": "collab"
}
```

### Get room info
```bash
curl http://localhost:3001/rooms/swift-a3f2b1
```

### Connect via WebSocket
```bash
wscat -c ws://localhost:3001/rooms/swift-a3f2b1/ws
```

---

## WebSocket protocol

All messages are JSON with a `type` field.

### Send code edit
```json
{
  "type": "client_edit",
  "content": "fn main() { println!(\"hello\"); }",
  "cursor_line": null,
  "cursor_col": null
}
```

### Trigger execution
```json
{
  "type": "run_request",
  "run_id": "run-001"
}
```

### Receive output (streamed line by line)
```json
{
  "type": "output_line",
  "run_id": "run-001",
  "line": "hello",
  "stream": "stdout"
}
```

### Receive result with diff
```json
{
  "type": "run_result",
  "run_id": "run-001",
  "exit_code": 0,
  "duration_ms": 1458,
  "diff": [
    { "kind": "equal", "content": "hello" },
    { "kind": "added", "content": "world" }
  ]
}
```

---

## Project structure

```
src/
  main.rs       — axum router, background TTL reaper
  room.rs       — Room actor, RoomRegistry, peer count
  ws.rs         — WebSocket handler, message protocol, peer loop
  executor.rs   — sandboxed subprocess execution, stdout/stderr streaming
  diff.rs       — line-level output diff between runs
  error.rs      — shared error types
```

---

## Building in public

Following this project day by day on X → [@dev_iniit](https://x.com/dev_iniit)

`#buildinpublic` `#rust` `#100DaysOfCode`

---

## License

MIT
