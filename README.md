# tokio-rs [learn]

A small learning implementation of an async runtime, inspired by Tokio.

This project demonstrates the core concepts of async runtimes in Rust:

- **Custom `RawWaker`** - Implementing the raw waker vtable for task scheduling
- **Executor** - A task queue with `Arc<Mutex<_>>` synchronization
- **Reactor** - Mio-based event loop for I/O multiplexing
- **Async TCP** - Non-blocking TCP streams and listeners
- **Timer** - Async timer futures
- **Multi-threaded** - Basic multi-threaded execution support

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐
│  Executor   │────▶│  Task Queue  │────▶│   Future    │
│  (run loop) │     │  (VecDeque)  │     │   (async)   │
└──────┬──────┘     └──────────────┘     └─────────────┘
       │
       │  waker
       ▼
┌─────────────┐     ┌──────────────┐
│   Reactor   │◀────│  Mio Poll    │
│  (Events)   │     │  (epoll/kqueue)│
└─────────────┘     └──────────────┘
```

## Getting Started

### Run tests

```bash
cargo test
```

### Run the echo server example

```bash
cargo run --example echo
```

### Check compilation

```bash
cargo check
cargo clippy
```

## Usage

### Basic executor with timer

```rust
use tokio_rs::{Executor, TimerFuture};
use std::time::Duration;

let mut executor = Executor::new();
executor.spawn(async {
    TimerFuture::new(Duration::from_millis(100)).await;
    println!("Timer completed!");
});
executor.run();
```

### Async TCP echo server

```rust
use tokio_rs::{AsyncTcpListener, AsyncTcpStream, Executor};
use std::net::SocketAddr;
use mio::Token;

let mut executor = Executor::new();
let reactor = executor.reactor();
let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
let mut listener = AsyncTcpListener::bind(addr, Arc::clone(&reactor), Token(0)).unwrap();

executor.spawn(async move {
    loop {
        let (stream, peer_addr) = listener.accept().await.unwrap();
        // Handle connection...
    }
});

executor.run();
```

## Key Components

### Executor

The `Executor` manages a queue of tasks. It polls each task and re-queues pending tasks. When no tasks are ready, it blocks on the reactor waiting for I/O events.

### Reactor

The `Reactor` wraps `mio::Poll` to provide I/O event notification. It maintains a map of tokens to wakers, waking tasks when their associated I/O is ready.

### Async TCP

- `AsyncTcpStream` - Non-blocking TCP stream with `read()` and `write()` futures
- `AsyncTcpListener` - Non-blocking TCP listener with `accept()` future

### Custom RawWaker

The `Task` struct implements a custom `RawWaker` vtable. The waker uses `Arc::into_raw`/`Arc::from_raw` to manage task lifetime, allowing tasks to be re-scheduled when I/O events occur.

## Known Limitations

- Memory leak in `RawWaker` vtable functions due to `Arc::mem::forget` pattern
- 10ms timeout on reactor `wait()` causes minor latency
- Single-threaded by default (multi-threaded support is basic)
- `AsyncTcpStream::write()` requires manual buffering for `write_all`
- `std::io::Write` trait not implemented on `AsyncTcpStream`

## License

MIT
