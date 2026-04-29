//! Reproduces the park/unpark overhead of `select!` under sustained
//! single-message traffic.
//!
//! Two channels (work + control); the consumer blocks in `select!`. Because
//! `select!` (via `run_select`) does no spin-with-backoff before parking — unlike
//! standalone `Receiver::recv()` on the list flavor — every send registers the
//! receiver and notifies it via mutex + `unpark`.
//!
//! At ~1us send intervals the consumer finishes processing well before the next
//! send, re-parks, then is unparked again. `SyncWaker::notify` and
//! `Thread::unpark` dominate the producer's profile.
//!
//! Message count via `MESSAGES_NUM` env (default 1_000_000).
//! Send interval via `SEND_INTERVAL_NS` env in nanoseconds (default 1000).
//!
//! Run with:
//!   cargo run --release --example select_park_overhead -p crossbeam-channel
//!   MESSAGES_NUM=200000 SEND_INTERVAL_NS=500 cargo run --release --example select_park_overhead -p crossbeam-channel
//!
//! Profile with samply:
//!   cargo build --release --example select_park_overhead -p crossbeam-channel \
//!     && samply record ./target/release/examples/select_park_overhead

use std::sync::LazyLock;
use std::thread;
use std::time::{Duration, Instant};

use crossbeam_channel::{bounded, select, unbounded};

const DEFAULT_MESSAGES: u64 = 1_000_000;
const DEFAULT_SEND_INTERVAL_NS: u64 = 1000;

static MESSAGES: LazyLock<u64> = LazyLock::new(|| {
    std::env::var("MESSAGES_NUM")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_MESSAGES)
});

static SEND_INTERVAL: LazyLock<Duration> = LazyLock::new(|| {
    Duration::from_nanos(
        std::env::var("SEND_INTERVAL_NS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_SEND_INTERVAL_NS),
    )
});

fn spin_for(d: Duration) {
    let start = Instant::now();
    while start.elapsed() < d {}
}

fn main() {
    let (work_tx, work_rx) = unbounded::<u64>();
    let (ctrl_tx, ctrl_rx) = bounded::<()>(1);

    let consumer = thread::spawn(move || {
        let mut count: u64 = 0;
        loop {
            select! {
                recv(work_rx) -> msg => match msg {
                    Ok(_) => count += 1,
                    Err(_) => break,
                },
                recv(ctrl_rx) -> _ => break,
            }
        }
        count
    });

    let start = Instant::now();
    for i in 0..*MESSAGES {
        work_tx.send(i).unwrap();
        spin_for(*SEND_INTERVAL);
    }
    drop(work_tx);
    let _ = ctrl_tx.send(());

    let received = consumer.join().unwrap();
    let elapsed = start.elapsed();

    println!("sent     : {}", *MESSAGES);
    println!("received : {}", received);
    println!("elapsed  : {:?}", elapsed);
    println!(
        "per-msg  : {:.2} ns",
        elapsed.as_nanos() as f64 / *MESSAGES as f64
    );
}
