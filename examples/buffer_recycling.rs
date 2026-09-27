//! Reuse a fixed number of response buffers across multiple workers.
//!
//! Run with `cargo run --example buffer_recycling`. Buffer storage is reused,
//! but the stack still allocates a new node on every push.

use concurrent_sharded_stack::{ConcurrentShardedStack, PopError};
use crossbeam_utils::Backoff;
use std::io::Write;
use std::sync::Barrier;
use std::thread;

const WORKERS: usize = 4;
const BUFFERS: usize = 2;
const REQUESTS_PER_WORKER: usize = 1_000;

fn main() {
    let pool = ConcurrentShardedStack::with_concurrency(WORKERS);
    for _ in 0..BUFFERS {
        pool.push(Vec::with_capacity(256)).unwrap();
    }
    let start = Barrier::new(WORKERS);

    let bytes_processed: usize = thread::scope(|scope| {
        let handles: Vec<_> = (0..WORKERS)
            .map(|worker| {
                let pool = &pool;
                let start = &start;
                scope.spawn(move || {
                    start.wait();
                    let mut bytes_processed = 0;
                    for request in 0..REQUESTS_PER_WORKER {
                        let backoff = Backoff::new();
                        let mut buffer = loop {
                            match pool.pop() {
                                Ok(buffer) => break buffer,
                                // Another worker may hold a buffer, or this scan
                                // may miss a concurrent return. Do not grow the pool.
                                Err(PopError::Empty) => backoff.snooze(),
                                Err(PopError::Closed) => {
                                    // This example closes only after all workers join.
                                    panic!("pool closed before workers finished");
                                }
                            }
                        };

                        // Stand in for serializing and sending a small response.
                        // These messages fit within the reserved buffer capacity.
                        writeln!(&mut buffer, "worker={worker} request={request}").unwrap();
                        bytes_processed += buffer.len();

                        buffer.clear();
                        pool.push(buffer).expect("pool is still open");
                    }
                    bytes_processed
                })
            })
            .collect();
        // Completion is established by joining every worker, never by Empty.
        handles.into_iter().map(|h| h.join().unwrap()).sum()
    });

    pool.close();
    let mut returned_buffers = 0;
    loop {
        match pool.pop() {
            Ok(buffer) => {
                assert!(buffer.is_empty());
                returned_buffers += 1;
            }
            Err(PopError::Closed) => break,
            Err(PopError::Empty) => unreachable!("all shards are closed"),
        }
    }
    assert_eq!(returned_buffers, BUFFERS);
    println!(
        "Processed {} responses ({bytes_processed} bytes), reusing {BUFFERS} buffers",
        WORKERS * REQUESTS_PER_WORKER
    );
}
