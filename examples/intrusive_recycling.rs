use concurrent_sharded_stack::{IntrusiveShardedStack, intrusive, intrusive_collections};
use intrusive_collections::{SinglyLinkedListAtomicLink, intrusive_adapter};
use std::sync::mpsc::{self, TryRecvError};
use std::time::{Duration, Instant};

#[derive(Debug)]
struct Buffer {
    link: SinglyLinkedListAtomicLink,
    bytes: Vec<u8>,
}
intrusive_adapter!(BufferAdapter = Box<Buffer>: Buffer { link => SinglyLinkedListAtomicLink });
// SAFETY: generated stateless adapter and Box keep this atomic-linked node
// alive at a stable address until the collector restores its owning pointer.
fn main() {
    let pool = unsafe { IntrusiveShardedStack::with_concurrency(4, BufferAdapter::new()) };
    pool.push(Box::new(Buffer {
        link: SinglyLinkedListAtomicLink::new(),
        bytes: Vec::with_capacity(256),
    }))
    .unwrap();
    let (tx, rx) = mpsc::channel();
    pool.pop()
        .unwrap()
        .defer(move |buffer| tx.send(buffer).unwrap());
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut buffer = loop {
        match rx.try_recv() {
            Ok(buffer) => break buffer,
            Err(TryRecvError::Empty) => {
                assert!(Instant::now() < deadline, "epoch delivery stalled");
                intrusive::collect();
                std::thread::yield_now();
            }
            Err(TryRecvError::Disconnected) => panic!("callback disconnected"),
        }
    };
    assert!(!buffer.link.is_linked());
    buffer.bytes.extend_from_slice(b"response body");
    buffer.bytes.clear();
    pool.push(buffer).unwrap();
    pool.close();
    // Stack drop synchronously destroys nodes still reachable in its shards.
    println!("Reused the same intrusive buffer after its epoch grace period");
}
