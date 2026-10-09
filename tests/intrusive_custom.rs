//! A downstream-style custom link, independent of the upstream atomic layout.
use concurrent_sharded_stack::{IntrusiveShardedStack, intrusive, intrusive_collections};
use intrusive_collections::singly_linked_list::SinglyLinkedListOps;
use intrusive_collections::{DefaultLinkOps, LinkOps, intrusive_adapter};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};
use std::sync::mpsc;

#[derive(Default)]
struct Link {
    next: AtomicPtr<Link>,
    claimed: AtomicBool,
}
#[derive(Clone, Copy, Default)]
struct Ops;
impl DefaultLinkOps for Link {
    type Ops = Ops;
    const NEW: Ops = Ops;
}
// SAFETY: stateless operations on atomics; acquire/release synchronize reuse.
unsafe impl LinkOps for Ops {
    type LinkPtr = NonNull<Link>;
    unsafe fn acquire_link(&mut self, p: Self::LinkPtr) -> bool {
        unsafe {
            p.as_ref()
                .claimed
                .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
                .is_ok()
        }
    }
    unsafe fn release_link(&mut self, p: Self::LinkPtr) {
        unsafe { p.as_ref().claimed.store(false, Ordering::Release) };
    }
}
unsafe impl SinglyLinkedListOps for Ops {
    unsafe fn next(&self, p: NonNull<Link>) -> Option<NonNull<Link>> {
        unsafe { NonNull::new(p.as_ref().next.load(Ordering::Relaxed)) }
    }
    unsafe fn set_next(&mut self, p: NonNull<Link>, next: Option<NonNull<Link>>) {
        unsafe {
            p.as_ref().next.store(
                next.map_or(std::ptr::null_mut(), NonNull::as_ptr),
                Ordering::Relaxed,
            )
        };
    }
}
struct Node {
    link: Link,
    id: usize,
}
intrusive_adapter!(CustomAdapter = Box<Node>: Node { link => Link });
// SAFETY: stateless generated adapter, with Box preserving the allocation.

#[test]
fn custom_link_ops_and_adapter_round_trip() {
    let stack = unsafe { IntrusiveShardedStack::with_concurrency(1, CustomAdapter::new()) };
    let node = Box::new(Node {
        link: Link::default(),
        id: 7,
    });
    let address = &*node as *const Node;
    assert!(stack.push(node).is_ok());
    let (tx, rx) = mpsc::channel();
    stack
        .pop()
        .unwrap()
        .defer(move |node| tx.send(node).ok().unwrap());
    drop(stack);
    for _ in 0..100_000 {
        if let Ok(node) = rx.try_recv() {
            assert_eq!(&*node as *const Node, address);
            assert_eq!(node.id, 7);
            assert!(!node.link.claimed.load(Ordering::Relaxed));
            return;
        }
        intrusive::collect();
        std::thread::yield_now();
    }
    panic!("callback did not run");
}
