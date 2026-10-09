mod support;
use concurrent_sharded_stack::{ConcurrentShardedStack, Guard, IntrusiveShardedStack, PopError};
use intrusive_collections::{SinglyLinkedListAtomicLink, intrusive_adapter};
use std::sync::{
    Arc,
    atomic::{AtomicPtr, AtomicUsize, Ordering},
};
use support::Hazards;

#[repr(C)]
struct Node {
    prefix: [u64; 4],
    link: SinglyLinkedListAtomicLink,
    id: usize,
}
intrusive_adapter!(A = Arc<Node>: Node { link => SinglyLinkedListAtomicLink });
fn node(id: usize) -> Arc<Node> {
    Arc::new(Node {
        prefix: [42; 4],
        link: Default::default(),
        id,
    })
}

#[test]
fn hazard_identity_is_untagged_link_not_container_and_other_nodes_progress() {
    let domain = Hazards::default();
    let stack = unsafe { IntrusiveShardedStack::with_guard_factory(1, A::new(), domain.factory()) };
    let a = node(1);
    let b = node(2);
    let link = &a.link as *const _ as *mut SinglyLinkedListAtomicLink;
    assert_ne!(link.cast::<()>(), Arc::as_ptr(&a).cast_mut().cast());
    stack.push(a.clone()).ok().unwrap();
    stack.push(b).ok().unwrap();
    // Model a reader of a tagged head, then unlink the shadow source so no
    // new reader can acquire this ownership cycle from it.
    let source = AtomicPtr::new(link.map_addr(|a| a | 1));
    let mut guard = domain.guard();
    assert_eq!(unsafe { guard.protect(&source) }, link.map_addr(|a| a | 1));
    source.store(std::ptr::null_mut(), Ordering::Release);
    stack.close();
    let count = Arc::new(AtomicUsize::new(0));
    for _ in 0..2 {
        let count = count.clone();
        stack.pop().unwrap().defer(move |n| {
            count.fetch_add(n.id, Ordering::Relaxed);
        });
    }
    domain.collect();
    assert_eq!(count.load(Ordering::Relaxed), 2); // B is independent of A's hazard.
    assert!(a.link.is_linked());
    assert!(matches!(stack.pop(), Err(PopError::Closed)));
    drop(stack);
    guard.unpin();
    domain.collect();
    assert_eq!(count.load(Ordering::Relaxed), 3);
    assert!(!a.link.is_linked());
}

#[test]
fn custom_backend_value_stack_preserves_borrowed_and_non_send_payloads() {
    let domain = Hazards::default();
    let text = String::from("borrowed");
    let stack = unsafe { ConcurrentShardedStack::with_guard_factory(1, 64, domain.factory()) };
    stack.push(text.as_str()).unwrap();
    assert_eq!(stack.pop(), Ok("borrowed"));
    drop(stack);
    drop(text); // callbacks must never access the already removed borrowed T.
    domain.collect();
    let stack = unsafe { ConcurrentShardedStack::with_guard_factory(1, 64, domain.factory()) };
    let rc = std::rc::Rc::new(7);
    stack.push(rc.clone()).unwrap();
    assert_eq!(stack.pop(), Ok(rc.clone()));
    drop(stack);
    drop(rc);
    std::thread::spawn(move || domain.collect()).join().unwrap();
}

#[test]
fn reference_hazard_backend_concurrent_transfer_and_reuse() {
    let domain = Hazards::default();
    let stack = unsafe { ConcurrentShardedStack::with_guard_factory(4, 8, domain.factory()) };
    let n = if cfg!(miri) { 16 } else { 2_000 };
    let seen: Vec<_> = (0..4 * n).map(|_| AtomicUsize::new(0)).collect();
    std::thread::scope(|scope| {
        for worker in 0..4 {
            let stack = &stack;
            let seen = &seen;
            scope.spawn(move || {
                for i in 0..n {
                    stack.push(worker * n + i).unwrap();
                    loop {
                        if let Ok(id) = stack.pop() {
                            assert_eq!(seen[id].fetch_add(1, Ordering::Relaxed), 0);
                            break;
                        }
                        std::thread::yield_now();
                    }
                }
            });
        }
    });
    assert!(seen.iter().all(|v| v.load(Ordering::Relaxed) == 1));
    stack.close();
    assert_eq!(stack.pop(), Err(PopError::Closed));
    domain.collect();
}

#[test]
fn retire_token_keeps_custom_domain_alive_and_callbacks_can_reinsert() {
    let domain = Hazards::default();
    let stack = Arc::new(unsafe {
        IntrusiveShardedStack::with_guard_factory(1, A::new(), domain.factory())
    });
    stack.push(node(7)).ok().unwrap();
    let retired = stack.pop().unwrap();
    let target = stack.clone();
    retired.defer(move |n| {
        target.push(n).ok().unwrap();
    });
    domain.collect(); // callback runs outside domain lock, can pin and publish.
    let retired = stack.pop().unwrap();
    drop(stack);
    drop(domain); // Only the retired token's factory now retains this domain.
    let id = Arc::new(AtomicUsize::new(0));
    let result = id.clone();
    std::thread::spawn(move || {
        retired.defer(move |n| {
            result.store(n.id, Ordering::Relaxed);
        })
    })
    .join()
    .unwrap();
    assert_eq!(id.load(Ordering::Relaxed), 7);
}

#[test]
fn send_but_not_sync_values_can_cross_threads() {
    let stack =
        unsafe { ConcurrentShardedStack::with_guard_factory(1, 64, Hazards::default().factory()) };
    std::thread::scope(|scope| {
        let stack = &stack;
        scope.spawn(move || stack.push(std::cell::Cell::new(42)).unwrap());
    });
    assert_eq!(stack.pop().unwrap().get(), 42);
}

#[test]
fn panicking_callback_releases_link_and_ownership_once() {
    let domain = Hazards::default();
    let stack = unsafe { IntrusiveShardedStack::with_guard_factory(1, A::new(), domain.factory()) };
    let a = node(7);
    stack.push(a.clone()).ok().unwrap();
    let retired = stack.pop().unwrap();
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            retired.defer(|_| panic!("user callback"));
        }))
        .is_err()
    );
    assert!(!a.link.is_linked());
    assert_eq!(Arc::strong_count(&a), 1);
    stack.push(a).ok().unwrap();
    drop(stack.pop().unwrap());
    domain.collect();
}

#[test]
fn scope_unpins_on_empty_closed_and_protection_panic() {
    let domain = Hazards::default();
    let stack = unsafe { IntrusiveShardedStack::with_guard_factory(1, A::new(), domain.factory()) };
    assert!(matches!(stack.pop(), Err(PopError::Empty)));
    assert_eq!(domain.counts(), (0, 0, 1));
    let a = node(1);
    stack.push(a.clone()).ok().unwrap();
    let (_, _, before) = domain.counts();
    domain.panic_next_protect();
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| stack.pop())).is_err());
    assert_eq!(domain.counts(), (0, 0, before + 1));
    assert!(a.link.is_linked());
    drop(stack.pop().unwrap());
    assert_eq!(domain.counts().1, 1);
    stack.close();
    let (_, _, before) = domain.counts();
    assert!(matches!(stack.pop(), Err(PopError::Closed)));
    assert_eq!(domain.counts(), (0, 1, before + 1));
    assert!(!a.link.is_linked());
}

#[test]
fn guard_can_replace_protection_and_reenter_after_idempotent_unpin() {
    let domain = Hazards::default();
    let mut a = 1_u64;
    let mut b = 2_u64;
    let first = AtomicPtr::new(&mut a);
    let second = AtomicPtr::new(&mut b);
    let mut guard = domain.guard();
    let done = Arc::new(AtomicUsize::new(0));
    unsafe {
        guard.protect(&first);
    }
    first.store(std::ptr::null_mut(), Ordering::Release);
    let result = done.clone();
    unsafe {
        domain.guard().retire((&raw mut a).cast(), move || {
            result.fetch_add(1, Ordering::Relaxed);
        });
    }
    assert_eq!(done.load(Ordering::Relaxed), 0);
    unsafe {
        guard.protect(&second);
    }
    domain.collect();
    assert_eq!(done.load(Ordering::Relaxed), 1);
    guard.unpin();
    guard.unpin();
    assert_eq!(domain.counts().0, 0);
    unsafe {
        guard.protect(&second);
    }
    assert_eq!(domain.counts().0, 1);
    guard.unpin();
    assert_eq!(domain.counts().0, 0);
}
