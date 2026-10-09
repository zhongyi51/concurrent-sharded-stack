//! Intrusive ownership using upstream adapters and a selectable reclaimer.
use crate::reclaim::{Factory, Scoped};
use crate::{EpochGuard, Guard, PopError, PushError, core::Core, default_shards};
use intrusive_collections::singly_linked_list::SinglyLinkedListOps;
use intrusive_collections::{Adapter, LinkOps, PointerOps, SinglyLinkedListAtomicLink};
use std::ptr::NonNull;

type Pointer<A> = <<A as Adapter>::PointerOps as PointerOps>::Pointer;

/// A removed node, still claimed until its reclaimer permits reuse.
/// No whole-node access is available before then. Drop schedules destruction;
/// forgetting the token leaks its ownership. The token can outlive its stack.
pub struct Retired<A, G = EpochGuard, L = SinglyLinkedListAtomicLink>
where
    A: Adapter + Clone + Send + Sync + 'static,
    A::LinkOps: SinglyLinkedListOps<LinkPtr = NonNull<L>> + Clone,
    Pointer<A>: Send + 'static,
    G: Guard,
    L: Send + Sync + 'static,
{
    link: Option<NonNull<L>>,
    adapter: A,
    guards: Factory<G>,
}
// SAFETY: the token owns the erased Send pointer; access is deferred. The unsafe
// stack constructor establishes the adapter's cross-thread conversion contract.
unsafe impl<A, G, L> Send for Retired<A, G, L>
where
    A: Adapter + Clone + Send + Sync + 'static,
    A::LinkOps: SinglyLinkedListOps<LinkPtr = NonNull<L>> + Clone,
    Pointer<A>: Send + 'static,
    G: Guard,
    L: Send + Sync + 'static,
{
}
impl<A, G, L> Retired<A, G, L>
where
    A: Adapter + Clone + Send + Sync + 'static,
    A::LinkOps: SinglyLinkedListOps<LinkPtr = NonNull<L>> + Clone,
    Pointer<A>: Send + 'static,
    G: Guard,
    L: Send + Sync + 'static,
{
    /// Deliver the original pointer only after safe reclamation. Callback may
    /// run on another thread; there is no delivery deadline. With EpochGuard, call
    /// collect on the retiring thread before waiting or going idle.
    pub fn defer(self, callback: impl FnOnce(Pointer<A>) + Send + 'static) {
        let mut guard = Scoped::new(&self.guards);
        let address = self.link.unwrap().as_ptr().cast();
        unsafe {
            guard.0.retire(address, move || callback(self.reclaim()));
        }
    }
    unsafe fn reclaim(mut self) -> Pointer<A> {
        let link = self.link.take().unwrap();
        unsafe {
            let value = self.adapter.get_value(link);
            self.adapter.link_ops().clone().release_link(link);
            self.adapter.pointer_ops().from_raw(value)
        }
    }
}
impl<A, G, L> Drop for Retired<A, G, L>
where
    A: Adapter + Clone + Send + Sync + 'static,
    A::LinkOps: SinglyLinkedListOps<LinkPtr = NonNull<L>> + Clone,
    Pointer<A>: Send + 'static,
    G: Guard,
    L: Send + Sync + 'static,
{
    fn drop(&mut self) {
        if let Some(link) = self.link.take() {
            Self {
                link: Some(link),
                adapter: self.adapter.clone(),
                guards: self.guards.clone(),
            }
            .defer(drop);
        }
    }
}

/// Help the default EpochGuard backend. Custom backends expose their own collection controls.
pub fn collect() {
    EpochGuard::collect();
}

/// Sharded intrusive Treiber stack. The only custom trait is [`Guard`].
///
/// Nodes use upstream `Adapter`, `LinkOps`, and `SinglyLinkedListOps`. Because
/// those traits do not promise concurrent access, construction is unsafe; all
/// later operations are safe. Generated Box/Arc adapters with
/// `SinglyLinkedListAtomicLink` satisfy the constructor's requirements.
///
/// ```
/// use concurrent_sharded_stack::{IntrusiveShardedStack, intrusive_collections};
/// use intrusive_collections::{intrusive_adapter, SinglyLinkedListAtomicLink};
/// struct Node { link: SinglyLinkedListAtomicLink, value: usize }
/// intrusive_adapter!(A = Box<Node>: Node { link => SinglyLinkedListAtomicLink });
/// // SAFETY: generated stateless Box adapter and built-in atomic link.
/// let stack = unsafe { IntrusiveShardedStack::with_concurrency(1, A::new()) };
/// stack.push(Box::new(Node { link: Default::default(), value: 7 })).ok().unwrap();
/// stack.pop().unwrap().defer(|node| assert_eq!(node.value, 7));
/// stack.collect(); // helps delivery, does not wait for it
/// ```
///
/// Constructor safety cannot be skipped:
/// ```compile_fail
/// use concurrent_sharded_stack::{IntrusiveShardedStack, intrusive_collections};
/// use intrusive_collections::{intrusive_adapter, SinglyLinkedListAtomicLink};
/// struct Node { link: SinglyLinkedListAtomicLink }
/// intrusive_adapter!(A = Box<Node>: Node { link => SinglyLinkedListAtomicLink });
/// let stack = IntrusiveShardedStack::new(A::new());
/// ```
pub struct IntrusiveShardedStack<A, G = EpochGuard, L = SinglyLinkedListAtomicLink>
where
    A: Adapter + Clone + Send + Sync + 'static,
    A::LinkOps: SinglyLinkedListOps<LinkPtr = NonNull<L>> + Clone,
    Pointer<A>: Send + 'static,
    G: Guard,
    L: Send + Sync + 'static,
{
    core: Core<L, G>,
    adapter: A,
}
impl<A, L> IntrusiveShardedStack<A, EpochGuard, L>
where
    A: Adapter + Clone + Send + Sync + 'static,
    A::LinkOps: SinglyLinkedListOps<LinkPtr = NonNull<L>> + Clone,
    Pointer<A>: Send + 'static,
    L: Send + Sync + 'static,
{
    /// Construct with the default epoch backend and hardware concurrency hint.
    /// # Safety
    /// See [`Self::with_guard_factory`].
    pub unsafe fn new(adapter: A) -> Self {
        unsafe { Self::with_concurrency(default_shards(), adapter) }
    }
    /// Construct with the default epoch backend and a power-of-two shard count.
    /// # Safety
    /// See [`Self::with_guard_factory`].
    pub unsafe fn with_concurrency(count: usize, adapter: A) -> Self {
        unsafe { Self::with_guard_factory(count, adapter, EpochGuard::default) }
    }
    /// Flush this thread's default epoch backend; does not wait for callbacks.
    pub fn collect(&self) {
        EpochGuard::collect();
    }
}
impl<A, G, L> IntrusiveShardedStack<A, G, L>
where
    A: Adapter + Clone + Send + Sync + 'static,
    A::LinkOps: SinglyLinkedListOps<LinkPtr = NonNull<L>> + Clone,
    Pointer<A>: Send + 'static,
    G: Guard,
    L: Send + Sync + 'static,
{
    /// Select a guard factory. Count must be a nonzero power of two; L alignment >= 2.
    ///
    /// # Safety
    /// - The factory must not panic. Every guard it creates must use the same
    ///   reclamation domain; the factory keeps that domain alive. Each call returns
    ///   an independent, inactive guard.
    /// - Adapter clones and link-op clones are interchangeable, non-panicking,
    ///   and safe to invoke concurrently. Erased ownership can be restored by
    ///   any clone, even after stack destruction and on another thread.
    /// - Link acquire/release are atomic, exclusive across compatible containers,
    ///   and synchronize reuse. Next access is atomic and preserves provenance.
    ///   No operation creates a conflicting exclusive reference to node/link.
    /// - Erased pointers keep the node at a stable live address until restored.
    ///   Other owners cannot move/free it, mutate the claimed link, or obtain
    ///   conflicting exclusive references while published or retired.
    ///
    /// These hold for generated Box/Arc adapters with the upstream singly atomic
    /// link. Ordinary non-atomic links do not satisfy this contract.
    pub unsafe fn with_guard_factory(
        count: usize,
        adapter: A,
        make_guard: impl Fn() -> G + Send + Sync + 'static,
    ) -> Self {
        Self {
            core: Core::new(count, make_guard),
            adapter,
        }
    }
    /// Publish a pointer. Duplicate/retired links panic. Closed rejection returns
    /// the original pointer, with this attempted insertion's claim released.
    pub fn push(&self, value: Pointer<A>) -> Result<(), PushError<Pointer<A>>> {
        if self.core.local_closed() {
            return Err(PushError::Closed(value));
        }
        let raw = self.adapter.pointer_ops().into_raw(value);
        let link = unsafe { self.adapter.get_link(raw) };
        let mut ops = self.adapter.link_ops().clone();
        unsafe {
            if !ops.acquire_link(link) {
                drop(self.adapter.pointer_ops().from_raw(raw));
                panic!("attempted to insert an object that is already linked or retired");
            }
            if self.core.push(link, &mut ops) {
                return Ok(());
            }
            ops.release_link(link);
            Err(PushError::Closed(self.adapter.pointer_ops().from_raw(raw)))
        }
    }
    /// Remove a node without prematurely exposing ownership. LIFO within shards;
    /// Empty is a scan result, Closed means closed/drained (not callbacks done).
    pub fn pop(&self) -> Result<Retired<A, G, L>, PopError> {
        let adapter = self.adapter.clone();
        let guards = self.core.guards.clone();
        let link = unsafe { self.core.pop(adapter.link_ops())? };
        Ok(Retired {
            link: Some(link),
            adapter,
            guards,
        })
    }
    /// Close each shard. Returns whether this call closed any shard.
    pub fn close(&self) -> bool {
        self.core.close()
    }
    /// Check whether every shard is closed.
    pub fn is_closed(&self) -> bool {
        self.core.is_closed()
    }
    /// Number of shards.
    pub fn shard_count(&self) -> usize {
        self.core.count()
    }
}
impl<A, G, L> Drop for IntrusiveShardedStack<A, G, L>
where
    A: Adapter + Clone + Send + Sync + 'static,
    A::LinkOps: SinglyLinkedListOps<LinkPtr = NonNull<L>> + Clone,
    Pointer<A>: Send + 'static,
    G: Guard,
    L: Send + Sync + 'static,
{
    fn drop(&mut self) {
        let drain = || {
            for index in 0..self.core.count() {
                while let Some(link) =
                    unsafe { self.core.take_exclusive(index, self.adapter.link_ops()) }
                {
                    // Exclusive stack teardown: retired nodes are separately owned.
                    unsafe {
                        let value = self.adapter.get_value(link);
                        self.adapter.link_ops().clone().release_link(link);
                        drop(self.adapter.pointer_ops().from_raw(value));
                    }
                }
            }
        };
        let cleanup = scopeguard::guard((), |_| drain());
        drain();
        scopeguard::ScopeGuard::into_inner(cleanup);
    }
}
