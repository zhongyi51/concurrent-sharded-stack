# Intrusive ownership and extension traits

The default `intrusive` feature adds a sharded Treiber stack with no mutex in
its implementation. It reuses `intrusive-collections::Adapter` and `PointerOps`
for layout/ownership conversions and adds two explicit unsafe contracts:

- `ConcurrentLinkOps` supplies thread-safe acquisition/release and atomic next
  access through `&self`. Built-in support targets only
  `singly_linked_list::AtomicLinkOps` / `SinglyLinkedListAtomicLink`.
- `EpochAdapter` requires interchangeable adapter clones, non-panicking
  conversions, stable ownership until restoration, and no conflicting exclusive
  references during that interval. Its full safety contract is in rustdoc.

The pointer must be `Send + 'static`, and the adapter must be `Clone + Send +
Sync + 'static`. Generated `Box` and `Arc` adapters can explicitly opt in with
`unsafe impl EpochAdapter`. Arbitrary scoped borrowed pointers, non-atomic
links, and stateful adapters with incompatible clones cannot opt in unchanged.
Custom links implement `LinkOps` plus `ConcurrentLinkOps`; they need not
implement `SinglyLinkedListOps`. See [the downstream-style custom-link
test](../tests/intrusive_custom.rs) for a complete example.

## Node lifecycle

1. `push(pointer)` erases ownership, claims the link, initializes next, and
   publishes it through a release CAS. Duplicate/retired links panic; closed
   pushes return their original pointer without publishing it.
2. `pop()` pins the epoch, reads the head with acquire ordering, then removes it
   through CAS. The unique winner receives an opaque `Retired<A>` token.
3. The token does not expose `T`, its link, or an owning pointer. Its link stays
   claimed and its next pointer stays unchanged, protecting old readers and
   preventing removal/reinsertion ABA.
4. `retired.defer(callback)` schedules restoration after the grace period.
   Only then is the link released and the original pointer returned to the
   callback. It may now be freed, reinserted, or used in an upstream list.

The same allocation is preserved; there is no per-node wrapper allocation.
Crossbeam thread registration, deferred metadata, large callback captures, and
user adapter operations may still allocate. This is not an allocation-free or
wait-free API. Lock-free progress describes the head/link algorithm, not user
callbacks, memory allocation, or timely reclamation.

A `Retired` can outlive its stack and move to another thread. Dropping it
schedules pointer destruction; forgetting it leaks ownership. Stack destruction
synchronously unlinks/drops still-reachable nodes, resuming cleanup if one
payload destructor panics. Retired nodes remain owned by their tokens/callbacks.
A second destructor panic during unwinding aborts, as with standard containers.

## Delivering callbacks

`defer` batches callbacks in the calling thread's epoch cache instead of flushing
on every node. Dropped tokens use the same cache. Call `intrusive::collect()` on
each retiring thread before waiting for callbacks or becoming idle; another
thread cannot flush its cache. Thread exit also publishes pending callbacks.
Flushing once per work batch (or when the pool is empty) amortizes reclamation
metadata allocation and global queue contention. Flushing after every node
restores eager publication but loses this benefit.

`intrusive::collect()` pins, flushes that thread's deferred work, and unpins. Repeated calls
help make progress, but provide no delivery deadline. Release any externally
held epoch guards while waiting. Delivery may run on another thread and is not
guaranteed at process exit; applications that need completion must count or
acknowledge callbacks, as the [example](../examples/intrusive_recycling.rs) does.
`Closed` reports that shards are drained, not that callbacks are finished.

Callbacks should be short and avoid blocking: they run inside collection work.
A panic propagates on the collecting thread. Safe immediate access to a whole
node before its grace period is intentionally unavailable. No empty marker
trait can make that access sound for an ordinary `Box<T>`.

## Why these traits?

The search considered [cordyceps::Linked<L>](https://docs.rs/cordyceps/0.3.4/cordyceps/trait.Linked.html),
which expresses handles and embedded links but requires pinned, non-`Unpin`
nodes and does not supply an MPMC epoch-reuse protocol. It is an alternative,
not a more general replacement for adapter-based layout/pointer customization.
The standard `AsRef`, `Deref`, and `Borrow` traits do not express link ownership
or reclamation either.

[Upstream Adapter](https://docs.rs/intrusive-collections/0.10.3/intrusive_collections/trait.Adapter.html)
allows stateful adapters and custom pointer representations. Its existing
traits do not promise concurrent atomic access or epoch-safe adapter cloning,
so those requirements belong in separate extension traits, rather than being
silently imposed on existing implementations. Atomic link naming alone is not
sufficient: different upstream collection links have different internals.
