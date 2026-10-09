# Reclamation and ownership

`src/core.rs` is the single intrusive head algorithm. It uses standard atomics,
upstream `SinglyLinkedListOps`, and `Guard`. `src/epoch.rs` is the only
production module that calls Crossbeam EBR. Both public stacks use this core.

## One extension trait

`Guard` owns the whole protection protocol:

| Method | Meaning |
|---|---|
| `protect` | Enter protection, load a tagged head, protect its untagged link address |
| `unpin` | End protection; idempotent and non-panicking |
| `retire(address, action)` | Execute action only after old accesses have ended |

An EBR backend pins before Acquire loads. A hazard backend must publish the
address and validate the head before permitting dereferences, or use another
proven synchronization protocol. Each protect replaces the guard's previous
protection. The core never dereferences next without a new protected head load.
Push and close also protect copied head pointers to preserve their provenance.

Bit zero of a head is the close tag. Return the exact tagged snapshot for CAS,
but protect/retire the untagged link identity. The containing object's address
may differ. Guards, loads, retirement, and factory-created guards must belong
to the same domain **instance**, not merely the same Rust type.

The action transfers ownership: never discard it before safe reclamation, even
on domain teardown. It may run synchronously once eligible, or on another thread.
Guard creation must not panic; retirement must not unwind before accepting the action.
Callbacks may panic. A backend may leak pending work but must never reclaim early.
The complete contract is in `Guard` rustdoc.

There is no domain trait, associated guard type, or `pin` method. A normal
`Fn() -> G + Send + Sync + 'static` factory creates independent inactive guards.
It retains the domain, and **all** guards it creates must use that same domain
instance. This is why custom-factory construction is unsafe, even for values.
`protect` starts protection lazily; an internal scope wrapper calls `unpin` on
normal return and panic. `retire` also works on an inactive guard. The guard may
be !Send/!Sync; it is created on the thread performing the operation, never stored
inside the shared stack or transferred in a `Retired` token. Tokens retain the
factory so they can retire on another thread after the stack has been dropped.

`collect`/flush is backend-specific. The default `EpochGuard::collect()`,
`stack.collect()` (default backend only), and `intrusive::collect()` flush the
current thread's Crossbeam bag; none promises callback completion. A custom
backend exposes its own collection method independently of `Guard`.

The public type remains `IntrusiveShardedStack<A, G, L>`: upstream `Adapter` A
maps the user's node T to its embedded link L; G is solely the reader/retirement
guard. L is normally inferred and defaults to `SinglyLinkedListAtomicLink`.
Keeping L supports custom atomic links without introducing another trait.

`tests/support/mod.rs` demonstrates an address-based backend using a mutex to
serialize hazard publication with reclamation decisions. It is a correctness
fixture, not a proposed production lock-free hazard pointer implementation.

## Upstream adapters, without marker traits

The intrusive constructor is unsafe because upstream adapter/link traits do not
promise concurrent execution. It requires interchangeable non-panicking clones,
atomic link claim/release and next access, stable erased ownership, and no
conflicting exclusive references. Generated Box/Arc adapters with upstream
`SinglyLinkedListAtomicLink` satisfy these conditions. Arbitrary ordinary links
or stateful adapters may not. Custom atomic links implement upstream `LinkOps`
and `SinglyLinkedListOps`; no crate-specific link trait is needed.

After construction, push/pop/close are safe. Pop produces `Retired`, which owns
the removed node but exposes no node reference. Only its reclaim action releases
the link and restores the original pointer. A token can outlive its stack.
`Retired::drop` schedules destruction; `forget` leaks. Neither retains a read guard.

## Values and cached storage

The value wrapper owns `repr(C) { atomic_link, MaybeUninit<T> }` nodes. Only the
successful pop reads payload bytes, using a raw field pointer (never a whole-node
exclusive reference). Old readers access only the separate link. T is returned
immediately; the empty allocation is retired, then unclaimed and cached.

Callbacks carry only an erased block pointer, a layout-specific deallocator,
and a Weak cache handle. They never inspect/drop T, so borrowed and non-Send
payloads remain supported locally. Per-shard bounded queues transfer ready blocks
without another intrusive retirement cycle. A dead/full cache frees the block.
Capacity bounds ready blocks, not delayed retirement backlog. The value cache
uses bounded ArrayQueues; whole-operation progress also depends on those queues.
Reachable values
are dropped synchronously on stack destruction; cleanup resumes after one panic.

## Migrating from 0.3

1. Remove `EpochAdapter` and `ConcurrentLinkOps` implementations/imports.
2. For custom links, implement upstream `SinglyLinkedListOps` instead.
3. Wrap intrusive construction in one documented unsafe block asserting the
   concurrency contract. There is no blanket safe `Default` constructor.
4. For external reclamation, implement `Guard` and pass a normal closure to
   `with_guard_factory`. Remove the earlier draft's `Reclaimer` implementation,
   associated guard type, and `pin`/`collect` trait methods. Move protection and
   retirement to G; keep collection controls on your own domain/backend.
5. Keep `Retired::defer` usage. Use `stack.collect()` or `intrusive::collect()`
   for the default epoch backend; use your backend's API otherwise.

The ordinary value API and its Send/Sync/lifetime requirements are preserved.
