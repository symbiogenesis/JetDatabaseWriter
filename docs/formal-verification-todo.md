# Formal verification and stronger guarantees: open plan

This plan strengthens the guarantees of the existing managed JET/ACE implementation. It adds development and CI tools first; the shipped targets, public runtime requirements and package dependency graph stay unchanged. A proof of an abstract protocol does not verify the existing C# implementation or a filesystem. Keep those evidence levels separate in documentation.

Expand the production Dafny boundary incrementally under the [boundary expansion policy](design/formal-verification.md#boundary-expansion-policy). Prioritize safety-critical behavior with useful contracts and actual production invocation; source volume is not the goal. Keep storage effects and unsupported native semantics explicit while progressively removing handwritten decision logic.

Keep only open work in this document. Remove completed requirements in the same commit that delivers them; retain contracts, assumptions, proof coverage and implementation correspondence in `docs/design/formal-verification.md` and proof documentation. This plan complements, rather than replaces, E1/E2/E4, F6, S1, I2/I5/I15/I17 in [the main todo list](todo.md).

## Acceptance policy

- Every theorem names its fault model and assumptions. A conditional theorem must not become an unconditional README claim.
- Preserve pure managed execution on netstandard2.1 and net10.0. No shipping verifier, solver, native component, external service or new package dependency is required by the initial work.
- Model process failure separately from power loss, media corruption, adversarial namespace edits and caller-stream behavior. Assume neither atomic page writes nor portable directory durability without evidence.
- Safety includes retaining evidence and refusing an unsafe operation. Recovery availability is a separate obligation: under valid evidence, exclusive access and eventually successful I/O, repeated recovery must converge to the authorized image.
- Model checks are bounded evidence; inductive proofs quantify over unbounded model instances. Neither substitutes for implementation correspondence or native interoperability.
- Local verification, Release builds and focused tests are allowed. Full test suites and every benchmark remain hosted-only. Changes to build/test behavior require the ordinary settled-main CI gate and any new proof gate.

## V3 — Refine journal persistence and page correspondence

Cause: the buffered journal protocol constructs aligned multi-page captures and write/truncate preparation with complete original-tail coverage and zero padding beyond the file end. Record-derived minimum/replay controls and content admission compose logical mutations and buffered effects under an explicit zero-exposure contract. Unrestricted pre-marker reordering can retain exact undo evidence while exposing an unrecorded zero gap that the parser must refuse. Serialized acquisition, the zero-exposure storage/protocol boundary and production-loop correspondence still require refinement.

- [ ] Bind the constructed page payloads and capture/preparation schedules to serialized record tags, manifest/page hashes, framing and production record iteration.
- [ ] Establish production/storage correspondence for the buffered content refinement's zero-exposure contract, or extend the protocol to safely admit pre-marker survivors where an old write or length effect reextends a truncated image. Repro: truncation persists before an older write and the resulting zero gap has no durable gap intent or replay marker; exact undo coverage alone cannot authorize parser admission (V3/V7).

Acceptance: refine the checked buffered protocol under explicit page and parser contracts. Preserve its trace/progress theorems without promoting logical record classification to a verified byte parser or abstract progress to C# loop refinement.

## V5 — Refine replacement evidence and physical publication

Cause: bounded header/decision array scans, exact-read cursor decisions and length/admission kernels refine the replacement byte classifier, with production conformance for field corruption and admission before bootstrap. Immutable-copy validation, complete generation authority, failed I/O and physical publication still require refinement.

- [ ] Extend complete `Publish`/`Recover` callback interruption traces to partial failed native syscall survivors and their persistence contracts. Repro: exceptions before/after real publication reads, writes and both flush fences retain selection, but do not determine which bytes or namespace effects a failed syscall persists (V5/V7).
- [ ] Extend production correspondence beyond the selected bootstrap and production-order cleanup traces, including receipt identity, interrupted acquisition/publication and failed bootstrap I/O. Connect the cleanup refinement's validated selected-image and successful destination-flush entry contract to the complete production trace.
- [ ] Complete the production copy/retry correspondence beyond the composed successful copy, destination fence and leased-cleanup schedule. Repro: validated exclusive acquisition and exact flush success remain model contracts; digest/path/GUID operations, failed syscalls and complete commit-decision control flow are not refined (V5/V6).

Acceptance: refined evidence/publication properties connected to implementation. Namespace durability and metadata preservation still require V7 qualification.

## V6 — Extend correspondence across recovery control flow

Cause: generated journal framing, record classification, payload/hash slices, truncate-tail selection and replay-source decisions cover selected pure boundaries. Serialized inventory preflight binds exact membership/source positions to saved ranges and ranked replay under explicit payload/effect contracts; generated replacement array scans refine bounded evidence admission. Actual record acquisition, cryptographic identity, full parser/map construction and returned I/O effects remain handwritten and can drift from the model.

- [ ] Derive and replay conformance traces for remaining production transitions, including failed flush/commit, conditional replay markers and generation bootstrap/cleanup.
- [ ] Exercise every relevant transition and rejection class, including repeated crashes and negative mutation controls. Keep local selections focused.
- [ ] Complete the manual refinement argument for the remaining operations in the boundary inventory; review changed production mappings and proof assumptions.
- [ ] Extend the executable boundary beyond inventory-bound replay selection into complete serialized acquisition and preparation/parser composition. Bind full record/checksum/tag/manifest acquisition to the constructed coverage maps, rather than relying on later inventory preflight and exact-payload contracts. Repro: exact replay membership does not prove that handwritten acquisition or hashing constructs the admitted records from the raw journal (V3/V6).

Acceptance: tested correspondence for each precisely named boundary, with the same verification, source freshness, license, target and trimming/NativeAOT obligations as the current kernel. No unchecked external contracts, new shipping dependencies or stronger claims for the remaining handwritten recovery algorithm.

## V7 — Qualify actual storage and coordination guarantees

Cause: E4/F6/I15 remain limited by namespace persistence, bootstrap and device ordering rather than only protocol logic.

- [ ] Define supported OS/filesystem/device combinations and the required persistence/coordination contract for each.
- [ ] Investigate a durably provisioned persistent journal/decision area to reduce per-transaction namespace changes, including initial provisioning, torn slots, reuse, generation rollover, extension metadata and stale decisions.
- [ ] Extend the [managed flush/source audit](design/recovery-storage-contract.md#managed-api-and-operating-system-evidence) to each deployed runtime and establish a supported directory-barrier or provisioning protocol under unchanged runtime constraints. The current implementation has no explicit parent-directory fence; retain the scoped guarantee until qualification evidence closes that gap.
- [ ] Extend hosted VM/block-level or physical interruption testing to journal creation, decisions, truncation, replay, replacement and cleanup; process termination alone is insufficient.
- [ ] Qualify the buffered model's exact bytes-and-length flush fence and crash quiescence against actual OS caches and outstanding I/O; a restarted recovery must not be overwritten by an older queued operation.
- [ ] Qualify lock/receipt lifecycle and missing-name bootstrap, including macOS cooperation. Do not change Access compatibility by requiring native Access to replay a library log.
- [ ] Define a separate caller-stream journal/reopen/exclusive-access contract before promising persistent recovery for arbitrary streams.

Acceptance: E4/F6/I15 evidence for each advertised platform. Never infer hardware durability from a successful flush call or a model that assumes it.

## V8 — Verify bounded parsing and arithmetic kernels

Cause: S1's remaining resource and traversal audit spans many handwritten readers. The generated complete descriptor codec proves bounded field acquisition/serialization, inverse laws, payload-admission priority and row-pointer packing. Shared range/read kernels cover bounded slices and exact positive-read consumption. External-page acquisition, native pointer/token meaning, containing parser traversal and aggregate resource bounds remain separate obligations.

- [ ] Prove parser progress, finite traversal with visited identities, decreasing budgets and aggregate allocation bounds for a selected hostile-file surface.
- [ ] Connect kernels to production paths before claiming a public parser is verified. Retain fuzzing and independent malformed-file tests.

Acceptance: named surface coverage with implementation correspondence; no whole-library hostile-file claim from isolated lemmas.

## V9 — Verify B-tree edits and conservative page reclamation

Cause: I2/I17 need correctness beyond successful round trips and incomplete ownership graphs.

- [ ] Specify ordered multiset semantics, separators, key encoding/comparison assumptions, row identity, page ownership and occupancy for supported native page forms.
- [ ] Prove search, insertion, deletion, split, merge and root changes preserve ordering and membership, then map a small production edit kernel to the proof.
- [ ] Use conservative reachability and potential-reference sets: freed pages must be disjoint from both reachable and potentially referenced pages.
- [ ] Treat opaque/unknown native references as potential ownership; incomplete enumeration cannot justify reclamation.
- [ ] Combine proofs with Microsoft-authored fixtures, independent page inventories and DAO read/compact checks. Retain safe fallback paths until evidence permits removal.

Acceptance: I2/I17 requirements proven for named page forms and independently validated; functionality and native-format completeness remain separate.

## V10 — Verify lifecycle and concurrency boundaries

Cause: the serialized writer lifecycle specification does not establish cross-process acquisition/recovery interleavings, lock ordering or cache pin/lease and copy-on-write correctness. Those require explicit coordination and storage assumptions beyond a single-writer trace.

- [ ] Model cross-process acquisition/recovery/cleanup interleavings; optionally use TLA+/TLC for bounded exploration and TLAPS or Dafny for unbounded invariants.
- [ ] Prove lock-order/rank rules or comparable deadlock prevention for supported cooperating clients, with explicit OS assumptions.
- [ ] Tie pin/lease and copy-on-write invariants to I7 before changing cache reuse.

Acceptance: scoped lifecycle guarantees; finite model checking is reported with its bounds and does not imply arbitrary concurrency correctness.

## V11 — Maintain claims, evidence and trust over time

Cause: future code, verifier or storage changes can invalidate proof claims without changing theorem names.

- [ ] Extend the evidence matrix as implementation and platform coverage grow; link each additional theorem to production conformance and independently qualified storage assumptions.
- [ ] Promote individual README guarantees only after their remaining implementation/platform criteria hold. Keep Partial format/expression/native-compatibility statuses until E1/E2 and feature work establish them.
- [ ] Reassess F* only for a concrete boundary where its extraction/trust tradeoff improves on Dafny under the unchanged-runtime constraint; verification-only F* remains possible without runtime dependencies.

Acceptance: readers can distinguish verified abstract algorithms, tested implementation behavior and qualified physical durability without following commit history.

## Implementation order and independent lanes

1. Refine the buffered journal state machine's remaining page/parser boundaries in V3. The integrator owns shared docs, the todo lists and final CI configuration.
2. Refine replacement evidence in V5 independently where its boundaries permit.
3. Extend V6 beyond the generated value/tag/byte kernel before promoting additional model proofs to implementation claims.
4. Pursue V7 separately; missing OS/hardware evidence must not hold back useful model proofs, and model success must not erase E4/F6/I15.
5. Expand into V8/V9/V10 according to concrete bug risk and existing workstream ownership. Avoid an engine rewrite or competing protocols merely to use a proof tool.
6. Apply V11 whenever a claim changes. Leave incomplete items here with self-contained next steps.

## Primary references

- [PoWER: Preconditions on Writes Enforcing Recoverability (OSDI 2025)](https://www.usenix.org/conference/osdi25/presentation/leblanc).
- [FSCQ and Crash Hoare Logic](https://css.csail.mit.edu/fscq/).
- [Yggdrasil and crash refinement (OSDI 2016)](https://www.usenix.org/conference/osdi16/technical-sessions/presentation/sigurbjarnarson).
- [Dafny reference: compilation, verification and trusted assumptions](https://dafny.org/latest/DafnyRef/DafnyRef.html), [stable 4.11.0 release](https://github.com/dafny-lang/dafny/releases/tag/v4.11.0).
- [F* tutorial and execution/extraction](https://fstar-lang.org/tutorial/book/part1/part1_execution.html).
- [SQLite atomic commit and persistence assumptions](https://sqlite.org/atomiccommit.html).
- [Linux fsync: file and directory persistence](https://man7.org/linux/man-pages/man2/fsync.2.html).
