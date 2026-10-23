# Formal verification and stronger guarantees: open plan

This plan strengthens the guarantees of the existing managed JET/ACE implementation. It adds development and CI tools first; the shipped targets, public runtime requirements and package dependency graph stay unchanged. A proof of an abstract protocol does not verify the existing C# implementation or a filesystem. Keep those evidence levels separate in documentation.

Keep only open work in this document. Remove completed requirements in the same commit that delivers them; retain contracts, assumptions, proof coverage and implementation correspondence in `docs/design/formal-verification.md` and proof documentation. This plan complements, rather than replaces, E1/E2/E4, F6, S1, I2/I5/I15/I17 in [the main todo list](todo.md).

## Acceptance policy

- Every theorem names its fault model and assumptions. A conditional theorem must not become an unconditional README claim.
- Preserve pure managed execution on netstandard2.1 and net10.0. No shipping verifier, solver, native component, external service or new package dependency is required by the initial work.
- Model process failure separately from power loss, media corruption, adversarial namespace edits and caller-stream behavior. Assume neither atomic page writes nor portable directory durability without evidence.
- Safety includes retaining evidence and refusing an unsafe operation. Recovery availability is a separate obligation: under valid evidence, exclusive access and eventually successful I/O, repeated recovery must converge to the authorized image.
- Model checks are bounded evidence; inductive proofs quantify over unbounded model instances. Neither substitutes for implementation correspondence or native interoperability.
- Local verification, Release builds and focused tests are allowed. Full test suites and every benchmark remain hosted-only. Changes to build/test behavior require the ordinary settled-main CI gate and any new proof gate.

## V3 — Compose byte preservation with durable journal decisions

Cause: exact durable captures, validated decisions and authorized mutations are premises of the byte model; the commit submodel is separate. Their composition with pending journal writes is not yet proved.

- [ ] Model pending/durable journal appends, write intents, failed flush outcomes and record boundaries in one transaction state machine.
- [ ] Derive pre-write enablement from journal state rather than supplying exact durable captures as a premise; include full-tail truncation and partial final pages.
- [ ] Compose transaction mutation, commit decision and restartable replay into an end-to-end before-or-committed theorem over arbitrary finite mixed traces.
- [ ] Admit reordered unflushed writes under an explicit storage model and preserve safety for every allowed crash outcome.
- [ ] Prove a terminating undo replay schedule and its progress measure; exact reconstruction as a sequence expression alone does not prove the production replay loop.

Acceptance: a checked composed protocol with explicit storage assumptions, without promoting abstract atomic decision classification to a verified byte parser.

## V5 — Refine replacement evidence and physical publication

Cause: the replacement and ownership models start from validated logical evidence and exclusive generation authority. They do not establish these premises from persisted record bytes or actual copy/flush behavior.

- [ ] Prove bounded byte-level header/decision classification, distinguishing incomplete records from invalid complete records and refusing insufficient bootstrap authority.
- [ ] Refine immutable-copy availability and prefix staging into bounded copy, publication and flush outcomes, including exceptions and missing destinations.
- [ ] Compose replacement selection with generation acquisition and cleanup, including failed receipt preparation and interrupted authority bootstrap.
- [ ] Preserve the distinction between an abstract successful recovery path and the actual production algorithm; integrate its copy/retry progress argument.

Acceptance: refined evidence/publication properties connected to implementation. Namespace durability and metadata preservation still require V7 qualification.

## V6 — Connect proofs to executable C#

Cause: a model and its handwritten implementation can drift independently.

- [ ] Derive focused conformance traces from the model for write/flush/truncate/commit/recovery/generation boundaries and replay against production helpers.
- [ ] Exercise every relevant transition and rejection class, including repeated crashes and negative mutation controls. Keep local selections focused.
- [ ] Inventory the manual refinement argument and unproved operations; require review when mapped methods or proof assumptions change.
- [ ] Choose a small pure executable kernel, such as decision classification or checked range arithmetic, only after the model and trace boundary are useful.
- [ ] Compare handwritten and Dafny-generated C# options. If generation is adopted, bundle only reviewed source/runtime support, check freshness, licenses, target compatibility, trimming and NativeAOT, and prove the generated kernel is actually invoked.
- [ ] Do not treat Dafny `extern` contracts as verification of existing C#. Retain a model-only claim until executable correspondence is established.

Acceptance: tested correspondence for a precisely named boundary; generated code is optional and must not change shipping dependencies or supported runtime requirements.

## V7 — Qualify actual storage and coordination guarantees

Cause: E4/F6/I15 remain limited by namespace persistence, bootstrap and device ordering rather than only protocol logic.

- [ ] Define supported OS/filesystem/device combinations and the required persistence/coordination contract for each.
- [ ] Investigate a durably provisioned persistent journal/decision area to reduce per-transaction namespace changes, including initial provisioning, torn slots, reuse, generation rollover, extension metadata and stale decisions.
- [ ] Compare managed directory-barrier capabilities against documented runtime/OS contracts. If unchanged runtime constraints prevent a portable barrier, retain a scoped guarantee and document the unresolved requirement.
- [ ] Extend hosted VM/block-level or physical interruption testing to journal creation, decisions, truncation, replay, replacement and cleanup; process termination alone is insufficient.
- [ ] Qualify lock/receipt lifecycle and missing-name bootstrap, including macOS cooperation. Do not change Access compatibility by requiring native Access to replay a library log.
- [ ] Define a separate caller-stream journal/reopen/exclusive-access contract before promising persistent recovery for arbitrary streams.

Acceptance: E4/F6/I15 evidence for each advertised platform. Never infer hardware durability from a successful flush call or a model that assumes it.

## V8 — Verify bounded parsing and arithmetic kernels

Cause: S1's remaining resource and traversal audit spans many handwritten readers.

- [ ] Identify shared range/offset/length primitives and prove overflow-safe slicing, exact reads and monotone consumption for both target frameworks.
- [ ] Prove parser progress, finite traversal with visited identities, decreasing budgets and aggregate allocation bounds for a selected hostile-file surface.
- [ ] Add codec round-trip/inverse proofs for defined representations, with rejection of malformed encodings and explicit native-format assumptions.
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

Cause: exclusive ownership, lock ordering, cache generations and nested undo are outside a single-threaded storage model.

- [ ] Specify legal writer/transaction/lease lifecycles, disposal idempotence, generation invalidation and nested savepoint semantics.
- [ ] Model cross-process acquisition/recovery/cleanup interleavings; optionally use TLA+/TLC for bounded exploration and TLAPS or Dafny for unbounded invariants.
- [ ] Prove lock-order/rank rules or comparable deadlock prevention for supported cooperating clients, with explicit OS assumptions.
- [ ] Tie pin/lease and copy-on-write invariants to I7 before changing cache reuse.

Acceptance: scoped lifecycle guarantees; finite model checking is reported with its bounds and does not imply arbitrary concurrency correctness.

## V11 — Maintain claims, evidence and trust over time

Cause: future code, verifier or storage changes can invalidate proof claims without changing theorem names.

- [ ] Extend the evidence matrix as implementation and platform coverage grow; link each additional theorem to production conformance and independently qualified storage assumptions.
- [ ] Promote individual README guarantees only after their remaining implementation/platform criteria hold. Keep Partial format/expression/native-compatibility statuses until E1/E2 and feature work establish them.
- [ ] Add proof/trust review to changes in mapped storage code and pin updates. Preserve counterexamples and failed proof diagnostics as useful CI evidence.
- [ ] Reassess F* only for a concrete boundary where its extraction/trust tradeoff improves on Dafny under the unchanged-runtime constraint; verification-only F* remains possible without runtime dependencies.

Acceptance: readers can distinguish verified abstract algorithms, tested implementation behavior and qualified physical durability without following commit history.

## Implementation order and independent lanes

1. Compose the pending/durable journal state machine in V3. The integrator owns shared docs, the todo lists and final CI configuration.
2. Refine replacement evidence in V5 independently where its boundaries permit.
3. Close V6 for a small production boundary before promoting a model proof to an implementation claim.
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
