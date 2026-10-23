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

## V1 — Specify the storage contract and trusted boundary

Cause: recovery behavior is documented, but no single proof-oriented contract distinguishes abstract protocol safety, implementation behavior and OS persistence.

- [ ] Define images, durable versus pending bytes, original length, generation, exclusive authority, validated evidence, successful commit acknowledgement and conservative refusal.
- [ ] State before-or-committed recovery safety and the stronger acknowledged-commit property, including failed/uncertain commit outcomes.
- [ ] Specify repeated recovery, truncation/re-extension, torn writes, partial versus invalid records, stale decisions and cleanup ownership.
- [ ] Inventory trusted assumptions: file flush semantics, journal discovery/namespace durability, ordering, hashing/collision assumptions, trusted directories, path identity, locks, runtime/verifier/compiler/solver and C# correspondence.
- [ ] Map each modeled transition to production methods and focused tests. Record uncovered parser, encoding, I/O and concurrency behavior explicitly.
- [ ] Correct inconsistent operational guidance when it disagrees with the recovery implementation.

Acceptance: a reviewed design contract with named assumptions, explicit exclusions and source/test correspondence. No broader power-loss or implementation-verification claim.

## V2 — Establish reproducible verification with no runtime dependencies

Cause: there is no pinned verifier, proof manifest, trust audit or required proof job.

- [ ] Pin a stable Dafny release and its distributed solver with integrity-checked platform downloads; record tool provenance and the update procedure.
- [ ] Add a fail-closed PowerShell verification entry point with bounded subprocess execution, explicit absolute temporary working/output paths and preserved system environment.
- [ ] Verify every declared proof file, reject undeclared/omitted proof files and reject disabled verification, assumptions, axioms, unchecked external contracts or admitted lemmas.
- [ ] Run Dafny's trust audit and fail on findings; successful process exit alone is insufficient. Keep logs and machine-readable coverage where supported.
- [ ] Add focused script regressions for missing/mismatched tools, proof failures, audit findings and omitted files. Negative controls must show that a false theorem and an introduced assumption are rejected.
- [ ] Add a required ordinary-main CI proof job and preserve proof diagnostics. Document local invocation without changing solution/package references.

Acceptance: clean verification and trust audit from a pinned toolchain, demonstrated fail-closed negative controls and successful hosted proof gate for the settled main revision.

## V3 — Prove the undo protocol's write preconditions

Cause: targeted fault tests do not quantify over every model state, arbitrary image size or arbitrary repeated failures.

- [ ] Build a small Dafny model of the current undo protocol using preconditions on writes, following PoWER/crash-refinement principles.
- [ ] Prove an original before-image is durable before any original byte can be overwritten; untouched original bytes retain their original values.
- [ ] Prove every truncation protects the complete removed original tail, including a partial last page; append-only bytes need no old image.
- [ ] Admit torn and reordered unflushed data writes within the declared storage model, without silently assuming atomic pages.
- [ ] Prove a flushed commit decision follows a fully flushed committed image, and that uncertain commit outcomes cannot select contradictory rollback.
- [ ] State each abstraction deliberately; model checksums as validated evidence under an explicit integrity assumption, not mathematical collision freedom.

Acceptance: mechanically checked inductive invariants over arbitrary finite images, with substantive transitions and explicit correspondence gaps. A tautological recovery selector is insufficient.

## V4 — Prove restartable recovery and generation cleanup

Cause: recovery itself can crash, and stale owners must never delete successor evidence.

- [ ] Prove restoration preserves recoverability after every partial write, repeated crash and retry.
- [ ] Model truncation followed by zero-filled re-extension and the recovery-start authorization needed to accept it.
- [ ] Prove complete valid decisions choose exactly one image and invalid complete evidence refuses without mutation.
- [ ] Prove cleanup requires matching generation and that repeated disposal cannot delete a successor generation's journal/receipt.
- [ ] Separate safety from termination; prove progress only with finite work and eventually successful I/O, without assuming crashes stop as a safety precondition.

Acceptance: verified recovery/ownership lemmas and a documented mapping to actual validation, replay and cleanup paths.

## V5 — Prove replacement decision and publication rules

Cause: encryption replacement has a separate old/new-image protocol and bootstrap boundary.

- [ ] Model Prepared, publication, Committed, interrupted recovery and cleanup using immutable validated old/new copies and generation-bound authority.
- [ ] Prove prepared-only evidence authorizes old, a complete valid commit authorizes new, and partial/invalid decisions follow the documented selection/refusal policy.
- [ ] Prove publication and retries preserve an authorized complete copy; mismatched generation, conflicting bytes and insufficient bootstrap authority refuse safely.
- [ ] Include missing destinations, partial publication prefixes and stale cleanup; document namespace durability and metadata preservation as separate obligations.

Acceptance: checked selection/preservation/cleanup properties; no claim that file-content flushing makes rename durable.

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

- [ ] Publish a compact evidence matrix: property, formal model/theorem, production boundary, conformance tests, platform evidence, trusted assumptions and remaining gap.
- [ ] Update README claims individually only when their corresponding acceptance criteria hold. Keep Partial format/expression/native-compatibility statuses until E1/E2 and feature work establish them.
- [ ] Add proof/trust review to changes in mapped storage code and pin updates. Preserve counterexamples and failed proof diagnostics as useful CI evidence.
- [ ] Reassess F* only for a concrete boundary where its extraction/trust tradeoff improves on Dafny under the unchanged-runtime constraint; verification-only F* remains possible without runtime dependencies.

Acceptance: readers can distinguish verified abstract algorithms, tested implementation behavior and qualified physical durability without following commit history.

## Implementation order and independent lanes

1. Establish V1/V2 and an initial V3/V4 pilot in parallel after this plan is committed. The integrator owns shared docs, the todo lists and final CI configuration.
2. Verify and review the pilot, add V5 where independent, then land reproducible proof CI and the scope/assumption matrix.
3. Close V6 for a small boundary before promoting a model proof to an implementation claim.
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
