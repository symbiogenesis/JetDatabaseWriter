# Formal verification and stronger guarantees: open plan

This plan strengthens the guarantees of the existing managed JET/ACE implementation. It adds development and CI tools first; the shipped targets, public runtime requirements and package dependency graph stay unchanged. A proof of an abstract protocol does not verify the existing C# implementation or a filesystem. Keep those evidence levels separate in documentation.

Expand the production Dafny boundary incrementally under the [boundary expansion policy](design/formal-verification.md#boundary-expansion-policy). Prioritize safety-critical behavior with useful contracts and actual production invocation; source volume is not the goal. Keep storage effects and unsupported native semantics explicit while progressively removing handwritten decision logic.

The target is a composed proof foundation for an explicit inventory of critical production behaviors, supported native forms and fault models. Completion requires production to execute verified logic, or a machine-checked refinement of it, throughout that scope, with justified implementation and environmental contracts. Completing selected kernels alone does not reach this endpoint. The plan does not certify maximum possible verification or a globally optimal architecture; V12 establishes the scope and reviews remaining opportunities under the repository's constraints.

Keep only open work in this document. Remove completed requirements in the same commit that delivers them; retain contracts, assumptions, proof coverage and implementation correspondence in `docs/design/formal-verification.md` and proof documentation. This plan complements, rather than replaces, E1/E2/E4, F6, S1, I2/I5/I15/I17 in [the main todo list](todo.md).

## Acceptance policy

- Every theorem names its fault model and assumptions. A conditional theorem must not become an unconditional README claim.
- Preserve pure managed execution on netstandard2.1 and net10.0, unchanged runtime requirements and shipping dependencies throughout the plan. Verifiers and solvers remain development tools.
- Model process failure separately from power loss, media corruption, adversarial namespace edits and caller-stream behavior. Assume neither atomic page writes nor portable directory durability without evidence.
- Safety includes retaining evidence and refusing an unsafe operation. Recovery availability is a separate obligation: under valid evidence, exclusive access and eventually successful I/O, repeated recovery must converge to the authorized image.
- Model checks are bounded evidence; inductive proofs quantify over unbounded model instances. Neither substitutes for implementation correspondence or native interoperability.
- Use generated verified logic or machine-checked refinement for in-scope safety-critical decisions and control flow. Manual correspondence arguments and conformance tests are intermediate evidence; keep their weaker status explicit until the corresponding proof obligation is discharged.
- Compose contracts across acquisition, parsing, authorization, mutation, recovery and cleanup. Each caller must establish its callee's preconditions, including identity, ownership, resource and failure-state obligations; individually proved components do not establish the complete path.
- Keep handwritten effect adapters narrow and reviewable. They perform I/O, locking and other explicitly contracted operations and report actual outcomes to verified control logic. Remove duplicated handwritten policy as verified logic takes over; moving a critical decision into a trusted adapter does not discharge its obligation.
- Review specification adequacy with reachable success and refusal cases, satisfiable preconditions and independent native evidence. An always-refusing implementation or an unreachable success theorem does not meet recovery availability or supported-operation requirements.
- Local verification, Release builds and focused tests are allowed. Full test suites and every benchmark remain hosted-only. Changes to build/test behavior require the ordinary settled-main CI gate and any new proof gate.

## V3 — Refine journal persistence and page correspondence

Cause: the buffered journal protocol constructs aligned multi-page captures and write/truncate preparation with complete original-tail coverage and zero padding beyond the file end. Record-derived minimum/replay controls and content admission compose logical mutations and buffered effects under an explicit zero-exposure contract. Unrestricted pre-marker reordering can retain exact undo evidence while exposing an unrecorded zero gap that the parser must refuse. Serialized acquisition, the zero-exposure storage/protocol boundary and production-loop correspondence still require refinement.

- [ ] Bind the constructed page payloads and capture/preparation schedules to serialized record tags, manifest/page hashes, framing and production record iteration.
- [ ] Compose verified acquisition and preparation with content admission, undo coverage and replay. Prove that the records actually admitted establish the byte, length and payload premises used by mutation and recovery; retain cryptographic primitive and storage effects as explicit contracts.
- [ ] Establish production/storage correspondence for the buffered content refinement's zero-exposure contract, or extend the protocol to safely admit pre-marker survivors where an old write or length effect reextends a truncated image. Repro: truncation persists before an older write and the resulting zero gap has no durable gap intent or replay marker; exact undo coverage alone cannot authorize parser admission (V3/V7).

Acceptance: compose the checked buffered protocol with executable page preparation and parser admission under explicit cryptographic and storage contracts. Carry its safety and progress obligations into V6's production controller; abstract progress alone does not establish C# loop refinement.

## V5 — Refine replacement evidence and physical publication

Cause: bounded header/decision array scans, exact-read cursor decisions and length/admission kernels refine the replacement byte classifier, with production conformance for field corruption and admission before bootstrap. Immutable-copy validation, complete generation authority, failed I/O and physical publication still require refinement.

- [ ] Extend complete `Publish`/`Recover` callback interruption traces to partial failed native syscall survivors and their persistence contracts. Repro: exceptions before/after real publication reads, writes and both flush fences retain selection, but do not determine which bytes or namespace effects a failed syscall persists (V5/V7).
- [ ] Extend production correspondence beyond the selected bootstrap and production-order cleanup traces, including receipt identity, interrupted acquisition/publication and failed bootstrap I/O. Connect the cleanup refinement's validated selected-image and successful destination-flush entry contract to the complete production trace.
- [ ] Complete the production copy/retry correspondence beyond the composed successful copy, destination fence and leased-cleanup schedule. Repro: validated exclusive acquisition and exact flush success remain model contracts; digest/path/GUID operations, failed syscalls and complete commit-decision control flow are not refined (V5/V6).
- [ ] Compose byte admission, immutable-copy identity, generation authority, publication fences, commit acknowledgement and cleanup authorization in verified executable decisions or machine-checked production refinement. Pass partial and failed effect outcomes back through that controller without granting authority from valid record bytes alone (V5/V6/V10).

Acceptance: composed evidence/publication properties govern the in-scope production decision path, including failures and retries. Namespace durability and metadata preservation still require V7 qualification.

## V6 — Extend correspondence across recovery control flow

Cause: generated journal framing, record classification, payload/hash slices, truncate-tail selection and replay-source decisions cover selected pure boundaries. Serialized inventory preflight binds exact membership/source positions to saved ranges and ranked replay under explicit payload/effect contracts; generated replacement array scans refine bounded evidence admission. Actual record acquisition, cryptographic identity, full parser/map construction and returned I/O effects remain handwritten and can drift from the model.

- [ ] Derive and replay conformance traces for remaining production transitions, including failed flush/commit, conditional replay markers and generation bootstrap/cleanup.
- [ ] Exercise every relevant transition and rejection class, including repeated crashes and negative mutation controls. Keep local selections focused.
- [ ] Complete manual refinement arguments and focused conformance for remaining boundary operations as intermediate evidence. Identify each remaining machine-checked obligation and each explicitly trusted effect; review changed production mappings and proof assumptions.
- [ ] Extend the executable boundary beyond inventory-bound replay selection into complete serialized acquisition and preparation/parser composition. Bind full record/checksum/tag/manifest acquisition to the constructed coverage maps, rather than relying on later inventory preflight and exact-payload contracts. Repro: exact replay membership does not prove that handwritten acquisition or hashing constructs the admitted records from the raw journal (V3/V6).
- [ ] Move complete in-scope journal and replacement decision flows into verified executable controllers, or supply machine-checked production refinement. Cover preparation, admission/map construction, conditional replay markers, failed flush/commit, uncertain outcomes, commit acknowledgement, bootstrap and cleanup. Prove that every allowed reported effect outcome preserves the controller invariant and that eventually successful exclusive I/O permits production completion.
- [ ] Replace duplicated handwritten decisions with calls to the verified implementation. Inventory and review the remaining adapters, including exception/cancellation translation, partial-read/write reporting, resource ownership and state retained between effects; connect each to the controller's contract without unchecked external declarations.

Acceptance: machine-checked composition and direct production use, or machine-checked refinement, cover each in-scope recovery decision flow. Manual arguments and tested traces remain supporting evidence. Retain the current verification, source freshness, license, target and trimming/NativeAOT obligations, explicit effect/crypto contracts and V7 qualification; no unchecked external contracts or new shipping dependencies.

## V7 — Qualify actual storage and coordination guarantees

Cause: E4/F6/I15 remain limited by namespace persistence, bootstrap and device ordering rather than only protocol logic.

- [ ] Define supported OS/filesystem/device combinations and the required persistence/coordination contract for each.
- [ ] Investigate a durably provisioned persistent journal/decision area to reduce per-transaction namespace changes, including initial provisioning, torn slots, reuse, generation rollover, extension metadata and stale decisions.
- [ ] Extend the [managed flush/source audit](design/recovery-storage-contract.md#managed-api-and-operating-system-evidence) to each deployed runtime and establish a supported directory-barrier or provisioning protocol under unchanged runtime constraints. The current implementation has no explicit parent-directory fence; retain the scoped guarantee until qualification evidence closes that gap.
- [ ] Extend hosted VM/block-level or physical interruption testing to journal creation, decisions, truncation, replay, replacement and cleanup; process termination alone is insufficient.
- [ ] Qualify the buffered model's exact bytes-and-length flush fence and crash quiescence against actual OS caches and outstanding I/O; a restarted recovery must not be overwritten by an older queued operation.
- [ ] Qualify lock/receipt lifecycle and missing-name bootstrap, including macOS cooperation. Do not change Access compatibility by requiring native Access to replay a library log.

Acceptance: E4/F6/I15 evidence for each advertised platform. Never infer hardware durability from a successful flush call or a model that assumes it.

## V8 — Verify bounded parsing and arithmetic kernels

Cause: S1's remaining resource and traversal audit spans many handwritten readers. The descriptor codec and shared range/read kernels cover their named bounded representation and acquisition decisions. The selected external chained-payload traversal proves distinct visits, decreasing row/payload budgets and aggregate traversal-owned array bounds, with production invocation and focused conformance. Exact handwritten table membership, containing public parser traversal, locator/cache/pool and destination resources, native pointer/token meaning and other reader surfaces remain separate obligations.

- [ ] Extend the chained-payload traversal proof through exact table membership and its containing public parser, including locator/cache/pool and destination resource bounds. Compose acquisition, visited identities and decreasing budgets across that complete named surface (V8/V12).
- [ ] Connect kernels to production paths before claiming a public parser is verified. Retain fuzzing and independent malformed-file tests.
- [ ] Extend the composed acquisition/codec/traversal/resource proofs across the remaining in-scope parser surfaces and native forms. Record unsupported semantics and remaining surfaces individually; an isolated traversal kernel does not close V8 (V8/V12).

Acceptance: composed proofs and production use cover the parser surfaces and properties enumerated in V12, including aggregate resource consumption. Claims name those surfaces; isolated bounds lemmas do not establish a public parser or whole-library hostile-file guarantee.

## V9 — Verify B-tree edits and conservative page reclamation

Cause: I2/I17 need correctness beyond successful round trips and incomplete ownership graphs.

- [ ] Specify ordered multiset semantics, separators, key encoding/comparison assumptions, row identity, page ownership and occupancy for supported native page forms.
- [ ] Prove search, insertion, deletion, split, merge and root changes preserve ordering and membership, starting with a small verified production edit kernel. Compose key comparison/encoding, page codecs, allocation and mutation planning through the containing edit path under explicit storage contracts.
- [ ] Use conservative reachability and potential-reference sets: freed pages must be disjoint from both reachable and potentially referenced pages.
- [ ] Treat opaque/unknown native references as potential ownership; incomplete enumeration cannot justify reclamation.
- [ ] Combine proofs with Microsoft-authored fixtures, independent page inventories and DAO read/compact checks. Retain safe fallback paths until evidence permits removal.
- [ ] Extend the production proofs to every in-scope edit, reclamation path and native page form in V12's inventory. Treat retained fallbacks as separate paths with their own obligations; mapping the initial kernel alone does not close V9.

Acceptance: I2/I17 requirements proven for named page forms and independently validated; functionality and native-format completeness remain separate.

## V10 — Verify lifecycle and concurrency boundaries

Cause: the serialized writer lifecycle and multi-client acquisition/recovery/cleanup models preserve their named invariants under explicit successful effects and canonical coordination. The multi-client model covers arbitrary finite interleavings and stale-ticket refusal; actual composite coordination acquisition, OS exclusion, lock ordering and cache pin/lease and copy-on-write correctness still require implementation and storage correspondence.

- [ ] Refine the multi-client model's atomic canonical coordinator to complete existing/missing-destination acquisition and ordinary held-handle disposal. Repro: replacement recovery can bootstrap before a database handle exists, while ordinary owner disposal can run after it closes; neither is mechanically refined by the retained-database cleanup schedule. Bind composite intent/native/source leases, stale attempts and crash quiescence to production traces (V10/V7).
- [ ] Prove lock-order/rank rules or comparable deadlock prevention for supported cooperating clients, with explicit OS assumptions.
- [ ] Tie pin/lease and copy-on-write invariants to I7 before changing cache reuse.
- [ ] Connect acquisition, recovery, cleanup, transaction/snapshot disposal and cache ownership transitions to verified production logic or machine-checked refinement. Compose lifecycle authority with V3/V5/V6; enumerate the cooperating clients, execution interleavings and fairness assumptions covered by each safety or progress claim (V10/V12).

Acceptance: composed lifecycle and recovery guarantees cover the inventoried production transitions under named OS and scheduling contracts. Use inductive proofs for claims over unbounded executions; report finite exploration with its bounds and separate safety, deadlock freedom and eventual progress.

## V11 — Maintain claims, evidence and trust over time

Cause: future code, verifier or storage changes can invalidate proof claims without changing theorem names.

- [ ] Extend the evidence matrix as implementation and platform coverage grow; link each additional theorem to production conformance and independently qualified storage assumptions.
- [ ] Promote individual README guarantees only after their remaining implementation/platform criteria hold. Keep Partial format/expression/native-compatibility statuses until E1/E2 and feature work establish them.
- [ ] Reassess F* only for a concrete boundary where its extraction/trust tradeoff improves on Dafny under the unchanged-runtime constraint; verification-only F* remains possible without runtime dependencies.
- [ ] Establish continuing specification, coverage and trust review triggers for mapped-code, compiler/verifier, runtime, native-format and storage-contract changes. Record the maintenance policy in durable design/proof documentation and remove this setup item once the policy is in place; ongoing maintenance is not a permanently open completion checkbox.

Acceptance: readers can distinguish verified abstract algorithms, tested implementation behavior and qualified physical durability without following commit history.

## V12 — Establish scope and close the proof architecture

Cause: selected parsers, small edit kernels and manual correspondence can complete individual increments while leaving critical production decisions outside the proofs. The current plan has no complete scope inventory or final composition/architecture review against which to judge that remainder.

- [ ] Inventory critical production behaviors across journal preparation/recovery, replacement, generation ownership, parser traversal/resource limits, structural edits/reclamation and transaction/cache lifecycle. Record entry points, supported native forms, required safety and progress properties, fault/coordination models and platform scope. Include alternate, fallback, failure and retry paths; classify other library surfaces with a concrete reason and risk-based priority.
- [ ] For each behavior, record its specification, proof and production implementation, dependent contracts, conformance/native evidence and remaining gap. Distinguish executable verification, abstract-only proof, manual correspondence, testing and platform qualification. Define the scope before treating a selected increment as completion; exclusions narrow the resulting claim and do not remove independent compatibility or safety requirements from the main todo list.
- [ ] Review specification adequacy with independent success/refusal examples and negative mutation controls. Check reachable initial states, satisfiable preconditions, meaningful supported operations, evidence retention, repeated interruptions and separate progress under eventually successful effects. Prevent stronger preconditions or broad refusal from silently removing required behavior.
- [ ] Discharge every in-scope critical decision and composition obligation through verified executable production code or machine-checked refinement. Review the complete paths, including adapter sequencing and outcomes; resolve each remaining gap or explicitly revise the scope and resulting claim with its reason and follow-up. Preserve outstanding implementation/platform obligations as open work.
- [ ] Review the resulting architecture for duplicated policy, unnecessary representations/protocols and avoidable trust. Require a traceable proof-to-production path and narrow effect adapters; simplify where that removes a concrete correctness or maintenance burden. Record the verifier/solver, compiler/projection boundary, .NET, cryptographic and host assumptions in a residual-trust inventory, with evidence and actionable further reductions or a documented reason to retain each. Verifier soundness and specification adequacy remain explicit trust obligations.
- [ ] Record the final coverage, composition argument, exclusions, residual trust and continuing review policy in durable design/proof documentation. Complete this plan only when its in-scope obligations and advertised platform criteria are met; retain any unresolved work as self-contained items. Describe the achieved scope precisely without a maximum-verification or optimal-architecture claim.

Acceptance: an auditable inventory connects each in-scope critical behavior to composed proofs and executing production logic, justified adapters and qualified advertised environmental assumptions. Coverage and trust reviews explain remaining opportunities under the unchanged-runtime constraints. Proof volume or a completed initial kernel is insufficient evidence of closure.

## Implementation order and independent lanes

1. Establish V12's scope inventory, required properties and specification review first; keep it current as new paths or native forms are discovered. V labels identify work, not execution order. The integrator owns shared docs, the todo lists and final CI configuration.
2. Refine the buffered journal state machine's remaining page/parser boundaries in V3 and replacement evidence in V5 independently where their boundaries permit.
3. Use V6 to compose those proofs through complete verified production decision flows. Keep manual correspondence and conformance as intermediate milestones before promoting implementation claims.
4. Pursue V7 separately; missing OS/hardware evidence must not hold back useful model proofs, and model success must not erase E4/F6/I15.
5. Expand into V8/V9/V10 according to concrete bug risk and the scope inventory. Start with small production increments, then close the remaining inventoried obligations. Avoid an engine rewrite or competing protocols merely to use a proof tool.
6. Apply V11 whenever a claim changes and establish its continuing review policy. Finish with V12's composition, architecture and residual-trust review. Leave incomplete items here with self-contained next steps.

## Primary references

- [PoWER: Preconditions on Writes Enforcing Recoverability (OSDI 2025)](https://www.usenix.org/conference/osdi25/presentation/leblanc).
- [FSCQ and Crash Hoare Logic](https://css.csail.mit.edu/fscq/).
- [Yggdrasil and crash refinement (OSDI 2016)](https://www.usenix.org/conference/osdi16/technical-sessions/presentation/sigurbjarnarson).
- [Dafny reference: compilation, verification and trusted assumptions](https://dafny.org/latest/DafnyRef/DafnyRef.html), [stable 4.11.0 release](https://github.com/dafny-lang/dafny/releases/tag/v4.11.0).
- [F* tutorial and execution/extraction](https://fstar-lang.org/tutorial/book/part1/part1_execution.html).
- [SQLite atomic commit and persistence assumptions](https://sqlite.org/atomiccommit.html).
- [Linux fsync: file and directory persistence](https://man7.org/linux/man-pages/man2/fsync.2.html).
