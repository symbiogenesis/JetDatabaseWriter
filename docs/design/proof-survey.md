# Proof and algorithm survey

Survey date: 2026-10-30. Supplemental storage/resource review: 2026-10-31.

This document connects mathematics, algorithms and proof-engineering
research to JetDatabaseWriter's performance, integrity, allocations and
maintainability. It records applicability decisions and their limits. The
[root plan](../../PLAN.md) owns outstanding implementation and acceptance work;
this document is not a second backlog.

## Scope and evidence

The original survey screens OpenAI's mathematics catalog, examines selected relevant
theorem statements and formalization scopes, and includes independent 2026
algorithm and software-verification research. The supplemental review includes
foundational storage, resource, parser and data-structure proofs from earlier
years, alongside relevant 2025/2026 work. It is an applicability review,
not an independent verification of every manuscript or a reproduction of the
papers' experiments.

The [OpenAI collection](https://github.com/openai/math) reports 721 manuscripts
in 373 families and approximately 42% of top-level results formalized. Its
authors describe the collection as raw research data at differing verification
stages. The [release history](https://github.com/openai/math/blob/main/history.md)
records withdrawals, repairs and formalization changes. Counts and status here
describe the material read on the survey date; check individual revisions
before relying on a result.

Catalog screening means reviewing the family descriptions for a matching
computational problem, then examining selected papers and formalization scopes;
it does not mean checking all 721 manuscripts. This literature review did not
rerun the upstream Lean, Rocq, Isabelle or F* artifacts. Publisher-reported verification,
independent examination and this repository's own CI evidence remain separate.

Distinguish these forms of evidence:

- A manuscript's theorem and hypotheses, including its computational model.
- A checked formal statement, which can have a different scope from the paper.
- Experimental evidence for a particular implementation and workload.
- Correspondence to this library's actual managed implementation, native page
  forms, resources and storage contracts.

An asymptotic theorem does not establish a useful practical crossover, a .NET
allocation reduction or Microsoft Access compatibility. A proof-generation
benchmark does not establish the correctness of a generated repository proof.
The [formal boundary](formal-verification.md), [scope inventory](verification-scope.md)
and [evidence map](verification-evidence-map.md) define the library's claims.

The current applicability decisions, in order of practical relevance, are:

| Opportunity | Expected benefit and next evidence |
|---|---|
| Recoverability at every write and across recovery layers | PoWER, CHL and Argosy supply complementary proof patterns for V3/V5/V6. Compose actual admitted bytes, returned effects and repeated recovery; retain V7/I15 storage qualification. |
| Transaction and concurrent implementation refinement | DaisyNFS, Perennial and ReLoC inform I5/V6/V10's remaining state, authority and CAS/lock correspondence. They do not verify the current C# controller. |
| Explicit peak-space and work budgets | IrisFit and time-credit proofs inform I7/S1/V8/V10: account for suspended owners, pool retention, growth overlap and adversarial probes before removing copies or tightening limits. |
| Native tree and relocation semantics | Verified B+ trees and CertiGC inform I2/I17/V9: tree descent must agree with leaf iteration, and relocation must preserve all rooted references before reclamation. |
| Rounded numeric seek bounds | Flocq supplies relevant rounding lemmas for I9, conditional on matching native key and CLR comparison semantics and proving no false negatives. |
| Reuse ordered runs before general stable sorting | Existing splice measurements establish a named allocation benefit. I12 still owns public timing acceptance; a full sorting implementation needs a remaining measured bottleneck. |
| Compose visited-set orchestration with the containing parser | Generated count/growth decisions retain exact membership and the single-scan path under V8/S1. Compose allocation effects, adversarial probe work, growth overlap and cancellation; exact membership alone is insufficient. |
| Fixed-contract proof repair and dependency review | Less proof-maintenance work under V6/V12, while preserving supported behavior and independently reviewed specifications. A benchmark solve rate is not repository acceptance. |
| Compact metadata and cache accounting | Possible allocation reductions under I7/I17/S1. Compare existing disk ledgers, ranges and bitmaps first; account for in-flight and leased storage as well as retained entries. |
| Proactive B-tree splitting | A later I2/V9 experiment after native byte-capacity and ownership invariants, incremental edits and workload evidence exist. |
| SAT, matrix arithmetic and unrelated pure mathematics | No matching production bottleneck or demonstrated drop-in improvement. Keep their theorem statements separate from practical performance claims. |

## Results with concrete repository applications

### Recoverability preconditions on storage effects

[PoWER Never Corrupts](https://www.usenix.org/system/files/osdi25-leblanc.pdf),
OSDI July 2025, puts a quantified precondition on durable writes: every newly
possible partial-persistence state must remain recoverable. Its machine-checked
correspondence to crash conditions supports using ordinary ghost state and
quantifiers in Dafny. This paper is already a root-plan reference; its concrete
application deserves inclusion here.

The existing [undo model](../../verification/Pages/Paging/UndoJournal.dfy)
already follows this discipline. V3/V5/V6 should carry it through actual
preparation, serialized admission and returned-effect interfaces in
[PersistentRollbackJournal](../../src/JetDatabaseWriter/Pages/Paging/PersistentRollbackJournal.cs)
and [EncryptionReplacementIntent](../../src/JetDatabaseWriter/Encryption/EncryptionReplacementIntent.cs).
Prove preservation of recovery authorization and acknowledged decisions for
every admitted outcome; retain separate progress obligations. A declared
effect contract still needs implementation correspondence.

The evaluated systems target persistent memory with aligned eight-byte atomic
persistence. Their corruption-detecting Boolean cannot be copied into a
torn-byte stream contract. Their CRC reasoning assumes bounded corrupted bits
and a trusted checksum property. It supplies no arbitrary-corruption or
adversarial-integrity guarantee; data/checksum generations still need protocol
correspondence. Neither this result nor a valid digest qualifies file/name
durability. Retain V7/I15 and the existing byte-level fault model.

### Recovery invariants and composition across layers

[FSCQ and Crash Hoare Logic](https://adam.chlipala.net/papers/FscqSOSP15/FscqSOSP15.pdf),
SOSP October 2015, distinguish normal postconditions, crash conditions and
recovery specifications. Recovery preserves the condition needed to restart
it after another interruption. This is useful specification structure for
V3/V5/V6, already recognized by the root plan. The original atomic-block disk
assumption and unverified Haskell/FUSE environment do not establish this
library's torn-write or managed-I/O contracts.

[Argosy](https://pdos.csail.mit.edu/papers/argosy:pldi19.pdf), PLDI June 2019,
proves in Coq that recovery refinements compose, including active recovery
that writes storage and can crash repeatedly. Its example layers a write-ahead
log over replicated disks. Crashing during upper-layer recovery can require
lower-layer recovery to run again; independent component proofs alone do not
justify that sequence.

For V3/V5/V6/V10, connect serialized acquisition, journal replay, replacement
publication and generation cleanup with explicit abstraction relations and
matching initial states. Include recovery before any new operation and
interruptions during recovery itself. Keep selected bytes, lengths, decision
identity and retained evidence related across layers. The composition theorem
is conditional on primitive/crash semantics, not an OS durability proof or a
termination guarantee under endless failures. Transfer these obligations into
the existing Dafny boundary rather than introducing another recovery engine.

### Transaction refinement and post-crash authority

[DaisyNFS](https://www.usenix.org/system/files/osdi22-chajed.pdf), OSDI July
2022, combines sequential Dafny operation proofs with a Coq-proved transaction
layer to derive concurrent, crash-safe refinement. Its transaction bodies
access shared mutable state through that layer. The paper retains trusted
cross-language specification correspondence and primitive contracts.

This directly informs [MutationExecutor](../../src/JetDatabaseWriter/Tables/MutationExecutor.cs),
[TransactionLifecycle](../../src/JetDatabaseWriter/Transactions/TransactionLifecycle.cs)
and [PagerTransaction](../../src/JetDatabaseWriter/Pages/Paging/PagerTransaction.cs)
under I5/V6/V9/V10. Page rollback alone does not cover catalog generations,
constraint state, counters, allocator hints or callbacks. Include those
effects in the refinement or prove their restoration separately. DaisyNFS's
allocator illustrates a useful distinction: an in-memory hint proposes a
candidate, while transactional allocation metadata authorizes its use.

[Perennial](https://www.chajed.io/papers/perennial:sosp2019.pdf), SOSP October
2019, uses versioned volatile resources, recovery leases and recovery helping
to reason about concurrent crash safety. These are useful patterns for
generation-bound cleanup and uncertain replacement completion under V5/V6/V10.
They do not establish an OS lock or permission from a pathname alone. The
original work proves safety, not liveness, and its Goose filesystem model
excludes deferred durability. Its [current upstream repository](https://github.com/mit-pdos/perennial)
has changed proof languages and reports no crash program logic on its default
Lean branch; pin a matching historical artifact before attempting reuse.

### Persistence models and failed flushes

[Ferrite](https://syslab.cs.washington.edu/papers/ferrite-asplos16.pdf), ASPLOS
April 2016, separates allowed crash outcomes from ordinary syscall semantics
using axiomatic/operational models and litmus tests. It validates an ext4 model
and demonstrates model-relative verification and synthesis of synchronization
calls. This is a route to reviewing both missing fences and unnecessary fences;
it is not a universal minimal-flush theorem for this library.

For V7/I15, make the [storage contract](recovery-storage-contract.md) precise
about byte/length persistence, creation/rename/deletion discovery, ordering
across files and restart quiescence. Couple each modeled premise to small
host-level litmus tests for the actual runtime, OS, filesystem, mount and
device configuration. Extend this to replacement and ownership receipts, not
only database contents. Finite tests validate assumptions; they do not prove
an unrestricted filesystem model. Linux/ext4 findings cannot qualify Windows,
macOS, SMB or arbitrary caller streams.

[Can Applications Recover from fsync Failures?](https://www.usenix.org/system/files/atc20-rebello.pdf),
USENIX ATC July 2020, is complementary experimental evidence. In its tested
Linux filesystems, failed writeback can leave pages marked clean, so a later
successful flush need not rewrite the lost bytes. A managed exception sweep
does not exercise that behavior. Preserve uncertain outcomes and evidence;
qualify rewriting, rereading and recovery after failure rather than treating
a bare retry as a successful fence. These are requirements on the host
contract, not a newly demonstrated defect in the library.

### Peak live storage and suspended owners

[IrisFit / Will It Fit?](https://iris-project.org/pdfs/2025-toplas-willitfit.pdf),
TOPLAS 2025, proves sound space-credit reasoning for concurrent programs with
tracing GC. Its analysis includes roots held by sleeping threads; retained
storage need not be reachable from an active cache. Its strongest guarantees
depend on its language's allocation, polling and protected-section semantics,
and the paper explicitly does not bound physical heap size.

Under I7/S1/V10, apply that accounting discipline to
[ReaderPageCache](../../src/JetDatabaseWriter/Pages/ReaderPageCache.cs),
[PageFrame](../../src/JetDatabaseWriter/Pages/Paging/PageFrame.cs),
[PageLease](../../src/JetDatabaseWriter/Pages/Paging/PageLease.cs),
[Pager](../../src/JetDatabaseWriter/Pages/Paging/Pager.cs) and suspended async
enumerators. Charge distinct live arrays, directories, lease objects and
producer loads, including displaced leased frames and duplicate reader
misses. Count old and new visited tables simultaneously during growth.
Release reusable-buffer credits only after all owners are discharged;
pool-retained storage is different from GC-unreachable storage.

Restate the named resource invariant in Dafny and connect actual acquisition,
transfer and disposal to it. IrisFit's runtime semantics are not .NET's:
neither its heap limit nor allocation-progress theorem transfers automatically.
This can guide safe copy removal and budget admission, but managed allocation
measurements and aggregate resource acceptance remain independent.

### Amortized work is a separate executable obligation

[Charguéraud and Pottier's time-credit development](https://www.chargueraud.org/research/2017/credits_jar/credits_jar.pdf),
JAR March 2019, verifies functional correctness and amortized complexity of
imperative union-find using consumable proof credits in Coq/CFML. Its
inverse-Ackermann bound concerns union-find; the transferable result is the
method for composing work budgets, not that bound for a hash table.

For V8/S1, add ghost work counters to the existing
[LongValueKernel](../../verification/LongValues/LongValueKernel.dfy) and
[LongValueTraversal](../../verification/LongValues/LongValueTraversal.dfy)
composition. Charge every probe, source scan, destination initialization and
rehash insertion, including unsuccessful and duplicate operations. Relate the
sum to the containing [LongValueStore](../../src/JetDatabaseWriter/LongValues/LongValueStore.cs)
path and the work between cancellation checks. Geometric array growth bounds
allocation volume but does not make deterministic colliding probes constant
time. Preserve worst-case adversarial bounds and separate total work,
peak storage and cancellation latency. Erased ghost counters add no executing
collection; wall-clock bounds still require managed-path qualification.

### Parser composition and stable input identity

[EverParse](https://www.microsoft.com/en-us/research/publication/everparse/),
USENIX Security 2019, verifies binary parser/serializer safety and inverse
properties. [EverParse3D](https://www.normalesup.org/~ramanana/research/everparse/pldi2022/paper.pdf),
PLDI 2022, adds verified double-fetch freedom using forward-stream permissions,
alongside memory, arithmetic and format correctness. User parsing actions'
functional correctness remains outside its stated specifications.

For V8/S1/S3, relate validation to the exact bytes consumed by
[RowDecoder](../../src/JetDatabaseWriter/ValueDecoding/RowDecoder.cs),
[DataPageRows](../../src/JetDatabaseWriter/Pages/DataPageRows.cs) and long-value
traversal. Compose cursor progress, layout admission, output effects and
failure/cancellation behavior. Keep the matching immutable page lease alive
through awaited consumers: checking one image cannot authorize use of a changed
or recycled buffer. Double-fetch freedom is a useful streaming discipline,
not a reason to prohibit rereading immutable leased bytes.

These proofs can inform removal of redundant copies/checks only after full
composition. Do not import F*/C into the managed package or require canonical
encodings that exclude valid native representations. Preserve opaque/reserved
bytes and independent Microsoft Access evidence; a format specification alone
does not establish compatibility.

### Refining frame ownership through CAS and cache locks

[ReLoC Reloaded](https://lmcs.episciences.org/7708), LMCS July 2021, provides
mechanized contextual refinement for fine-grained concurrent implementations
through logically atomic relational specifications in Coq/Iris. It supplies
a pattern for relating concrete concurrent operations to an abstract ledger,
rather than treating that ledger's atomic transitions as verified code.

For V10/I7, bridge [PageFrameOwnership](../../verification/Pages/Paging/PageFrameOwnership.dfy)
to [PageFrame](../../src/JetDatabaseWriter/Pages/Paging/PageFrame.cs) and
[PageLease](../../src/JetDatabaseWriter/Pages/Paging/PageLease.cs):
successful CAS, failed retries, lease-construction compensation, final release
and cache-locked lookup/producer handoff all need correspondence. Identify the
linearization points and relate actual frame/array identity to the tickets.
Keep .NET Interlocked, Volatile, locks, allocation and pooling as explicit
effect boundaries, with their callers' preconditions established.

Extend the existing Dafny interleaving model rather than adopting another
shipping engine. A refined reference ledger would strengthen the justification
for removing copies; it would not establish database-read linearizability
during concurrent mutation, deadlock freedom, an aggregate heap ceiling or
better throughput. Those remain separate named obligations and measurements.

### B+ tree refinement and exact leaf iteration

[Mündler and Nipkow's verified B+ trees](https://arxiv.org/html/2208.09066v1),
ICTAC 2022, refine an abstract set through a functional tree to imperative
membership, insertion and range iteration in Isabelle/HOL. An ordered fringe
of leaf pointers connects tree descent to the linked leaf chain. The paper
proves functional deletion, but not its imperative counterpart.

For I2/V9, relate [IndexCursor](../../src/JetDatabaseWriter/Indexes/IndexCursor.cs)
and [IndexBTreeEditor](../../src/JetDatabaseWriter/Indexes/IndexBTreeEditor.cs)
through the same ordered leaf sequence. Splits, root changes and sibling edits
must preserve exact results without omissions or duplicate visits. Replace the
paper's set/count-capacity assumptions with duplicate-preserving encoded-key
and bookmark semantics, native byte capacity, prefix compression and Prev/Next
links. Page emission proofs alone do not establish that correspondence.

The earlier [KIV/TVLA B+ tree proof project](https://kiv.isse.de/projects/btree.html)
complements the missing imperative deletion argument: checked recursive
insertion/deletion compose with splitting, merging and redistribution proofs,
including balance, size and no-dangling-pointer invariants. Reuse this local
edit/parent-controller decomposition for grouped edits and reclamation. Neither
development proves JET compressed page forms, transaction rollback or physical
crash safety. Retain I2/V9's native evidence and V6 composition before removing
fallback paths; no throughput improvement follows from the structural proofs.

### Relocation preserves the complete rooted reference graph

[CertiGC](https://arxiv.org/html/2609.13186v1), a 2026 preprint on a verified
generational collector, connects a C implementation to Rocq/VST proofs of
root-reachable graph isomorphism. Forwarding, scanning and remembered-set
proofs preserve labels, edges and sharing through the outer collector loop.
Its graph/root invariants and contracted nonportable pointer/tag primitives
remain assumptions; it is not a proof about database files or .NET GC.

For I17/V9, apply the graph-preservation criterion to
[CompactRowRelocationMap](../../src/JetDatabaseWriter/Maintenance/CompactRowRelocationMap.cs),
[CompactPinnedDatabaseValidator](../../src/JetDatabaseWriter/Maintenance/CompactPinnedDatabaseValidator.cs)
and [UsageMapOwnership](../../src/JetDatabaseWriter/Pages/UsageMapOwnership.cs).
Monotone unique bookmarks establish neither complete incoming references nor
preserved object identity. Require an injective relocation that preserves root
observations, reference targets and shared ownership, with all surviving edges
accounted for before reclaiming storage. Opaque native metadata remains
potential ownership and must stay pinned until completeness is established.
The collector's trusted root enumeration cannot discover unknown Access
references. Keep native inventory evidence, source-preserving refusal and
journal/publication crash safety separate from this relocation proof pattern.

### Rounded numeric bounds without lost matches

[Flocq](https://flocq.gitlabpages.inria.fr/), introduced at ARITH 2011, provides
machine-checked multi-radix floating/fixed-point arithmetic. Its
[generic rounding theorems](https://flocq.gitlabpages.inria.fr/flocq/html/Flocq.Core.Generic_fmt.html),
including `round_le`, `round_ge_generic` and `round_le_generic`, establish
monotonicity and bounds relative to representable values under explicit format
and rounding hypotheses. These are useful missing mathematical ingredients
for I9, rather than a general query-optimizer equivalence theorem.

[IndexSeekFilter](../../src/JetDatabaseWriter/Indexes/IndexSeekFilter.cs)
currently requires exact numeric operands. For a future rounded range, prove
that every row satisfying the original CLR predicate lies inside the encoded
seek interval. For example, an integral key satisfying `key > 1.5` cannot be
sought with the strict bound `key > 2`; the key `2` would disappear. Bound
direction and strict/inclusive endpoints must be derived together, with the
original residual predicate retained.

Instantiate the proof for integer, scaled Currency/Decimal and Single formats,
then connect native key ordering, widening/conversion, limits, NaN, infinities,
signed zero and null behavior. Real-valued generic rounding alone covers none
of those implementation choices. Preserve predicate evaluation order,
exceptions and per-execution captures. Keep native comparison evidence and
paired seek/scan measurements separate; do not replace current conservative
admission until its no-false-negatives refinement is established.

### Stable sorting and splice allocations

[Virtual-Memory Powersort](https://arxiv.org/abs/2605.27147v2), submitted May 26
and revised July 8, is an ESA 2026 paper. It gives stable adaptive sorting with
an auxiliary buffer of `O(sqrt(n log n))` objects, versus conventional
mergesort's `n/2`, while adding only `O(n)` comparisons and moves relative to
earlier Powersort implementations. The
[author's publication page](https://www.wild-inter.net/publications/moltmann-nakajima-wild-2026)
links the implementation and experimental material.

[IndexEntrySplicer](../../src/JetDatabaseWriter/Indexes/IndexEntrySplicer.cs)
requires stable ties, including preexisting equal keys before additions and
declaration order within each input. It also accepts unordered inputs and must
leave them unchanged, as specified by
[IndexEntryOrderingTests](../../tests/JetDatabaseWriter.Tests/Indexes/IndexEntryOrderingTests.cs).
A merge may be used only after establishing sorted survivor and addition
sequences. The existing stable full-sort behavior remains necessary for the
general input contract.

Prefer the smallest compatible representation change: reuse the private
survivor buffer and merge already ordered additions. A full Powersort port is
justified only if large general stable sorts remain material after profiling.
Sort scratch is distinct from the result buffer, materialized rows and encoded
key bytes. [QueryOrdering](../../src/JetDatabaseWriter/Queries/QueryOrdering.cs)
also preserves stable ties, but a replacement must additionally preserve
deferred execution, multikey ordering, selector invocation and exceptions.

The production splicer implements this ordered-run merge. It tests additions
first so unordered additions can reach the fallback without scanning the
survivor run. Backward merging reuses the private result buffer and preserves
existing-before-added equal keys. The general stable sort remains available
for either unordered run. Focused tests cover long shared-prefix keys,
duplicates, removals, input immutability and the result's allocation bound.

The relevant acceptance work is I2/I12/I13. Compare stable fast and fallback
paths with a fixed baseline on hosted CI, including ordered, interleaved,
duplicate-heavy and unordered inputs. Retain meaningful input-immutability and
refusal tests. A targeted allocation regression can establish a named managed
allocation bound; it does not establish a wall-clock speedup.

The [hosted algorithm-control run](https://github.com/symbiogenesis/JetDatabaseWriter/actions/runs/41112173782)
passed for `33668cee06fcd74c0ec04f825fc049f61ed61638`, using the default
adaptive job on Windows x64, .NET 10.0.14 and SDK 10.0.403. Its fixture has 600
entries with 128-byte shared-prefix keys and 60 duplicate-key additions; the
fallback reverses the additions. Both controls execute the original private
list plus stable `OrderBy(...).ToList()` algorithm with the same inputs.

| Case | Mean | Error | Allocated per operation |
|---|---:|---:|---:|
| Ordered merge | 14.108 microseconds | 0.240 microseconds | 15,896 bytes |
| Ordered sort control | 63.282 microseconds | 0.541 microseconds | 55,848 bytes |
| Unordered fallback | 68.248 microseconds | 1.354 microseconds | 55,848 bytes |
| Unordered sort control | 66.300 microseconds | 0.888 microseconds | 55,848 bytes |

For this fixture, the merge is 4.49 times faster and allocates 71.54% fewer
bytes. The fallback's 2.94% higher timing estimate has overlapping 99.9%
confidence intervals; the run establishes neither a fallback regression nor
unchanged timing. Error is BenchmarkDotNet's half-width of that interval.
All four setups and measurements completed with valid means and allocation
counts. These are same-host algorithm controls, not public-insert speedups or
a revision comparison; result storage remains allocated.

The [public-insert revision comparison](https://github.com/symbiogenesis/JetDatabaseWriter/actions/runs/41112178971)
uses validated baseline `a662067f54dd231a21d53f51a27cfdee9207c9b3` and the same
head, with the default adaptive job. It measures `InsertSameLeaf` and
`InsertCrossLeaf` in baseline/head, head/baseline and identical-head pairs on
one runner. This comparison includes all changes between the revisions;
algorithm attribution comes from the separate splice controls above.

The revision comparison completed successfully, with all twelve measurements
valid. Within each revision, allocations were identical across its runs.
Timing ratios are head/baseline for the revision pairs and second/first
for the identical-head pair:

| Public insert case | Baseline-first time ratio | Head-first time ratio | Identical-head time ratio | Baseline bytes | Head bytes |
|---|---:|---:|---:|---:|---:|
| Same leaf | 1.0209 | 1.1535 | 1.0026 | 597,032 | 571,784 |
| Cross leaf | 1.0718 | 0.9658 | 0.9576 | 785,160 | 734,664 |

The named public workloads allocate 4.23% and 6.43% fewer bytes across the
revision comparison. Public timing acceptance remains open under I12. The
15.35% slower same-leaf pair exceeds its within-run error margins, while the
four identical-source same-leaf head means span 3.181 to 3.753 milliseconds. The forward
head cross-leaf distribution is multimodal. All six logs warn about short
3-to-4-millisecond destructive iterations (`InvocationCount=1`). These results
support neither causal attribution of the timing differences nor a public
speedup or no-slowdown guarantee. Retain the measured allocation benefit;
isolate source effects and runner variation before expanding the optimization.

### Exact visited-set behavior in generated production code

[DafnyPro](https://arxiv.org/abs/2601.05385v1), a January 8 preprint, combines
annotation generation with a base-program diff checker, pruning of redundant
invariants and reusable proof hints. Its workflow is relevant to the existing
Dafny boundary without adopting another verifier or shipping dependency.

The concrete target is
[LongValueVisitedSet](../../src/JetDatabaseWriter/LongValues/LongValueVisitedSet.cs)
and the generated
[LongValueKernel](../../verification/LongValues/LongValueKernel.dfy).
Useful executable contracts establish exact membership, insertion of one
absent nonzero identity, preservation during rehash, and safe early lookup
termination at an empty slot. Keep count/growth callers, allocation failures,
native row identity and the containing parser's resource/ownership obligations
explicit. Deterministic hash correctness does not establish expected
constant-time lookup for adversarial files.

Production now calls generated `ContainsVisited`, `AddVisited` and
`RehashVisited` array operations. Their contracts establish exact nonzero
membership, preservation of the probe-chain invariant, occupied-slot progress
and unchanged duplicate/full-table outcomes. `AddVisited` distinguishes a new
identity, an existing identity and a full table in one scan. Below half load,
the wrapper uses that scan directly; at the growth threshold it checks for a
duplicate before allocating. Rehash constructs a valid destination from any
source array in distinct, larger, initially zero-filled storage, including
duplicate source identities. Its output has unique nonzero entries and an
occupied count equal to the number of distinct nonzero source identities.
Generated `PrepareVisitedAdd` and `CompleteVisitedGrowth` compose those array
operations with occupied-count bookkeeping and the half-load growth policy.
The ghost `TrackedVisited` representation connects empty initial storage,
distinct exact membership, occupied count, half occupancy, the native capacity
ceiling and `capacity <= 16 + 4*count`. Its set/count expressions execute only
during verification. The C# adapter supplies the requested distinct zero-filled
CLR array and retains/publishes the matching array/count pair; actual allocation,
adapter sequencing and containing-parser composition remain explicit obligations.

The controller returns only the selected operation's payload in a native
64-bit decision: zero for unchanged state, a direct count after insertion,
a high flag plus capacity for growth, and all bits set for exhausted capacity.
Classification, payload inverses and a constructive representation theorem retain
the abstract transition; unchanged and growth-request outcomes retain the input
count through ghost effective projections. Null initial storage is admitted
directly, alongside the existing empty-array form. The adapter publishes matching
count/storage only after success. This representation adds no allocation,
collection or probe work. Timing still requires the hosted original-wrapper
controls across ordinary, colliding, duplicate-boundary and single-row inputs;
verification alone establishes no performance result.

Resource composition must also count work inside an insertion. Growth still
requires lookup, rehash and insertion work using deterministic hashing.
Half occupancy does not bound every collision cluster by a constant.
The containing `LongValueStore.CopyPayloadAsync` checks cancellation between
rows; the synchronous probe and rehash loops run inside one row. Under V8/S1,
assess total probes and cancellation latency on colliding valid identities,
and count both arrays while growth holds the old and new tables. A terminating
array loop and a row limit do not establish a useful latency bound. Preserve
exact cycle refusal when evaluating any alternative representation.

The single-scan change removes the separate absence lookup on ordinary
insertions. It does not change the geometric array allocation policy or
establish a public read speedup; hosted comparisons own timing acceptance.
The generated orchestration retains that scan/allocation schedule and adds no
executing set, recount or tuple allocation. The recorded integration and
benchmark results in this survey predate this increment and supply no new timing or
acceptance verdict for it.

V8 owns the remaining composition into the full named parser. Validate the
exact source using the integrity-pinned verifier, audit, negative controls and
generated freshness, then exercise production collision, growth, duplicate and
cycle-refusal paths. Independent malformed-file and native evidence remain
required.

### Specification adequacy and proof maintenance

[ProofPulse](https://sr-lab.github.io/publication/2026/ASE1/), an August ASE 2026
paper, analyzes Dafny proof dependencies to identify weak coupling, unnecessary
preconditions and vacuous proofs. The authors report evaluation on 252
programs. Coverage cannot determine semantic intent or replace the verifier
and trust audit.

Its [artifact](https://github.com/VeriFixer/ProofPulse) uses Node.js, Dafny and
Z3; Linux is its primary platform. Example material uses Dafny 4.11.1, whereas
this repository pins 4.11.0. The pinned verifier already provides
[proof-dependency warnings, logs and coverage](https://dafny.org/v4.11.0/DafnyRef/DafnyRef#1375-analyzing-proof-dependencies).
Use those diagnostics first through the optional `-AnalyzeProofCoverage` mode
of [verify-proofs.ps1](../../scripts/verify-proofs.ps1); see its
[execution instructions](../../verification/README.md#proof-dependency-diagnostics).
The mode retains a separate diagnostic receipt and reports after strict
complete-inventory verification, audit, negative controls and generated-source
checks. Prefer `-ProofCoverageSources` for a small admitted source and its exact
transitive normal-source dependencies; whole-inventory assertion isolation can
produce very large logs and exceed the process budget. The diagnostic receipt
names the complete selected closure. Selection never narrows mandatory
verification, and ordinary CI never enables diagnostics. Coverage output never
supplies acceptance evidence.

If richer ProofPulse attribution becomes useful, an external pilot must
demonstrate compatibility with the exact pinned verifier/solver, preserve
dependency closure and leave canonical sources, generated output and receipts
unchanged. Keep third-party analysis outside the shipping package and required
gate until its behavior and maintenance cost are qualified. Finding review
belongs to V12; successful diagnostic execution alone does not close it.

The focused visited-table review illustrates the distinction. Writing zero
into an empty slot is an identity update, so the preservation lemma accepts
zero; executable insertion still requires a nonzero identity for occupied-slot
progress. The probe-distance injectivity lemma retains its ring-domain bounds
because its calls require them, even when dependency analysis reports a bound
as unnecessary. Neither disposition changes generated executable behavior.

The [Comparator methodology](https://github.com/leanprover/comparator) separates
a trusted theorem statement from an untrusted proposed proof, checks statement
dependencies and restricts admitted axioms. OpenAI publishes
[Comparator challenges](https://github.com/openai/math/blob/main/lean/ComparatorChallenges/README.md).
Comparator moved into Lean core in release 4.35.0; the standalone repository
was archived October 20. Its useful transfer is independently controlled
contracts and negative witnesses. The current Dafny review policy already
requires review of stronger preconditions, weakened guarantees and changed
supported behavior; no Lean migration or approximate regex-based semantic
checker is warranted.

[DafnyCOMP](https://proceedings.iclr.cc/paper_files/paper/2026/hash/c04d37be05ba74419d2d5705972a9d64-Abstract-Conference.html),
an ICLR 2026 study, reports severe verification failures for its evaluated
models when contracts must compose across small multi-function programs.
Its measured models are not a universal limit. It supports V6/V12's separate
obligation to compose parsing, admission, effects and recovery through the
executing controller. Successful isolated lemmas do not discharge that work.

### Fixed-contract repair and compiler trust

[AxDafny](https://arxiv.org/abs/2606.32007v1), a June 30 preprint from Axiomatic
AI, evaluates iterative synthesis of implementations and proof annotations.
Its separate evaluation of programs compiled to Python finds that verified
programs can still exceed resource limits: functional contracts need not
specify complexity.
Its static preservation check retains the original `requires` and `ensures`
clauses as a subset. Consequently, that check alone cannot reject an added
precondition that excludes supported inputs. The paper additionally uses
model-based review.

Transfer the repair loop, not its acceptance predicate. For V6/V8/V12, fix
reviewed entry domains, guarantees and semantic helper definitions before
annotation search; inspect added preconditions and changes to executable work
separately. Retain successful supported-input witnesses as well as refusal
controls. Specify resource counters when a resource bound is the intended
claim, then qualify allocations and timing on the executing managed path.
An automated reviewer does not replace the repository's trust review.

[Verified VCG and Verified Compiler for Dafny](https://arxiv.org/abs/2512.05262v1)
is CPP 2026 work, first submitted December 4, 2025. Its HOL4 development proves
a verification-condition generator and compilation to CakeML for an imperative
Dafny subset with arrays, loops and mutually recursive calls. This is relevant
to V12's residual-trust inventory, but it does not verify the pinned upstream
Dafny-to-C# compiler, the repository's projection, Roslyn or .NET. It therefore
does not reduce the current shipping trust boundary. Reconsider only if a
checked translation covers the exact source subset and managed output without
changing the library's targets or runtime dependencies; introducing a second
shipping engine would add an unrelated maintenance obligation.

### B-tree edits and bounded structural work

[Towards a B+-tree with Fluctuation-Free Performance](https://arxiv.org/abs/2603.04785v2),
submitted March 5 and revised October 13, is a preprint with simulation
evidence. Its FFBtree proactively splits critical nodes and proves at most one
node split per insertion in its model. The result concerns structural/I/O
fluctuation, not a wall-clock latency guarantee.

This is relevant to
[IndexBTreeEditor](../../src/JetDatabaseWriter/Indexes/IndexBTreeEditor.cs) and
I2/V9, but its entry/pointer-count capacity model does not cover JET's
variable-length encoded keys and shared-prefix page compression. The proposed
critical-node bitmap also needs bounded storage and a correct reconstruction,
rollback and invalidation policy for databases opened after native edits.

Complete ordinary incremental edits and their ownership/reclamation contracts
before adopting proactive splitting. Remaining whole-tree rebuilds already
limit relational and attachment scaling under I13. Any later FFBtree experiment
needs a byte-capacity invariant, native-compatible page representation and a
measured bound on actual page reads, writes and allocations, including bitmap
construction and maintenance. The paper's bound cannot be copied into a public
guarantee without that refinement.

### Clustered page identities

[Optimal Time-Space Tradeoff for Dynamic Difference-Encoded Dictionaries](https://arxiv.org/abs/2608.06077v1),
an August 6 preprint, gives exact dynamic integer dictionaries near gap entropy
with expected amortized operation bounds under its word-RAM and universe-size
assumptions. Clustered page identities are a possible match for ownership,
visited and compact/repair metadata.

Measure identity distributions, cardinalities, peak overlap and retained bytes
under S1/I7/I17. Compare simple ranges, bitmaps and delta-coded sorted arrays
before adding a complex dynamic dictionary. Include mutation, absent-key
queries, cancellation and scratch construction in the comparison. Internal
metadata can change; Access's prescribed usage-map encoding cannot be replaced
by a research representation. No alternative is selected without evidence.

Use the existing immutable path as a concrete baseline:
[CompactRowRelocationMap](../../src/JetDatabaseWriter/Maintenance/CompactRowRelocationMap.cs)
validates strictly increasing source and destination bookmarks and searches a
disk-capable fixed-width ledger with one eight-byte scratch buffer.
[CompactRawIndexBuilder](../../src/JetDatabaseWriter/Maintenance/CompactRawIndexBuilder.cs)
already streams sorted keys and supports disk-backed level fences. A dynamic
dictionary does not automatically improve immutable metadata. Compare disk bytes, seeks,
construction scratch and decode work as well as retained managed bytes, and
preserve bookmark order because it controls equal-key tie order.

### Cache movement versus live storage

OpenAI's [randomized k-server formalization scope](https://github.com/openai/math/blob/main/lean/docs/110.md)
gives an `O(log^2(k+1))` competitive movement bound against oblivious finite
request sequences. Its uniform finite-rational-metric algorithm has polynomial
preprocessing and per-request bit cost, with an instance-dependent additive
movement constant. These are not constant-time cache operations or bounds on
the memory occupied by live page images.

[LruCache](../../src/JetDatabaseWriter/Infrastructure/LruCache.cs) implements a
CLOCK approximation rather than strict LRU. In
[ReaderPageCache](../../src/JetDatabaseWriter/Pages/ReaderPageCache.cs), the
non-`Pager` miss path loads outside the cache lock, so overlapping misses can
create multiple images of one page; a displaced leased image remains live.
`Pager` serializes its loads through `frameGate`, which must be represented
separately. I7/V10 accounting therefore needs retained frames, displaced leased
frames, in-flight loads, row directories and lease objects. Measure duplicate
reader misses, invalidation, cancellation and abandoned enumeration before
selecting an eviction or request-coalescing change. The k-server theorem alone
does not justify replacing CLOCK or establish an allocation improvement.

## Headline results and unsuitable transfers

### Exponential Time Hypothesis

OpenAI's [October 26 announcement](https://openai.com/index/building-scientific-understanding-with-academic-communities/)
releases its ETH result and describes an independent examination led by
Impagliazzo, Paturi and Williams. The
[formalization scope](https://github.com/openai/math/blob/main/lean/docs/378.md)
states exact fixed-width CNF counting in subexponential time and polynomial
space. Its time bound is weaker than the sharper October 25 manuscript's
bound. These scopes must be cited separately.

In the formalized bound, `k >= 2` is fixed and `L` is the canonical input's
bit length: time is `(L+1)^{O_k(1)} * 2^{O_k(n (log log n)^2 / sqrt(log n))}`
with polynomial tape space. The manuscript removes one `log log n` factor
from that exponent, and removes the other for `m <= Dn` with fixed `D`.
The announced independent examination is a review process, not a published
verdict or a benchmark of an executable SAT solver.

The production library does not solve SAT, and its development verifier does
not acquire a practical solver improvement merely from this theorem.
Subexponential time does not imply polynomial time or establish `P = NP`.
The result alone supplies no demonstrated replacement for page scans, B-tree
edits, native collation or existing cryptographic primitives. Reconsider it
only when a concrete solver implementation or derived algorithm matches an
identified workload and computational model.

### Other reviewed results

| Primary result and status on the survey date | Repository decision |
|---|---|
| [Optimal-order randomized k-server](https://github.com/openai/math/blob/main/lean/docs/110.md), September manuscript and Lean scope | See [cache movement versus live storage](#cache-movement-versus-live-storage). The metric movement bound does not supply the current cache's aggregate memory or ownership contract. |
| [Unbounded bin-packing configuration-LP gaps](https://github.com/openai/math/blob/main/lean/docs/118.md), September manuscript and Lean scope | Useful caution for I17: a configuration-LP estimate has no universal fixed additive guarantee in the theorem's general setting. It supplies no faster compact implementation or lower bound specialized to fixed-size Access pages. |
| [Braided cuckoo hashing](https://arxiv.org/abs/2610.33229v1), October 23 preprint, stated to appear at SODA 2027 | Its near-full-table high-probability bounds assume a specified randomized model and operation sequence. The deterministic half-full visited set does not inherit them. No table replacement is selected. |
| [Near-Optimal In-Place Sorting](https://arxiv.org/abs/2607.27040v2), July 29 preprint, revised October 11 | Its main algorithms are unstable. Direct use violates splice/query tie contracts. Ordinal decoration costs storage; consider only at independently justified unstable-sort sites. |
| [Space-Efficient Lock-Free Linear-Probing Hash Table](https://arxiv.org/abs/2606.17315v1), June 15 preprint | Its expected amortized step analysis assumes no concurrent insertions of the same key and depends on point contention. Reader misses can target the same page; table linearizability also does not establish frame ownership, eviction or publication. No concurrency is needed in the per-traversal visited set. |
| [Locality in Open Addressing Hash Tables](https://arxiv.org/abs/2607.16390), July 17 preprint | High-occupancy probe/locality tradeoffs caution against saving array space by raising load without measuring collision behavior. The current half-occupancy policy remains. |
| [Consensus for Compressed Static Functions](https://arxiv.org/abs/2609.39557), September 30 preprint | Queries outside the fixed domain can return arbitrary values. This cannot replace exact ownership, corruption or visited-set membership. |
| [The Case for Synthesized Data Compression](https://arxiv.org/abs/2610.09913), October 7 experimental proposal | Dataset-specific compression measurements are not a universal codec theorem. Prescribed native encodings and arbitrary-input parser safety exclude using synthesized code carried in database data. |
| [OpenAI algorithm catalog](https://github.com/openai/math/blob/main/overview.tex): integer/matrix multiplication, exact Fourier transforms, log-space derandomization, graph matching, approximate edit distance and deletion-trace reconstruction | No corresponding measured library bottleneck or compatible recovery contract was identified. Arithmetic-model savings do not translate to fixed-width managed operations; approximate/reconstruction outputs cannot authorize invented database content. |

The older [open-addressing result associated with Yao's conjecture](https://arxiv.org/abs/2501.02305)
was submitted January 4, 2025. It is useful background, not a 2026 discovery,
and its no-reordering bounds must not be conflated with arbitrary dynamic
deletions or measured .NET dictionary improvements.

## Applying proof-search progress

[Anthropic/Theorem's October 22 report](https://www.anthropic.com/research/formal-verification)
describes proofs for 239 fuzz entry points in 123 libraries. Its published
binary-level setting assumes bounded inputs, a fixed memory budget and
single-threaded RISC-V/Linux execution. Harness properties such as round-trip,
idempotence and reference equivalence can become quantified specifications;
those artifacts do not directly verify this managed C# library or its storage
durability.

[DeepMind's May/June formal-proof-search study](https://arxiv.org/abs/2605.22763v2)
reports checked solutions to selected Erdős and OEIS problems. Its transferable
contribution is verifier-guided search and reusable lemmas. No imported
number-theory or combinatorics theorem was identified that changes native
database behavior.

Use this increased proof-writing capability within the existing architecture:
review contracts and independent success/refusal examples first; construct
small executable proofs; preserve dependencies, tool pins and trust controls;
then compose the production caller. Keep ordinary tests, fuzzing, native
interoperability and physical-storage qualification at their stated evidence
levels. See the [proof review procedure](proof-trust-review.md) and
[proof execution instructions](../../verification/README.md).

## Regression coverage for relied-on behavior

Current applications include the ordered-run splice, generated visited-set
behavior and per-effect recoverability discipline in the undo model, alongside
the existing proof/trust pipeline. Further literature transfers remain
composition/qualification obligations, candidates, cautions or rejected
transfers; their theorems are not production guarantees. The following unit
and conformance tests check the executing contracts, including the existing
admitted proof boundaries cited
by the integration evidence. The [evidence map](verification-evidence-map.md)
retains the complete production mappings and remaining obligations.

| Relied-on behavior or model boundary | Relevant regression evidence |
|---|---|
| Stable ordered-run merge and general splice fallback | [IndexEntryOrderingTests](../../tests/JetDatabaseWriter.Tests/Indexes/IndexEntryOrderingTests.cs) checks existing-before-added ties, declaration order, input/key immutability and the result-storage allocation bound. `Splice_MatchesIndependentStableBucketsForEverySmallRunAndRemovalSubset` compares every three-existing/two-added run over four empty/prefix/high-byte keys against independent stable buckets, with every removal subset. [IndexPageCodecAndEntrySplicerTests](../../tests/JetDatabaseWriter.Tests/Indexes/IndexPageCodecAndEntrySplicerTests.cs) retains missing-removal refusal. |
| Existing query ordering contract | [QueryPipelineParityTests](../../tests/JetDatabaseWriter.Tests/Queries/QueryPipelineParityTests.cs) checks stable multikey ties, paging, live captures, deferred evaluation and selector failures across root/include/tail paths. This remains tested handwritten behavior. |
| Conservative numeric seek, relocation and ownership baselines | [IndexSeekFilterTests](../../tests/JetDatabaseWriter.Tests/Indexes/IndexSeekFilterTests.cs) and [TypedMappingCoercionTests](../../tests/JetDatabaseWriter.Tests/Queries/TypedMappingCoercionTests.cs) retain fractional-bound refusal/predicate results. [CompactRowRelocationMapTests](../../tests/JetDatabaseWriter.Tests/Maintenance/CompactRowRelocationMapTests.cs) checks exact bookmark identity, monotone uniqueness, budgets and partial reads. [UsageMapOwnershipTests](../../tests/JetDatabaseWriter.Tests/Pages/UsageMapOwnershipTests.cs) and [UsageMapNonliveAliasTests](../../tests/JetDatabaseWriter.Tests/Pages/UsageMapNonliveAliasTests.cs) retain opaque-reference protection and source-preserving refusal. These are tested handwritten contracts; rounded-seek and complete rooted-graph refinement remain open. |
| Exact visited membership, probing, rehash and count/growth orchestration (`LongValueKernel`) | [LongValueTraversalTests](../../tests/JetDatabaseWriter.Tests/ValueEncoding/LongValueTraversalTests.cs) checks wrapped/full probes, duplicate-source rehash, native decision boundaries, null/empty initialization, count retention and duplicate/growth allocation boundaries. Independent multi-step controller traces compare membership and count through repeated growth with a `HashSet<uint>` oracle and the production wrapper. |
| Descriptor representation and bounded chained-payload traversal (`LongValueKernel`, `LongValueTraversal`) | [LongValueDescriptorKernelTests](../../tests/JetDatabaseWriter.Tests/ValueEncoding/LongValueDescriptorKernelTests.cs) uses independent byte/word oracles and unchanged-destination refusal checks. [LongValueStreamingTests](../../tests/JetDatabaseWriter.Tests/ValueEncoding/LongValueStreamingTests.cs) and [LongValueTraversalTests](../../tests/JetDatabaseWriter.Tests/ValueEncoding/LongValueTraversalTests.cs) check successful payloads, cycles/no progress, budgets, cancellation and leases. |
| Bounded reads, journal admission and replay (`RecoveryKernel`, `UndoJournal`) | [SharedParsingKernelTests](../../tests/JetDatabaseWriter.Tests/Infrastructure/SharedParsingKernelTests.cs), [RecoveryKernelTests](../../tests/JetDatabaseWriter.Tests/Pages/Paging/RecoveryKernelTests.cs), [JournalContentKernelTests](../../tests/JetDatabaseWriter.Tests/Pages/Paging/JournalContentKernelTests.cs) and [BufferedJournalConformanceTests](../../tests/JetDatabaseWriter.Tests/Pages/Paging/BufferedJournalConformanceTests.cs) check independent range/byte oracles, admission/refusal and production replay schedules. |
| Replacement admission, publication and cleanup (`RecoveryKernel`, `ReplacementProtocol`) | [ReplacementKernelTests](../../tests/JetDatabaseWriter.Tests/Encryption/ReplacementKernelTests.cs), [ReplacementFieldConformanceTests](../../tests/JetDatabaseWriter.Tests/Encryption/ReplacementFieldConformanceTests.cs), [ReplacementPublicationConformanceTests](../../tests/JetDatabaseWriter.Tests/Encryption/ReplacementPublicationConformanceTests.cs) and [ReplacementCleanupConformanceTests](../../tests/JetDatabaseWriter.Tests/Encryption/ReplacementCleanupConformanceTests.cs) check invalid-evidence refusal and interrupted effect schedules. |
| Generation authority and cooperating-client ownership (`GenerationOwnership`, `MultiProcessOwnership`) | [RecoverableWriterLockTests](../../tests/JetDatabaseWriter.Tests/Transactions/RecoverableWriterLockTests.cs), [CrossProcessLockTests](../../tests/JetDatabaseWriter.Tests/Transactions/CrossProcessLockTests.cs) and replacement cleanup conformance check stale-owner refusal, incomplete acquisition retries and exclusion. |
| Serialized scopes and frame lifetime (`WriterLifecycle`, `PageFrameOwnership`) | [WriterLifecycleConformanceTests](../../tests/JetDatabaseWriter.Tests/Writer/WriterLifecycleConformanceTests.cs) checks rollback/scope identity. [PageLeaseTests](../../tests/JetDatabaseWriter.Tests/Pages/Paging/PageLeaseTests.cs) checks concurrent same-lease disposal and distinct-reader acquisition/release, exactly-once accounting/return and surviving-reader/producer ownership. [PagerFrameCacheTests](../../tests/JetDatabaseWriter.Tests/Pages/Paging/PagerFrameCacheTests.cs) checks generation refusal during delayed lease acquisition, fresh cache sharing and old-image lifetime through invalidation/disposal. |
| Row-slot/jump-table acquisition (`RecoveryKernel`) and compaction (`DataPageKernel`) | [DataRowSlotKernelTests](../../tests/JetDatabaseWriter.Tests/Pages/DataRowSlotKernelTests.cs), [Jet3JumpTableKernelTests](../../tests/JetDatabaseWriter.Tests/Pages/Jet3JumpTableKernelTests.cs) and [DataPageReclaimKernelTests](../../tests/JetDatabaseWriter.Tests/Pages/DataPageReclaimKernelTests.cs) compare independent slot/page images and check refusal before mutation. |
| Index entry/child selection (`RecoveryKernel`) and page emission (`IndexMutationKernel`) | [IndexEntryBoundaryKernelTests](../../tests/JetDatabaseWriter.Tests/Indexes/IndexEntryBoundaryKernelTests.cs), [IndexChildSelectionKernelTests](../../tests/JetDatabaseWriter.Tests/Indexes/IndexChildSelectionKernelTests.cs) and [IndexPageEmissionKernelTests](../../tests/JetDatabaseWriter.Tests/Indexes/IndexPageEmissionKernelTests.cs) check bitmap/summary/byte oracles, truncated evidence and unchanged-destination refusals. |
| Bitmap access/update (`RecoveryKernel`) and ordered enumeration (`UsageMapKernel`) | [UsageMapBitmapKernelTests](../../tests/JetDatabaseWriter.Tests/Pages/UsageMapBitmapKernelTests.cs) and [UsageMapEnumerationKernelTests](../../tests/JetDatabaseWriter.Tests/Pages/UsageMapEnumerationKernelTests.cs) check native arithmetic boundaries, exact selected-bit changes, enumeration/failure positions and borrowed-page cancellation. |
| Mandatory proof gates and supplemental diagnostics | [verify-proofs.tests.ps1](../../scripts/tests/verify-proofs.tests.ps1) checks dependency-complete selection, diagnostic evidence separation and refusal before coverage or complexity analysis when verifier/solver pins, verification, audit, negative controls or any generated kernel's freshness fail. [proof-review.tests.ps1](../../scripts/tests/proof-review.tests.ps1) and [formal-workflow.tests.ps1](../../scripts/tests/formal-workflow.tests.ps1) retain review-inventory and required-gate controls. |

These are finite regression and conformance checks, separate from the pinned
verifier's quantified contracts. Tests do not establish the surveyed papers'
theorems, complete handwritten implementation refinement, adversarial resource
bounds, native compatibility or physical durability. Test links identify
inspectable evidence; only a completed exact-revision CI run supplies an
execution verdict. New proof or adapter claims require corresponding success
and refusal evidence under the [continuing review policy](proof-trust-review.md).
Review coverage against the property and its caller, rather than the number
of proof lemmas or tests: use independent result oracles, preserve supported
success domains, and exercise the effect adapters that establish the proof's
premises. Additional isolated kernel examples cannot discharge an untested
containing path or one of the evidence map's explicit composition gaps.

## Acceptance and plan ownership

The implementation results below belong to the named revisions. The literature
review and documentation refinements add no new execution or benchmark evidence.

The [integration CI gate](https://github.com/symbiogenesis/JetDatabaseWriter/actions/runs/41108919138)
passed for `33668cee06fcd74c0ec04f825fc049f61ed61638`. All 26 expected checks
passed, including the fifteen test partitions across the three runtime/library
combinations, strict analyzers, package and generated consumers, platform
encryption checks and formal verification. The formal receipt records nine
sources, 814 verified checks, zero errors or audit findings, passing negative
controls and fresh generated kernels. Only guarded DAO cases and the expected
explicit fuzz harnesses were skipped or not run. These results retain the
scope and native/platform evidence limits described above.

| Existing plan owner | Research-informed work and acceptance |
|---|---|
| V3/V5/V6 | Apply PoWER's per-effect recoverability discipline and CHL/Argosy recovery specifications to serialized admission, preparation, actual outcomes and complete layered restart. Compose initial states and abstraction relations; component proofs alone do not close the controller. |
| V7/I15 | Qualify explicit byte/length, namespace and stale-I/O premises against fixed host configurations using persistence litmus tests and failure survivors. Ferrite and failed-fsync experiments guide the model; neither supplies a platform qualification receipt. |
| V8/S1 | Compose generated visited-set count/growth orchestration with actual allocation/adapter effects, acquisition and the containing parser's complete resource/ownership bounds, including collision probes, rehash overlap and cancellation latency. Production executes generated verified behavior or a checked refinement. |
| I5/V6/V10 | Use transaction and concurrency refinement to cover auxiliary-state restoration, generation authority, actual CAS/cache-lock steps and producer/lease compensation. Include state outside page transactions explicitly; abstract ownership alone does not verify C# interleavings. |
| V12 | Review the pinned verifier's dependency diagnostics against independent examples. Consider optional ProofPulse only for a concrete attribution gap and demonstrate exact-pin compatibility. Preserve ordinary verification and negative controls; coverage alone cannot promote a guarantee. |
| I2/V9/I13 | Stable splice behavior and verified native incremental edits before proactive splitting. Compose exact tree/leaf-chain correspondence and local split/merge/deletion contracts; retain fallbacks, ordering, ownership, byte-capacity and DAO checks. |
| I17/V9 | Establish complete roots and incoming/potential references, then prove injective relocation preserves rooted object identity, edges and sharing before reclamation. Collector graph proofs do not discover unknown native references or establish crash-safe publication. |
| I7/S1/I17 | Evidence-led metadata representations with peak aggregate accounting, including pinned/evicted images, in-flight loads, live leases, suspended traversals, pool retention and growth overlap; compare immutable disk-ledger and simple bitmap/range baselines before dynamic compression. |
| I9 | Instantiate rounding bounds and prove no false negatives under native key/CLR numeric semantics, strict/inclusive endpoints and preserved predicate evaluation. Flocq supplies mathematical ingredients, not a refinement of the seek planner. |
| I12 | Settled-main CI and hosted paired measurements with allocation controls and representative workload shapes before accepting performance claims. |

Do not create parallel engines, alternative file encodings or new trusted
contracts merely to adopt a paper. Preserve managed shipping targets and
runtime dependencies. Add self-contained remaining work to the root plan when
a concrete implementation or evidence gap is discovered; keep completed work
out of that plan.
