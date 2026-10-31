# Proof and algorithm survey

Survey date: 2026-10-30.

This document connects recent mathematics, algorithms and proof-engineering
research to JetDatabaseWriter's performance, integrity, allocations and
maintainability. It records applicability decisions and their limits. The
[root plan](../../PLAN.md) owns outstanding implementation and acceptance work;
this document is not a second backlog.

## Scope and evidence

The survey screens OpenAI's mathematics catalog, examines selected relevant
theorem statements and formalization scopes, and includes independent 2026
algorithm and software-verification research. It is an applicability review,
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
rerun the upstream Lean or Rocq artifacts. Publisher-reported verification,
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
| Reuse ordered runs before general stable sorting | Existing splice measurements establish a named allocation benefit. I12 still owns public timing acceptance; a full sorting implementation needs a remaining measured bottleneck. |
| Compose exact visited-table operations with their caller | Integrity and explicit resource bounds under V8/S1. Include adversarial probe work, growth overlap and cancellation; exact membership alone is insufficient. |
| Fixed-contract proof repair and dependency review | Less proof-maintenance work under V6/V12, while preserving supported behavior and independently reviewed specifications. A benchmark solve rate is not repository acceptance. |
| Compact metadata and cache accounting | Possible allocation reductions under I7/I17/S1. Compare existing disk ledgers, ranges and bitmaps first; account for in-flight and leased storage as well as retained entries. |
| Proactive B-tree splitting | A later I2/V9 experiment after native byte-capacity and ownership invariants, incremental edits and workload evidence exist. |
| SAT, matrix arithmetic and unrelated pure mathematics | No matching production bottleneck or demonstrated drop-in improvement. Keep their theorem statements separate from practical performance claims. |

## Results with concrete repository applications

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
[LongValueKernel](../../verification/LongValueKernel.dfy).
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
occupied count equal to the number of distinct nonzero source identities. The
wrapper maintains occupied-count bookkeeping and the half-load growth policy;
those caller obligations remain manually reviewed rather than proved.

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
| V8/S1 | Exact visited-set operations composed with count/growth, acquisition and the containing parser's complete resource/ownership bounds, including collision probes, rehash overlap and cancellation latency. Production executes generated verified behavior or a checked refinement. |
| V12 | Review the pinned verifier's dependency diagnostics against independent examples. Consider optional ProofPulse only for a concrete attribution gap and demonstrate exact-pin compatibility. Preserve ordinary verification and negative controls; coverage alone cannot promote a guarantee. |
| I2/V9/I13 | Stable splice behavior and verified native incremental edits before proactive splitting. Retain fallback contracts, ordering, ownership, byte-capacity and DAO checks. |
| I7/S1/I17 | Evidence-led metadata representations with aggregate accounting, including pinned/evicted images, in-flight loads, live leases and simultaneous traversals; compare immutable disk-ledger and simple bitmap/range baselines before dynamic compression. |
| I9 | Rounded-bound seek inference requires a separate no-false-negatives proof under native numeric semantics and preserved predicate evaluation. None of the surveyed asymptotic results discharges it. |
| I12 | Settled-main CI and hosted paired measurements with allocation controls and representative workload shapes before accepting performance claims. |

Do not create parallel engines, alternative file encodings or new trusted
contracts merely to adopt a paper. Preserve managed shipping targets and
runtime dependencies. Add self-contained remaining work to the root plan when
a concrete implementation or evidence gap is discovered; keep completed work
out of that plan.
