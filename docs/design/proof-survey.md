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

The relevant acceptance work is I2/I12/I13. Compare stable fast and fallback
paths with a fixed baseline on hosted CI, including ordered, interleaved,
duplicate-heavy and unordered inputs. Retain meaningful input-immutability and
refusal tests. A targeted allocation regression can establish a named managed
allocation bound; it does not establish a wall-clock speedup.

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
verification, audit, negative controls and generated-source checks. It is not
enabled by ordinary CI and never supplies acceptance evidence.

If richer ProofPulse attribution becomes useful, an external pilot must
demonstrate compatibility with the exact pinned verifier/solver, preserve
dependency closure and leave canonical sources, generated output and receipts
unchanged. Keep third-party analysis outside the shipping package and required
gate until its behavior and maintenance cost are qualified. Finding review
belongs to V12; successful diagnostic execution alone does not close it.

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

## Headline results and unsuitable transfers

### Exponential Time Hypothesis

OpenAI's [October 26 announcement](https://openai.com/index/building-scientific-understanding-with-academic-communities/)
releases its ETH result and describes an independent examination led by
Impagliazzo, Paturi and Williams. The
[formalization scope](https://github.com/openai/math/blob/main/lean/docs/378.md)
states exact fixed-width CNF counting in subexponential time and polynomial
space. Its time bound is weaker than the sharper October 25 manuscript's
bound. These scopes must be cited separately.

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
| [Optimal-order randomized k-server](https://github.com/openai/math/blob/main/lean/docs/110.md), September manuscript and Lean scope | The general-metric competitive bound does not demonstrate a better eviction policy for the present cache. Revisit only with a matching heterogeneous cost model and measured benefit; include pinned images and lease ownership in I7. |
| [Unbounded bin-packing configuration-LP gaps](https://github.com/openai/math/blob/main/lean/docs/118.md), September manuscript and Lean scope | Useful caution for I17: a configuration-LP estimate has no universal fixed additive guarantee in the theorem's general setting. It supplies no faster compact implementation or lower bound specialized to fixed-size Access pages. |
| [Braided cuckoo hashing](https://arxiv.org/abs/2610.33229v1), October 23 preprint, stated to appear at SODA 2027 | Its near-full-table high-probability bounds assume a specified randomized model and operation sequence. The deterministic half-full visited set does not inherit them. No table replacement is selected. |
| [Near-Optimal In-Place Sorting](https://arxiv.org/abs/2607.27040v2), July 29 preprint, revised October 11 | Its main algorithms are unstable. Direct use violates splice/query tie contracts. Ordinal decoration costs storage; consider only at independently justified unstable-sort sites. |
| [Space-Efficient Lock-Free Linear-Probing Hash Table](https://arxiv.org/abs/2606.17315), June 15 preprint | Linearizability and lock-freedom do not establish cache eviction, pinning or publication semantics. Its complexity assumptions require separate review. No concurrency is needed in the per-traversal visited set. |
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

| Existing plan owner | Research-informed work and acceptance |
|---|---|
| V8/S1 | Exact visited-set operations composed with count/growth, acquisition and the containing parser's complete resource/ownership bounds. Production executes generated verified behavior or a checked refinement. |
| V12 | Review the pinned verifier's dependency diagnostics against independent examples. Consider optional ProofPulse only for a concrete attribution gap and demonstrate exact-pin compatibility. Preserve ordinary verification and negative controls; coverage alone cannot promote a guarantee. |
| I2/V9/I13 | Stable splice behavior and verified native incremental edits before proactive splitting. Retain fallback contracts, ordering, ownership, byte-capacity and DAO checks. |
| I7/S1/I17 | Evidence-led metadata representations with aggregate accounting, including pinned/evicted images, live leases and simultaneous traversals. |
| I9 | Rounded-bound seek inference requires a separate no-false-negatives proof under native numeric semantics and preserved predicate evaluation. None of the surveyed asymptotic results discharges it. |
| I12 | Settled-main CI and hosted paired measurements with allocation controls and representative workload shapes before accepting performance claims. |

Do not create parallel engines, alternative file encodings or new trusted
contracts merely to adopt a paper. Preserve managed shipping targets and
runtime dependencies. Add self-contained remaining work to the root plan when
a concrete implementation or evidence gap is discovered; keep completed work
out of that plan.
