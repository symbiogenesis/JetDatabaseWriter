# Proof and trust review records

The mandatory `formal` CI job checks [proof-review.json](../../verification/proof-review.json) before installing the verifier. The record binds reviewed implementation, conformance evidence, proofs, generated kernels, verifier/solver pins, build targets and proof tooling to their exact text content. A changed input fails until its review record is updated. This is an enforceable freshness requirement, not an attestation of reviewer identity or judgment, an automated C# refinement proof, or a storage qualification receipt.

## Scope and source inventory

[check-proof-review.ps1](../../scripts/check-proof-review.ps1) defines two mandatory boundaries:

- `implementation` includes the mapped journal, replacement, descriptor, range/read and lifecycle sources. It also discovers generated verification sources and textual calls to the shipped recovery/long-value kernels and their `BinaryBuffer`/`StreamRead` adapters. Cited conformance evidence is hashed with the source.
- `proof-trust` includes every Dafny source, the exact proof/dependency inventory, Dafny and bundled-Z3 pins, executable projection and audit policy, their regression tests, required CI lane, compiler/runtime targets and the documented model/storage boundary.

The production source-name inventory is recorded independently of source contents. Adding, deleting or moving any library C# source, including hidden files, requires a classification review, even when it does not call a kernel. A new kernel or explicit adapter caller must also be added to the implementation input set. Changes to unrelated existing C# file contents do not require a proof receipt update. The inventory detects changes in names; source inspection determines whether a new helper must join the explicit mapped-source set. Aliases, indirection and handwritten operations are not a machine-checked call graph.

Hashes use UTF-8 text with CRLF normalized to LF. Input paths are repository-relative and case-sensitive; duplicates, escaping paths, missing inputs and omitted new proof sources refuse. Evidence references must exist locally and are included in the checked content set. The receipt does not hash itself.

## Reviewing a change

Before changing hashes, inspect the source diff and update the affected boundary's concrete `obligations`, `conformance` and `remainingAssumptions` explanation. Keep the boundary and [production correspondence](formal-verification.md#production-correspondence) consistent.

For mapped storage or lifecycle code, review admission before effects, journal/database flush ordering, uncertain outcomes, lease/receipt authority, cleanup ownership, conditional replay markers, generation invalidation and snapshot/savepoint behavior as relevant. State which executable decisions are proved, which production traces were exercised, and which returned-I/O or namespace effects remain contracts. Add or change the exact conformance evidence paths rather than treating successful arithmetic proofs as validation of handwritten loops.

For a verifier, solver, compiler pin or trust-tooling update, inspect upstream provenance and the version/asset/digest changes. Review the policy, compiler projection, normal-source dependencies, negative controls and platform execution evidence together. Reinstall changed verifier distributions into fresh external directories and run verification, audit, both negative controls and generated freshness; qualify Windows and Linux distributions as required by the [tool update policy](../../verification/README.md#review-and-updates). A hash update alone does not satisfy that policy.

Record only the reviewed final input hashes and current production-name inventory digest. There is intentionally no blanket accept or receipt-refresh mode in the gate. Existing `Get-ProofReviewHash`, `Get-ProofReviewInputs` and `Get-ProofReviewInventoryHash` functions provide deterministic calculations; using them to calculate a hash does not perform the required review. Do not mark pending hosted checks as successful in the committed explanation.

Run a focused development check in a fresh external directory:

```powershell
pwsh -NoProfile -File scripts/check-proof-review.ps1 `
    -OutputDirectory (Join-Path ([IO.Path]::GetTempPath()) ('jdw-review-' + [guid]::NewGuid().ToString('N')))
pwsh -NoProfile -File scripts/tests/proof-review.tests.ps1
pwsh -NoProfile -File scripts/tests/verify-proofs.tests.ps1
pwsh -NoProfile -File scripts/tests/formal-workflow.tests.ps1
```

The ordinary settled-main CI gate remains required. Review receipts supplement the existing proof, analyzer, framework, trimming/NativeAOT and interoperability gates.

## Failure and counterexample evidence

The `proof-trust-review` artifact retains `review.json` with the review-freshness verdict, including the refusing input or inventory mismatch. The `formal-verification` artifact retains verifier/auditor stdout and stderr, audit reports, exact expected-negative-control sources, and per-process JSON with command arguments, exit status and timeout/launch failure details. Both artifacts have 30-day retention and upload after failed gates. Cancellation and hosting service failures can still prevent artifact upload.

A genuine proof-gate failure additionally emits `failure.json`, recording the failure, hosted SHA when available, input hashes and retained diagnostics. Its `inputs/` directory copies regular local proof and normal-source dependency files, proof/tool manifests, verifier scripts and generated kernels needed to inspect that failed attempt. Linked files and paths through linked directories are refused without copying their targets; refused or failed snapshots are recorded separately. If receipt retention itself fails, `failure-receipt.error.txt` records the supplemental error where possible and the original verifier failure remains the reported failure. An incomplete attempt never emits a successful `summary.json`. Existing result directories refuse reuse, preserving earlier failures for comparison.

The deliberately false theorem must fail verification, and the assumed theorem must trigger audit rejection. Their failure output is expected evidence that the controls work; those files remain external to the admitted proof inventory. A `failure.json` describes a real gate failure and explicitly distinguishes it from these expected controls. Checked counterexample lemmas in the admitted models remain ordinary proved sources and receive the same review and freshness checks as positive properties.
