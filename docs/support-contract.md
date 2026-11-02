# Supported operating envelope and release boundary

The first release targets the named operations and formats below. Broader real-format coverage, managed Compact & Repair and complete proof composition remain independent workstreams in [PLAN.md](../PLAN.md). Their long-term scope does not expand this release's guarantees.

| Surface | Release contract | Evidence and limits |
|---|---|---|
| Database generations | Jet3 Access 97, Jet4 `.mdb` and ACE `.accdb`, within the tested feature variants | The [native validation matrix](design/writer-disk-format-validation-matrix.md) owns fixture provenance and interoperability status. Recognizing a generation does not establish every collation, encryption provider or application-object layout. Pre-Jet3 formats are excluded. |
| Reading | Stable input, bounded configured parser surfaces, streaming row APIs and supported explicit index queries | Readers cache data and do not provide snapshots of external mutations. Materializing APIs retain their requested results. See [reading](reading.md) and [resource configuration](operations.md#configuration). |
| Writing | One exclusive writer; supported row/schema/relationship/complex mutations either complete or follow the documented rollback/faulted-writer outcome | Oversized ordinary Text and Binary values refuse intact. Unassigned native bytes retain their documented preservation policy. Unsupported expressions, collations and layouts refuse. Complete Access application preservation is excluded. |
| File recovery | Process-crash recovery with the database and its adjacent valid recovery evidence retained under exclusive access | Native Access does not replay this library's journal. Recover through the library before opening a crash-interrupted database natively. |
| Caller streams | Live rollback under the caller-stream contract | A stream wrapper around a file does not acquire persistent journal identity or reopen capabilities. See the [stream contract](design/caller-stream-recovery.md). |
| Power loss | No qualified storage configuration | The [storage contract](design/recovery-storage-contract.md) owns platform adapter status and qualification receipts. File-content flushes, process kills and verified models do not qualify physical persistence. |
| Storage maintenance | Free-page scrubbing and free-tail shrinking | Full Compact & Repair, complete forensic erasure and reclamation of unknown/shared storage are excluded. |
| Formal verification | The generated executable decisions and abstract protocols named in the evidence map | The [evidence map](design/verification-evidence-map.md) owns the distinction between executable proof, abstract proof, tested correspondence and remaining contracts. No whole-library, complete parser or filesystem proof is claimed. |

## Compatibility at a glance

| Surface | Supported operations | Safely refused or unqualified |
|---|---|---|
| Files | Jet3, Jet4 and ACE within the fixture and engine variants in the [native matrix](design/writer-disk-format-validation-matrix.md) | Pre-Jet3 generations are unsupported. Third-party encryption providers and unsupported native layouts refuse. Recognition does not qualify every feature variant. |
| Text and binary values | Values fitting the declared column and supported native encoding | Oversized values and unsupported leading-U+FEFF Text/Memo shapes refuse intact; see [writing values](writing.md#writing-data). Remaining native-engine confirmation is explicit in the matrix and storage notes. |
| Expressions and indexes | The supported expression subset and General97, General Legacy and General sort orders | Unsupported expressions or collations refuse mutation. Query planning uses a scan when an index cannot preserve the requested semantics. |
| Existing Access applications | Data operations and the separately tested metadata-preservation cases | Complete forms, reports, VBA, macros and application-object preservation are unqualified; the library does not execute them. |
| Recovery | Exclusive file reopen with the database and valid adjacent journal; caller-stream live rollback | Native Access cannot replay this journal. Physical power-loss durability is unqualified on every storage configuration. |
| Maintenance | Free-page scrubbing and free-tail shrinking | Full Compact & Repair and complete forensic erasure are outside the release contract. |

Release acceptance requires a successful exact-revision main CI gate, including selected strict Release analysis, every test partition/runtime/library combination, required no-skip native interoperability, package/AOT consumers and proof/trust checks. Both fuzz and portability campaigns must successfully execute that same revision before publication, including when an automatic campaign skipped after content selection. The publication preflight verifies main ancestry, the exact CI revision, complete campaign job inventories and run-attempt-bound evidence receipts. Missing, pending, failed, skipped or expired evidence refuses publication. A known defect that loses accepted values, corrupts indexes or bypasses required refusal/rollback blocks release; passing unrelated jobs does not waive it.

Missing native fixtures, physical power-loss qualification and incomplete full-path refinement stay explicit in the plan and evidence map. They must be resolved before broadening the corresponding contract, rather than being accepted through a new proof assumption or an ambiguous feature checkmark. Publishing still requires explicit authorization under [PUBLISH.md](../PUBLISH.md).
