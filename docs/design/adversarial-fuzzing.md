# Adversarial fuzzing

The explicit reader, writer and delimited-text harnesses run only through the
hosted [fuzz workflow](../../.github/workflows/fuzz.yml), after ordinary CI has
passed for the exact main revision. Ordinary regression tests exercise the
harness contracts and the coordinator with synthetic inputs and process doubles;
they do not launch a fuzz campaign.

## Inputs and coverage guidance

The coordinator selects bounded database fixtures from the checked-in corpus.
It schedules structural mutation families across the available JET/ACE formats
before spending additional cases on random bytes and truncation. Mutations use
known page and descriptor fields: header metadata, data row counts and pointers,
table-definition counts and pointers, index metadata, usage maps and encryption
metadata. Boundary values and out-of-file references complement random
changes. The original fixture hash, mutation location and value, seed and final
input hash identify each case independently of the order processes finish.
Generated writer dates use a fixed epoch so replay does not depend on the clock.

Mutation selection and parser reachability are separate signals. The coverage
artifact records which structural groups exist in the corpus, which were selected
and which actually ran. Harness operation markers record the public surfaces
reached by those inputs. A mutation at an index field that fails during open does
not demonstrate index traversal. Review both signals when choosing the next seed,
fixture or case budget; preserve healthy controls so deeper operations remain
reachable. Missing groups require an appropriate fixture or a larger campaign.
These counters guide subsequent campaigns; they are not instrumented branch
coverage or a claim that every path is safe.

The reader exercises catalog discovery, column metadata, index listing, indexed
seeks and bounded row reads. Known fixture passwords exercise encryption parsing
and key derivation under a shared work budget. Linked-source authorization is
denied, including transitive reads, so database content cannot grant access to a
local file or external service. Ordinary regression tests establish that the deny
policy is actually invoked before opening a source.

The writer uses transactional writes. Attempted operations and their success or
refusal have separate markers. Every accepted input rejection compares the
entire database image with its pre-operation snapshot. This covers schema and row
mutations and explicit rollback; a rejection that changes bytes fails the harness.
Successful output must reopen with its original password and complete readback.
Round-trip corruption and assertion failures are never accepted as input errors.

## Resource and failure contracts

Each input runs in a separate process with a wall-clock timeout and memory limit.
The coordinator kills the entire process tree on a resource violation. The reader
also limits tables, indexes, rows, long values, attachments, schema definitions and
encryption work, and passes cancellation through public operations. Unexpected
programming exceptions escape the harness; documented malformed-input,
unsupported-format, authorization and resource refusals are recorded explicitly.
The process result must contain exactly one passing test; missing, malformed or
stale result files cannot pass a case.

The driver retains the original failure input and performs bounded reduction.
Candidates must reproduce the same failure signature before replacing the current
reproducer. Reduction has both attempt and time budgets; a retained result is a
reproducer found within those budgets; it can remain unchanged and is not a
proof of global minimality.
Managed failure signatures include the exception type and stack frames; a raw
process abort without result XML has only its exit-code signature. The original
input remains the authoritative evidence in either case.
Crashes, timeouts and memory failures remain failures even when no reduction is
possible. Logs, result XML, hashes and the manifest remain available for diagnosis.

## Running and retaining a campaign

Publish the settled main revision and require its ordinary CI gate to succeed.
Then dispatch on main, for example:

```powershell
gh workflow run fuzz.yml --ref main -f cases=64 -f seed=20261015
```

Verify the run's head SHA and completed verdict on both framework legs. The
`fuzz-net10.0` and `fuzz-net8.0` artifacts contain the per-case manifests, coverage
signals, logs and any retained failure inputs. Download useful evidence before
artifact retention expires. A successful campaign establishes its listed cases
under its budgets; it does not establish native interoperability or complete
hostile-file auditing.

For a failure, use the manifest's method, seed and retained input to prepare a focused
hosted reproduction. Preserve the exception/signature and move a minimized
trigger into an ordinary targeted regression before fixing production code. Do
not weaken the accepted exception set, raise budgets or delete the trigger merely
to obtain a green campaign. New parser surfaces should add both a structural
mutation target where applicable and an operation marker, plus a healthy control
that reaches the operation.
