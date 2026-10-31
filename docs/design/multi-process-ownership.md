# Cooperating process ownership model

[MultiProcessOwnership.dfy](../../verification/Transactions/MultiProcessOwnership.dfy) models arbitrarily many cooperating clients of one canonical database identity. Each client has its own acquisition queue, phase, cached namespace observations and acquisition ticket. The shared state contains native and receipt evidence, the exclusive coordinator and the sets of previously used tickets and generations. This is an unbounded verification-only model; no code is extracted.

`Coordinate` is an assumed atomic exclusive-coordination operation, rather than a proof that a particular collection of .NET handles implements that operation. Every preparation, recovery-control and deletion operation requires the currently held client/ticket pair. Client IDs may restart; acquisition tickets and generation tokens are not reused. Queuing, observation, waiting, crash and restart are scheduler/process events. Their action ticket carries no authority.

Clients can queue and observe while another client is preparing, replaying or cleaning up. Observations can become stale, including observing both ownership names absent while the previous client still holds the cleanup scope. A client crash releases only that client's coordination, preserves surviving evidence and leaves other clients' local state intact. Restart clears cached observations and must acquire a fresh ticket. Failed operations can release coordination while retaining partial or complete evidence for the next attempt. These are process-failure schedules with quiescent old I/O; they do not characterize power-loss survivors or delayed syscalls.

## Checked obligations

| Theorem | Property |
|---|---|
| `StepPreserves`, `TracePreserves` | All enabled interleavings preserve phase/evidence consistency, current-ticket authority and historical ticket/generation sets. The trace theorem covers every intermediate state of any finite modeled trace. |
| `AtMostOneCoordinatedClient` | Two clients cannot simultaneously hold acquisition, recovery or cleanup scopes. |
| `MutationRequiresCurrentTicket` | Every change to either shared ownership name requires the acting client's current coordination ticket. |
| `MissingNamesDoNotAuthorizeSuccessor` | A waiting client cannot acquire coordination during the previous client's cleanup, even when both native and receipt names are absent. |
| `ObservationDoesNotGrantAuthority`, `StaleTicketCannotAct`, `UsedTicketCannotBeReacquired`, `CrashRevokesAllEffectAuthority` | A cached pathname snapshot cannot grant authority. Used tickets cannot be reacquired; a crash revokes every effect operation, and an earlier ticket cannot act after the same client acquires a newer ticket. |
| `IncompleteBootstrapRefuses`, `CrashRetainsEvidence`, `FailedOperationRetainsEvidence` | Partial or native-only evidence cannot authorize fresh/recorded bootstrap. Process failure and failed-operation release retain surviving evidence. A waiter's crash cannot release another client's coordination. |
| `FreshAcquisitionCompletes` | A fresh coordinated generation constructs complete native/receipt authority in four successful abstract operations. |
| `RecordedAcquisitionCompletes` | An exact surviving receipt with a matching or absent native name constructs an active generation in one or two successful abstract operations. |
| `SuccessfulCleanupCompletes` | An active generation has a five-operation successful recovery/cleanup schedule: begin replay, complete selected-image validation/flush, remove native, remove receipt, release coordination. |
| `EarlyCoordinatorReleaseAllowsOverlappingScopes` | A checked counterexample shows that admitting a successor before the prior cleanup scope ends violates exclusion, although both names are absent. |
| `StaleCleanupCounterexample` | A checked counterexample shows that pathname deletion using an old observation would erase a successor's receipt; the admitted operation refuses that stale cleanup. |

The completion lemmas construct successful schedules. They establish neither fairness nor termination during infinitely many crashes, failed flushes or refused coordination attempts. `Replay` may repeat without progress; `CompleteRecovery` is permitted only as the named successful selected-image validation and destination-flush effect. Image contents and that effect's implementation remain contracts.

## Production correspondence and limits

The production references are under `src/JetDatabaseWriter/`.

| Model boundary | Production point | Remaining contract |
|---|---|---|
| Canonical coordinator | `Encryption/EncryptionReplacementIntent.cs` `Recover`: exclusive intent acquisition, observed destination acquisition, validation, `AcquireRecovery`, publication and final destination retention | Exact exclusive coordination is assumed. The destination may initially be absent; retained intent/native/source leases precede the published database handle. This composite acquisition is not proved equivalent to the model's atomic `Coordinate`. Path aliasing, OS sharing/locks and cooperating-client rules need independent qualification. |
| Fresh staged evidence | `Transactions/RecoverableWriterLock.cs` `AcquireRecovery`: native creation/write/flush, receipt creation/write/flush | `Partial` denotes evidence not admitted as exact. `Exact` requires complete validated path/generation bytes and the named successful fence. SHA/GUID/receipt parsing, returned I/O effects and directory persistence remain unproved. |
| Recorded bootstrap | `AcquireRecovery`: exact receipt validation, matching native validation or absent-native reconstruction | The model rechecks shared evidence under coordination; it never authorizes bootstrap from a client's cached observation. Real reads, exclusive native/receipt handles and the exact persisted survivor are contracts. |
| Replay completion | `EncryptionReplacementIntent.Cleanup`: selected-image hash/admission and `database.Flush(true)` before ownership cleanup | `CompleteRecovery` is a successful-effect contract, with selected bytes and the actual flush outside this model. Failed effects cannot select this transition. Candidate copies and intent-name cleanup are covered by the separate replacement model. |
| Native/receipt cleanup | `RecoverableWriterLock.RecoveryLease.Cleanup`: close native, delete native, close receipt, delete receipt | These cleanup transitions apply after a selected destination exists and while the caller retains its database coordination. Closing an individual name handle does not release the modeled coordinator. Final `Release` occurs only after the caller's coordinated cleanup scope ends, including intent deletion where applicable. |
| Stale owner disposal | `RecoverableWriterLock.Dispose` and the existing `GenerationOwnership` model | Ordinary owner disposal can run after the physical database store closes and retains both native/receipt handles through deletion. That held-handle schedule is outside this model; it must not be substituted for the retained-database recovery-cleanup schedule. |

Existing focused production evidence includes `ReplacementCleanupConformanceTests.Recover_OwnershipCleanupCuts_RetainSelectedImageAndExcludeSuccessor`, which observes removed names, attempts a competing database open, interrupts each ownership cleanup boundary and retries recovery. `RecoverableWriterLockTests` separately checks invalid/partial receipts, native-only bootstrap refusal, exact receipt reconstruction, live-holder refusal and completed stale disposal. These finite same-process competing-handle traces supplement the abstract model; they do not establish actual cross-process or macOS exclusion, a lock-rank/deadlock proof, power-loss durability or arbitrary adversarial namespace behavior.

The assumptions remain: one canonical identity per database, cooperating clients, fresh generation/ticket tokens, validated evidence, successful fences where named, exclusive coordination and crash quiescence. The model permits safe refusal after incomplete bootstrap. It makes no native Access recovery requirement and does not change the managed runtime or shipping dependency graph.
