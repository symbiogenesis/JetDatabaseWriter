# GitHub workflows

The required [CI workflow](../.github/workflows/ci.yml) owns ordinary validation. Other workflows provide release publication, optional evidence or developer artifacts. They remain separate where their inputs, prerequisites or execution costs differ.

| Workflow | Trigger | Responsibility |
|---|---|---|
| [CI](../.github/workflows/ci.yml) | Main pushes, pull requests, manual dispatch and release calls | Common script/security/proof-review checks and the aggregate `CI gate`; content-selected Release analyzers, complete Windows suites, pinned no-skip DAO120 compatibility against matching analyzed runner artifacts, proofs, package validation, generated consumers and focused Linux/macOS recovery checks. |
| [Publish NuGet Package](../.github/workflows/publish-nuget.yml) | `v*` tags | Calls full CI, verifies published-main ancestry and exact-revision CI/fuzz/portability evidence, then publishes its exact package artifact from a bounded Linux job. |
| [Benchmarks](../.github/workflows/benchmarks.yml) | Manual, main only | Hosted measurements, with optional validated baseline and controlled comparisons. Requires successful CI for each measured revision. |
| [Adversarial fuzz](../.github/workflows/fuzz.yml) | Successful main CI and manual main dispatch | Bounded mutation cases on .NET 10 and 11, with retained reproductions and coverage. Requires successful exact-revision CI. |
| [Portability](../.github/workflows/portability.yml) | Successful main CI and manual main dispatch | All applicable Linux/macOS tests on .NET 10 and 11, with xUnit and golden diagnostics. Requires successful exact-revision CI, which already supplies Windows coverage and strict analyzers. |
| [InferSharp](../.github/workflows/infersharp.yml) | Manual | Independent analysis of both library assets with pinned tooling, reviewed findings and retained translation diagnostics. |
| [Calculated native profiles](../.github/workflows/calculated-native-profiles.yml) | Manual, main only | Independent Access Runtime controls under six regional profiles on disposable Windows hosts. Requires successful exact-revision CI. |
| [Maintenance](../.github/workflows/maintenance.yml) | Manual, main only | Candidate compiler/analyzer feedback or an SDK lock refresh; returns reviewable artifacts without publishing Git refs. |

Ordinary partition results are checked from xUnit XML: failures, infrastructure errors, unknown skip reasons and unapproved explicit not-run identities fail acceptance. The native inventory additionally requires every named method to pass, verifies the actual loaded ACEDAO DLL version and permits no skips. Its scope is DAO120; native Jet3/DAO3.6 and unlisted feature variants remain separate evidence obligations.

Both package-producing paths run `scripts/test-package.ps1` before artifact upload. It checks complete packaged licenses and documentation, then restores the actual archives into isolated core native/Standard consumers and EF provider consumers on .NET 10 and .NET 11. Core consumers check indexed create/read/update behavior and embedded resources; EF consumers reference only the provider package and verify transitive core asset selection, database creation, saving, querying and reopening. These controls complement the project-reference tests and generated AOT consumers.

CI selects expensive lanes against validated main ancestors, retaining unconditional checks and the aggregate gate even for documentation changes. Manual CI and release calls validate every lane. Specialized workflows add evidence after that gate; their results do not replace it. See [AGENTS.md](../AGENTS.md#gating-a-commit-on-ci) for the acceptance and reporting rules.

Native compatibility can still execute after self-test failures when its analyzed
artifacts exist. This preserves independent evidence for diagnosing golden-byte
or runtime failures; the aggregate gate still requires every selected lane to
succeed. Cancellation stops superseded native runs.

Two strict Release test builds produce the native and standard runner artifacts. Fifteen runtime/partition jobs and the three native compatibility jobs download those exact outputs; they do not repeat compiler or analyzer work. Automatic fuzz and portability runs bind checkout and prerequisite validation to the successful CI run SHA, rather than the current main tip. The exact run selection receipt must select .NET validation; documentation/proof-only runs skip those expensive campaigns, while manual dispatch remains explicit. Fuzz coverage receipts name the validated SHA and refuse a mismatched checkout; a workflow_run trigger's default-branch SHA cannot label that evidence. Guards accept the exact CI workflow path with its supported manual ref suffix and retain same-repository, main-branch and successful-event checks.

## Developer maintenance

Successful fuzz and portability matrices each produce a final `Record release evidence` job. It checks the actual Git checkout and retains a small receipt bound to the repository, workflow, revision, run ID and run attempt for 30 days. Campaign titles identify their target revision even while pending, because `workflow_run` metadata can name a newer default-branch revision. Publication selects the newest matching official campaign, requires its complete successful job inventory, and validates the bounded receipt archive. A newer failed or pending run cannot fall back to older success. Skipped automatic campaigns supply no receipt; release acceptance requires explicit successful campaigns for the release SHA. Portability also validates xUnit outcomes before recording success.

Six generic process-runner cases execute with Windows PowerShell on Windows and `pwsh` plus POSIX `sleep` on Linux/macOS. Only the three methods explicitly inventoried with `Platform=Windows` are excluded on non-Windows hosts: they test process DACLs and restricted-token termination, which are Windows APIs. Each Windows ordinary partition requires those exact methods to pass, and coverage regressions reject missing requirements, broader exclusions or additional unreviewed platform traits. The focused Linux/macOS main-CI jobs also run the six portable cases. Those focused jobs require nonempty xUnit results without any skips and retain their reports for each operating system and runtime, including after failures. Unexpected selected-test skips still refuse validation.

For candidate checks, stage new files and use the helper, which supplies the task, immutable base SHA and encoded patch:

```powershell
pwsh -NoProfile -File scripts/submit-candidate.ps1 -Base origin/main
```

For a restore-only SDK update, supply an exact installed release version:

```powershell
gh workflow run maintenance.yml --ref main -f task=sdk-lock-refresh -f sdk-version=11.0.100-rc.3.26522.148
```

The workflow validates the selected task's inputs before checkout or SDK setup. Candidate checks preserve the trusted main coordinator before applying the patch; SDK refresh runs the restore-only updater. Review the returned patch before integrating it, then require ordinary main CI.

## Manual evidence

BenchmarkDotNet's generated-project builds skip repeated analyzer execution in referenced projects after the required exact-revision CI has analyzed them. Compiler checks and source generators still run, and the generated builds retain a ten-minute timeout. The measurement jobs retain their default adaptive warmup and iteration policy; a build timeout still fails the measurement and retains its diagnostics.

Dispatch these after successful CI for the settled main revision:

```powershell
gh workflow run benchmarks.yml --ref main -f filter="*AccessWriterBenchmarks*" -f job=default
gh workflow run fuzz.yml --ref main -f cases=64 -f seed=20261015
gh workflow run portability.yml --ref main
gh workflow run calculated-native-profiles.yml --ref main
```

InferSharp is independent and can be dispatched with `gh workflow run infersharp.yml --ref main`. Serialized manual workflows retain queued requests; ordinary branch CI cancels superseded revisions. Every hosted job has a timeout, actions are pinned to commits, repository permissions are read-only and checkouts do not persist credentials. Release publication receives its NuGet secret only in the push step.
