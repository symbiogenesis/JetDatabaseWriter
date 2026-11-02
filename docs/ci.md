# GitHub workflows

The required [CI workflow](../.github/workflows/ci.yml) owns ordinary validation. Other workflows provide release publication, optional evidence or developer artifacts. They remain separate where their inputs, prerequisites or execution costs differ.

| Workflow | Trigger | Responsibility |
|---|---|---|
| [CI](../.github/workflows/ci.yml) | Main pushes, pull requests, manual dispatch and release calls | Common script/security/proof-review checks and the aggregate `CI gate`; content-selected Release analyzers, complete Windows suites, pinned no-skip DAO120 compatibility against matching analyzed runner artifacts, proofs, package validation, generated consumers and focused Linux/macOS recovery checks. |
| [Publish NuGet Package](../.github/workflows/publish.yml) | `v*` tags | Calls full CI, then publishes its exact package artifact from a bounded Linux job. |
| [Benchmarks](../.github/workflows/benchmarks.yml) | Manual, main only | Hosted measurements, with optional validated baseline and controlled comparisons. Requires successful CI for each measured revision. |
| [Adversarial fuzz](../.github/workflows/fuzz.yml) | Successful main CI and manual main dispatch | Bounded mutation cases on .NET 10 and 11, with retained reproductions and coverage. Requires successful exact-revision CI. |
| [Portability](../.github/workflows/portability.yml) | Successful main CI and manual main dispatch | Complete Linux/macOS suites on .NET 10 and 11, with xUnit and golden diagnostics. Requires successful exact-revision CI, which already supplies Windows coverage and strict analyzers. |
| [InferSharp](../.github/workflows/infersharp.yml) | Manual | Independent analysis of both library assets with pinned tooling, reviewed findings and retained translation diagnostics. |
| [Calculated native profiles](../.github/workflows/calculated-native-profiles.yml) | Manual, main only | Independent Access Runtime controls under six regional profiles on disposable Windows hosts. Requires successful exact-revision CI. |
| [Maintenance](../.github/workflows/maintenance.yml) | Manual, main only | Candidate compiler/analyzer feedback or an SDK lock refresh; returns reviewable artifacts without publishing Git refs. |

Ordinary partition results are checked from xUnit XML: failures, infrastructure errors, unknown skip reasons and unapproved explicit not-run identities fail acceptance. The native inventory additionally requires every named method to pass, verifies the actual loaded ACEDAO DLL version and permits no skips. Its scope is DAO120; native Jet3/DAO3.6 and unlisted feature variants remain separate evidence obligations.

Both package-producing paths run `scripts/test-package.ps1` before artifact upload. It checks complete packaged licenses and documentation, then restores the actual archive into isolated native and Standard consumers. Their indexed create/read/update controls and embedded-resource checks complement the project-reference tests and generated AOT consumers.

CI selects expensive lanes against validated main ancestors, retaining unconditional checks and the aggregate gate even for documentation changes. Manual CI and release calls validate every lane. Specialized workflows add evidence after that gate; their results do not replace it. See [AGENTS.md](../AGENTS.md#gating-a-commit-on-ci) for the acceptance and reporting rules.

Native compatibility can still execute after self-test failures when its analyzed
artifacts exist. This preserves independent evidence for diagnosing golden-byte
or runtime failures; the aggregate gate still requires every selected lane to
succeed. Cancellation stops superseded native runs.

Two strict Release test builds produce the native and standard runner artifacts. Fifteen runtime/partition jobs and the three native compatibility jobs download those exact outputs; they do not repeat compiler or analyzer work. Automatic fuzz and portability runs bind checkout and prerequisite validation to the successful CI run SHA, rather than the current main tip. The exact run selection receipt must select .NET validation; documentation/proof-only runs skip those expensive campaigns, while manual dispatch remains explicit. Fuzz coverage receipts name the validated SHA and refuse a mismatched checkout; a workflow_run trigger's default-branch SHA cannot label that evidence. Guards accept the exact CI workflow path with its supported manual ref suffix and retain same-repository, main-branch and successful-event checks.

## Developer maintenance

For candidate checks, stage new files and use the helper, which supplies the task, immutable base SHA and encoded patch:

```powershell
pwsh -NoProfile -File scripts/submit-candidate.ps1 -Base origin/main
```

For a restore-only SDK update, supply an exact installed release version:

```powershell
gh workflow run maintenance.yml --ref main -f task=sdk-lock-refresh -f sdk-version=10.0.403
```

The workflow validates the selected task's inputs before checkout or SDK setup. Candidate checks preserve the trusted main coordinator before applying the patch; SDK refresh runs the restore-only updater. Review the returned patch before integrating it, then require ordinary main CI.

## Manual evidence

Dispatch these after successful CI for the settled main revision:

```powershell
gh workflow run benchmarks.yml --ref main -f filter="*AccessWriterBenchmarks*" -f job=default
gh workflow run fuzz.yml --ref main -f cases=64 -f seed=20261015
gh workflow run portability.yml --ref main
gh workflow run calculated-native-profiles.yml --ref main
```

InferSharp is independent and can be dispatched with `gh workflow run infersharp.yml --ref main`. Serialized manual workflows retain queued requests; ordinary branch CI cancels superseded revisions. Every hosted job has a timeout, actions are pinned to commits, repository permissions are read-only and checkouts do not persist credentials. Release publication receives its NuGet secret only in the push step.
