# AGENTS.md

Instructions for coding agents working in this repository. They apply to every session and every subagent; the people who work here follow them too.

Reference material, read when a task needs it:

- [docs/glossary.md](docs/glossary.md): Access/JET acronyms, constants and on-disk terms used in the code, docs and comments.
- [docs/design/](docs/design/): format notes for pages, rows, indexes, relationships, long values, complex columns, encryption and calculated columns.
- [PLAN.md](PLAN.md): the source of truth for all outstanding project work, dependencies and acceptance criteria, including formal verification.
- [PUBLISH.md](PUBLISH.md): how a release is cut and published.

## The project

JetDatabaseWriter reads and writes Microsoft Access databases in pure .NET: Jet3 (Access 97) and Jet4 `.mdb` files, and ACE `.accdb` files. The solution file is `JetDatabaseWriter.slnx`, and `global.json` pins the .NET 10 SDK.

| Project path | Targets | Role |
|---|---|---|
| `src/JetDatabaseWriter` | netstandard2.1, net10.0 | The library and NuGet package |
| `tests/JetDatabaseWriter.Tests` | net10.0 | xUnit v3 on Microsoft Testing Platform. CI runs on .NET 10 and .NET 11; a separate .NET 10 lane loads the netstandard2.1 library |
| `tests/JetDatabaseWriter.TestSupport` | net10.0 | Non-packable helpers that the tests and FormatProbe share |
| `benchmarks/JetDatabaseWriter.Benchmarks` | net10.0 | BenchmarkDotNet |
| `tools/JetDatabaseWriter.FormatProbe` | net10.0 | Diagnostics against real Access files |
| `src/JetDatabaseWriter.Scaffold` | net10.0 | Entity scaffolding tool |

Version 4.0.0 has never been released. The tags v1.0.0 to v2.2.0 are upstream history and must never be pushed, and MinVer takes the version from `v`-prefixed tags. Break APIs freely: add no compatibility shims, `[Obsolete]` wrappers, flags kept only for old behaviour, migration notes, or "breaking change" callouts in the README.

When a design decision is left to you, choose what Microsoft Access does, then the safest option, and state the choice briefly.

## Local execution and CI

- Local builds are allowed, including full solution builds and Release analyzer checks.
- Specific tests may run locally, including ordinary regression tests and DAO tests. Select the relevant methods, classes, or a focused namespace; a framework filter alone or excluding fuzz tests still runs a full test leg.
- Full test suites run only on CI, including a full run of just one target framework. Do not run the whole suite locally in batches of filtered tests.
- All benchmarks run only on CI, including a single benchmark, short/medium/dry jobs, smoke checks, and debugging runs. Building the benchmark project locally is allowed; executing benchmarks is not.
- Local builds and targeted tests provide development feedback. Required acceptance gates still run on CI against the settled `main` revision.
- For compiler and analyzer feedback without a local build, stage new files and run `pwsh -NoProfile -File scripts/submit-candidate.ps1 -Base origin/main`. This selects `task=candidate` in the main-only `maintenance.yml` workflow, which applies the unpublished patch, installs its SDK, compiles all solution targets, formats changed source and retains a reviewable patch plus strict build diagnostics. Review and apply formatter fixes before integrating; candidate success does not replace the ordinary main CI gate. Oversized workflow inputs are refused before dispatch.
- Windows test analyzer lanes execute their exact Release binaries with `dotnet test --test-modules` immediately after their own build, without restoring or building again. Every selected .NET run requires all five test partitions in each of the three runtime/library combinations and every analyzer gate. The ordinary partition excludes the Schema, RoundTrip and Writer namespaces; the schema-writer partition includes those namespaces except the three format-specific statement fault classes; each format partition includes its complete matching class. This retains every test and fault position once per runtime/library combination, including future tests through the ordinary complement. The ordinary lanes upload the complete analyzed runner for focused DAO checks; all partitions upload xUnit results with individual test durations and separate golden diagnostics.
- Specialized benchmark, fuzz and portability workflows require a completed, successful `ci.yml` run for the exact main revision before their build jobs start. Finish that gate before dispatching them; no additional local build is required.

## Build

- `dotnet build JetDatabaseWriter.slnx -c Release` must report 0 warnings and 0 errors. `Directory.Build.props` turns on `TreatWarningsAsErrors`, `AnalysisLevel` latest-all, `EnforceCodeStyleInBuild`, XML documentation and nullable reference types for every project. CI analyzes every project and target framework in Release, so an analyzer finding in test code fails it too.
- The analyzers take most of a Release build. `-p:AnalyzeOnly=<project names>` analyzes only the named projects and `-p:AnalyzeExcept=<project names>` every project but those (names separated by spaces); the other projects compile without analyzers, and source generators run either way. CI's analyze jobs use them, and `dotnet build tests/JetDatabaseWriter.Tests -c Release -f net10.0 -p:AnalyzeOnly=JetDatabaseWriter.Tests` checks test code alone.
- The tools analyzer lane builds `.github/ci-tools.slnf`, covering every solution project except the separately analyzed library and tests; project references still compile the library. Keep the filter complete when adding projects. Do not compile the full test runner in that lane.
- `JetDatabaseWriter.Tests` turns off `RunAnalyzersDuringBuild`, `EnforceCodeStyleInBuild` and `GenerateDocumentationFile` in non-Release builds, for speed; keep Release strict. Its `NoWarn` holds only `CA1707` and `SA1615`, and nothing else may be added. Fix any other finding in test code. Where a rule works against what a test checks, suppress it at the site with `#pragma warning disable <id> // <reason>`, for example CA1812 on a POCO that only reflection, `Rows<T>` or an expression tree creates.
- `JetDatabaseWriter.FormatProbe` does not turn off its analyzers.
- CI restores with `--locked-mode`. After changing a package reference or a target framework, restore once without it and commit the updated `packages.lock.json` files.
- Update the SDK with `pwsh -NoProfile -File scripts/update-sdk.ps1 -Version <exact version>` after installing it, or dispatch `maintenance.yml` on `main` with `task=sdk-lock-refresh` and `sdk-version=<exact version>` for a restore-only patch artifact. Commit `global.json` and every changed solution/smoke lock together. The updater rolls back file changes on restore failure; `scripts/restore-dependencies.ps1` verifies the solution and all seed, trimmed and native locks without compiling. AOT CI verifies all smoke locks before any consumer build.

## Gating a commit on CI

See [the workflow guide](docs/ci.md) for workflow responsibilities and manual dispatch commands.

Gate every commit using the validation selected for its contents by GitHub CI. Local builds and targeted test runs are allowed during development, but do not replace that gate. Documentation and standalone tooling changes keep the lightweight script/security/proof-review checks and aggregate gate; they do not require unrelated builds, tests, AOT consumers or proofs. Several agents often share one machine, so full test suites and all benchmark runs stay on CI. Report gate results from CI.

`ci.yml` runs on every push to `main` and every pull request. Keep content selection inside the workflow: trigger-level `paths` or `paths-ignore` filters would suppress the required gate and common checks. Integrate the reviewed work into local `main`, then publish only that branch:

```
git -c push.followTags=false push origin main:main
gh run list --workflow ci.yml --branch main --commit <commit>
```

Record the run URL, exact tested SHA and verdict. Inspect the lane-selection receipt, unconditional script/security/proof-review checks, selected jobs and aggregate `CI gate`. The expensive jobs are Dafny recovery-model verification/trust audit, seventeen Windows Release analyzer/package jobs (fifteen execute the complete ordinary suites across five partitions per runtime/library combination), Windows generated trimmed/NativeAOT consumers, and focused encryption replacement tests on Linux and macOS on .NET 10 and .NET 11. A separate documentation package job builds and packs only the Release library when packaged prose changes without selecting full .NET validation. Do not treat a queued or running workflow as passed. A timing-sensitive failure may be retried once with `gh run rerun <id> --failed` before diagnosing it as reproducible.

Selection in `scripts/select-ci-lanes.ps1` follows file roles and dependencies, including prose inside project and fixture directories:

| Changed content | Required validation beyond the common checks |
|---|---|
| Ordinary Markdown in known documentation/project directories, root instructions/plans and license prose | None |
| Root `README.md` or `THIRD-PARTY-NOTICES.md` shipped in NuGet | Documentation package job; full .NET validation already includes packing |
| Explicitly allowlisted standalone script helpers and ordinary script regressions | None; the common script checks execute their regressions |
| .NET source, tests, binary/data fixtures, generated consumer inputs, AOT/restore tooling or the fixture script copied into the test runner | All .NET analyzer, test, package, AOT and platform jobs |
| Proofs, proof/trust inputs and designated design documents, proof tooling, selection/review tooling or their coverage regressions | Dafny |
| Generated shipping verification code | All .NET jobs and Dafny |
| Shared build/workflow configuration or unknown paths/content types | All .NET jobs and Dafny |

Mixed changes take the union of these requirements. Recognize proof/trust documents before ordinary prose; a document fixture such as `.docx` remains a test input, and a source-comment edit remains a source change. Do not ignore an entire `docs/` or `scripts/` directory. Keep standalone helper exceptions explicit and the package/copy-input regressions synchronized with project declarations when adding dependencies.

Push and pull-request runs compare the complete Git tree against a validated main ancestor for each expensive lane, using only the newest matching run for each candidate SHA. A completed failed overall run can supply a lane baseline only when every job in that lane and both common checks passed; missing, duplicate or incomplete required job inventories and unsuccessful unknown jobs refuse inheritance. A skipped documentation package job is expected when full .NET validation already packed that tree. The selection receipt records each ancestor's SHA, run URL and evidence type, the complete changed paths and the documentation package decision. Packaging uses the .NET ancestor. This retains unvalidated changes across overlapping or failed pushes; never select lanes using only the immediately preceding commit. Handwritten C# changes receive unconditional proof-review checks. Unchanged lanes inherit their validated ancestor; missing history/API evidence runs the affected lane. Manual dispatch and release calls run everything; use ordinary push/PR selection for documentation-only work. Keep the content-selection, workflow coverage and aggregate-result regressions intact when modifying these rules.

New runs cancel superseded branch validation so the settled tip does not wait behind obsolete work; release runs are preserved. Cancellation is never a passing gate or an eligible inheritance baseline. If an exact older revision is needed for specialized evidence, rerun its cancelled CI and require completion before dispatching that evidence.

InferSharp runs independently in `.github/workflows/infersharp.yml` through manual dispatch only (`gh workflow run infersharp.yml --ref main`). Its two Linux jobs analyze both library targets with v1.5; ordinary CI does not depend on them. Unreviewed findings fail that workflow; inspect the `infersharp-<framework>` text, SARIF and translation-coverage artifacts. The exact false-positive exceptions in `.github/infersharp-baseline.json` require source review; never regenerate them merely to accept output. Source hashes and stale exceptions are checked by the gate. For changes to InferSharp, record its separate run URL, tested SHA and verdict.

For a completed run, the reporting mode of the existing helper prints every step result, build summaries, every test partition summary and failing-test messages without pushing or dispatching:

```
pwsh -NoProfile -File scripts/ci-gate.ps1 -RunId <id>
```

The helper never publishes or deletes Git refs. After publishing `main`, `-Sha <commit>` finds or dispatches its CI run and verifies the published main tip; `-Benchmarks`, `-Baseline` and `-Job` select hosted benchmark measurements. It checks once by default; exit code 2 means pending. Use `-WaitSeconds` only when bounded waiting is needed. Alternatively, dispatch `gh workflow run ci.yml --ref main` and verify the resulting run's SHA. Keep work branches local and remove this session's eligible worktrees and branches after their changes are integrated and validated, retaining useful outputs.

## Formal models

`verification/` contains Dafny models and the source of the small executable recovery kernel. Only its reviewed generated C# enters the library; Dafny, its solver and runtime support are not shipping dependencies. Follow [the proof tooling and trust policy](verification/README.md), keep every `.dfy` and its normal-source dependencies in the exact manifest, and run `scripts/verify-proofs.ps1` with the integrity-pinned toolchain after changing proofs, generated code or proof tooling. The default checks generated freshness; use `-UpdateGeneratedSource` to regenerate after reviewing a source change. Do not add assumptions, admitted lemmas, unchecked external contracts or disabled verification. Changes to mapped recovery code require review of [the model boundary](docs/design/formal-verification.md); the mapping alone is not a C# refinement proof. Keep [the formal verification workstream](PLAN.md#v--formal-verification-and-stronger-guarantees) open-work-only. When selected, the required `ci.yml` formal job verifies the proofs, trust audit, negative controls and generated freshness independently of ordinary builds; review-record freshness remains unconditional.

## Tests

Expected results in all three ordinary test lanes:

- Nothing fails.
- Every skipped test is a skip-guarded DAO test ("Requires Microsoft Access (DAO.DBEngine.120)"). Microsoft Access is absent on hosted CI, so those cases skip there. Selected DAO tests can run locally on an Access-equipped host using locally built binaries or CI-built binaries from the matching revision; engine-specific skips remain evidence gaps.
- 3 explicit-only fuzz tests are not run.

The Microsoft Testing Platform summary counts guarded DAO cases and the three explicit-only fuzz harnesses together as skipped. Check the skip reasons against the current guarded cases; any other skip is a regression.

For a quick local loop, build in Release and run a selected test: `dotnet test --project tests/JetDatabaseWriter.Tests -c Release -f net10.0 --filter-method "<Namespace.Class.Method>"`. To use an existing build directly, run `tests/JetDatabaseWriter.Tests/bin/Release/<tf>/JetDatabaseWriter.Tests.exe -method "<Namespace.Class.Method>" -longRunning 300`. Always include a focused test selection for local execution; unfiltered test runs belong on CI.

xUnit v3 on Microsoft Testing Platform:

- Use the stable xUnit v3 framework (`xunit.v3`); the current stable 4.x package supports per-test parallelism. Do not use prerelease packages. Test projects are executables: keep `<OutputType>Exe</OutputType>`.
- Use `using Xunit;`. Do not add `Xunit.Abstractions`, `xunit.runner.visualstudio`, `xunit.abstractions` or `xunit.assert`.
- The base command is `dotnet test --project tests/JetDatabaseWriter.Tests`; locally, add a focused inclusion filter such as `--filter-method` or `--filter-class`. The unfiltered command is for CI only. Pass Microsoft Testing Platform options straight to `dotnet test`, with no `--` separator. Do not use VSTest filters (`--filter "FullyQualifiedName~..."`) or `--nologo`; use `--verbosity quiet` for quieter output.
  - One target framework: `-f net10.0`.
  - One method: `--filter-method "<Namespace.Class.Method>"`.
  - One class: `--filter-class "<Namespace.Class>"`.
  - One namespace: `--filter-namespace "<Namespace>"`.
  - Exclusions: `--filter-not-class`, `--filter-not-method` and `--filter-not-namespace`.
  - Traits: `--filter-trait Category=Fuzz` and `--filter-not-trait Category=Fuzz`.
  - Several values go after one switch, separated by spaces (`--filter-class Foo Bar`).
  - Other switches: `--list-tests`, `--stop-on-fail on`, and `-?`.
- Fuzz harnesses are `[Fact(Explicit = true)]` with `[Trait("Category", "Fuzz")]`. Run them only on purpose: `dotnet test --project tests/JetDatabaseWriter.Tests --filter-trait Category=Fuzz --explicit only`.
- Test code compiles for net10.0. CI runs the native library on .NET 10 and .NET 11, and the netstandard2.1 library on .NET 10 using `-p:TestNetStandard=true`. `LibraryTarget.IsNetStandard` reports the loaded asset. The standard lane excludes scaffolding tests. The root SDK pin stays on .NET 10; `.github/dotnet11/global.json` pins the CI-only runtime installation.
- Helpers that the tests and FormatProbe share go in `JetDatabaseWriter.TestSupport`, never in the library. Run child processes through `PowerShellProcessRunner`, not by reading a redirected stream to its end before `WaitForExit`, which ignores the timeout.
- Crash recovery children use the allowlisted `--jdw-crash-child` entry point, executing the original scenario with the schema-cache verification fixture before any suite discovery. Preserve the ordinary xUnit/MTP entry point, exact shared-runtime pin, readiness/phase markers and forced-termination exit checks. Unique crash directories and child-only environment overrides permit normal bounded test parallelism.
- Writing tests:
  - Prefer primary constructors for fixtures and output, for example `public class MyTests(DatabaseCache db, ITestOutputHelper output) : IClassFixture<DatabaseCache>`.
  - Test methods may return `Task` or `ValueTask`.
  - `xunit.runner.json` uses `parallelMode: all` with conservative scheduling and at most twice the CPU count, including tests within one class. Keep tests independent: give each its own database stream or uniquely named file. Diagnostic assertions capture through `TestTraceScope` rather than changing global trace listeners per test. Process-wide heap measurements and tests that cannot share the machine take `[Fact(DisableParallelization = true)]` (or the theory equivalent); a class whose tests share state takes `[TestClass(DisableParallelization = true)]`.
  - `[InlineData]` is type-checked strictly; `TheoryData<T>`, `MemberData` and `ClassData` still work.
- Write the test first, and for a bug write a failing test first.
- Golden byte tests retain normalized specimens and complete byte/page differences in CI's `golden-bytes-<framework>` artifacts. Approved `Databases/Golden/*.bin` fixtures must match the pinned hashes. For a deliberate format correction, review the byte differences and independent native evidence before updating both bytes and hash; never accept generated output automatically.

## Benchmarks

Run every benchmark on GitHub Actions (`.github/workflows/benchmarks.yml`). Never execute benchmarks locally, even for a single case, a short/dry smoke check, or debugging:

```
gh workflow run benchmarks.yml --ref main -f filter="<filter patterns>" -f baseline="<branch point SHA>" -f job=default
```

- The `filter` input takes BenchmarkDotNet `--filter` patterns separated by spaces, for example `"*AccessWriterBenchmarks* *AccessReaderRowDecodeBenchmarks*"`. Verify the dispatched run tests the settled `main` SHA.
- With `baseline`, the runner benchmarks the baseline commit and then the commit under test back to back on the same machine. Use a baseline already published in main history; never publish a work branch or tag to make it available. The comparison table (mean, error, allocations and head/baseline ratios) goes to the run summary.
- A baseline also requires completed, successful `ci.yml` for its exact SHA. The helper and direct workflow refuse invalid or pending baselines before dispatch/build and pass only the resolved immutable SHA to the runner. The runner installs the baseline's SDK before building it. Shared fixture correctness checks belong in ordinary tests as well as benchmark setup.
- The `job` input accepts `short`, `medium` or `dry` in place of BenchmarkDotNet's default adaptive job.
- Dispatch returns immediately. A run can take an hour or more; inspect its status and require a completed verdict before claiming benchmark acceptance. The helper's `-RunId` reporting mode can summarize the completed run.
- The full results are in the run's `benchmark-results` artifact.
- Hosted runners are noisy: treat a difference of a few percent as noise unless it is well outside both error columns (BenchmarkDotNet's Error, half the 99.9% confidence interval).

BenchmarkDotNet practice on CI:

- BenchmarkDotNet warms up by default; do not add a separate warmup run.
- The hosted comparison script uses `--stopOnFirstError`; a failed fixture or measurement aborts remaining cases and retains partial diagnostics. Fix the failing case before treating any results as acceptance.
- Keep the default adaptive job for release-quality numbers. Use the workflow input `job=short` only for a focused refresh. Narrow a CI run with its `filter` input instead of lowering iteration or warmup counts.
- BenchmarkDotNet already switches Windows to the High performance power plan during runs; add no boilerplate for it.
- `[IterationSetup]` suits destructive writer benchmarks that need a fresh database per operation, but it forces `InvocationCount=1` and `UnrollFactor=1`. Copy from an unmeasured baseline fixture instead of rebuilding the schema in each benchmark.
- A `Mean` or `Allocated` of `NA`, or a "Benchmarks with issues" section in `BenchmarkDotNet.Artifacts/results/*-report-github.md`, means the benchmark is broken; fix it before tuning anything. `--job dry` reports `Error = NA` from its single measurement, which is expected.
- Compare a change against its branch point, measured in the same run: the `baseline` workflow input does that.

## Code

- netstandard2.1 compatibility:
  - Guard `System.Threading.Lock` with `#if NET9_0_OR_GREATER`.
  - Guard `RandomAccess` and one-shot crypto APIs with `#if NET6_0_OR_GREATER`.
  - Use `IncrementalHash`, `RandomNumberGenerator.Create()` and `Flush(bool)`.
  - Do not use `Activator`.
- Checked arithmetic is on everywhere (`CheckForOverflowUnderflow`). A narrowing cast whose source can exceed the target range, such as `(ushort)uintValue` or `(byte)(a + b)`, throws `OverflowException`. In hash, CRC, XOR and shift code, use `unchecked { ... }` or mask explicitly (`value & 0xFFFFu`). Keep file offsets in `long` and narrow with an explicit `checked((int)...)`.
- `BannedSymbols.txt` (rule RS0030) bans blocking and sync-over-async APIs: `Thread.Sleep`, `Thread.Join`, `Task.Wait`, `Task.WaitAll`, `Task.WaitAny` and `Task<T>.Result`. It also bans `Environment.FailFast`. Await instead.
- InferSharp/Pulse can miss `await using` disposal, or ownership that flows through helper coordinators. For analyzer-facing fixes, prefer direct `try`/`finally` disposal and methods that own their resources.
- Match the surrounding code: its comment density, naming and idioms.

## Files and tooling

- Line endings: `.gitattributes` sets `* text=auto eol=crlf`, so working-tree files are CRLF, UTF-8 without a BOM. Some file-writing tools emit LF. Normalize a file you create or fully rewrite before you commit it or apply scripted multi-line edits to it, for example with ``[regex]::Replace($text, "(?<!\r)\n", "`r`n")``.
- Do not use Python for anything in this repository: implementation, diagnostics, inspection or throwaway helpers. Use PowerShell 7, .NET tooling and `rg`.
- In bulk PowerShell rewrites, write with `[System.IO.File]::WriteAllText(...)`. `Set-Content` after `Get-Content -Raw` adds a blank line at the end of the file.
- Repository scripts live in `scripts/`.
- CI runs `pwsh -NoProfile -File scripts/test-scripts.ps1` in the unconditional Linux checks job to parse all PowerShell scripts and run their regression suite with synthetic inputs and mocked external commands. Independent checks run in at most four isolated subprocesses, retaining every child failure and its diagnostics. For targeted local checks, invoke only the relevant file under `scripts/tests/`; these checks do not launch real benchmarks, fuzzers or GitHub operations.

## Git

- Several agents often work in this repository at the same time, in the main checkout and in their own worktrees.
  - Work on a branch in your own worktree. When fanning out parallel work, give each agent or task its own individual Git worktree and branch.
  - Commit each logical change within a worktree separately; do not combine unrelated changes into one commit.
  - Never modify, remove or prune another worktree, branch or stash. The stash stack is shared, so never use a bare `git stash` or `git stash pop`.
  - Land work by fast-forwarding `main` to the reviewed branch.
- Commit titles follow Conventional Commits: `feat:`, `fix:`, `refactor:`, `perf:`, `test:`, `docs:`, `build:`, `ci:`, with `!` for a breaking change.
- Commit in isolated local work branches, integrate reviewed session work into local `main`, and publish only `main`. Never push work branches or publication tags. Preserve other active sessions' uncommitted work; only remove this session's eligible worktrees and local branches after integration and validation.

## PLAN.md

`PLAN.md` lists open work only. When a fix lands, the fixing commit, in the same pass:

- deletes the completed item and every reference to it, or narrows the item and its references to the remaining gap;
- adds any new follow-ups as self-contained items (cause, repro or evidence gap, plan label, dependencies and acceptance conditions);
- updates the design docs the fix invalidates.

Never add "Fixed" entries, test counts, history, or a narrative of what changed: the commit messages record fixes. Facts worth keeping belong in the README or `docs/design/`.
