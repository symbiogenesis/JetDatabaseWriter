# AGENTS.md

Instructions for coding agents working in this repository. They apply to every session and every subagent; the people who work here follow them too.

Reference material, read when a task needs it:

- [docs/glossary.md](docs/glossary.md): Access/JET acronyms, constants and on-disk terms used in the code, docs and comments.
- [docs/design/](docs/design/): format notes for pages, rows, indexes, relationships, long values, complex columns, encryption and calculated columns.
- [docs/todo.md](docs/todo.md): all open work, and the wave-2 plan.
- [PUBLISH.md](PUBLISH.md): how a release is cut and published.

## The project

JetDatabaseWriter reads and writes Microsoft Access databases in pure .NET: Jet3 (Access 97) and Jet4 `.mdb` files, and ACE `.accdb` files. The solution file is `JetDatabaseWriter.slnx`, and `global.json` pins the .NET 10 SDK.

| Project | Targets | Role |
|---|---|---|
| `JetDatabaseWriter` | netstandard2.1, net10.0 | The library and NuGet package |
| `JetDatabaseWriter.Tests` | net10.0, net8.0 | xUnit v3 on Microsoft Testing Platform. The net8.0 leg loads the library's netstandard2.1 build |
| `JetDatabaseWriter.TestSupport` | net10.0, net8.0 | Non-packable helpers that the tests and FormatProbe share |
| `JetDatabaseWriter.Benchmarks` | net10.0 | BenchmarkDotNet |
| `JetDatabaseWriter.FormatProbe` | net10.0 | Diagnostics against real Access files |
| `JetDatabaseWriter.Scaffold` | net10.0 | Entity scaffolding tool |

Version 4.0.0 has never been released. The tags v1.0.0 to v2.2.0 are upstream history and must never be pushed, and MinVer takes the version from `v`-prefixed tags. Break APIs freely: add no compatibility shims, `[Obsolete]` wrappers, flags kept only for old behaviour, migration notes, or "breaking change" callouts in the README.

When a design decision is left to you, choose what Microsoft Access does, then the safest option, and state the choice briefly.

## Build

- `dotnet build JetDatabaseWriter.slnx -c Release` must report 0 warnings and 0 errors. `Directory.Build.props` turns on `TreatWarningsAsErrors`, `AnalysisLevel` latest-all, `EnforceCodeStyleInBuild`, XML documentation and nullable reference types for every project. CI analyzes every project and target framework in Release, so an analyzer finding in test code fails it too.
- The analyzers take most of a Release build. `-p:AnalyzeOnly=<project names>` analyzes only the named projects and `-p:AnalyzeExcept=<project names>` every project but those (names separated by spaces); the other projects compile without analyzers, and source generators run either way. CI's analyze jobs use them, and `dotnet build JetDatabaseWriter.Tests -c Release -f net10.0 -p:AnalyzeOnly=JetDatabaseWriter.Tests` checks test code alone.
- `JetDatabaseWriter.Tests` turns off `RunAnalyzersDuringBuild`, `EnforceCodeStyleInBuild` and `GenerateDocumentationFile` in non-Release builds, for speed; keep Release strict. Its `NoWarn` holds only `CA1707` and `SA1615`, and nothing else may be added. Fix any other finding in test code. Where a rule works against what a test checks, suppress it at the site with `#pragma warning disable <id> // <reason>`, for example CA1812 on a POCO that only reflection, `Rows<T>` or an expression tree creates.
- `JetDatabaseWriter.FormatProbe` does not turn off its analyzers.
- CI restores with `--locked-mode`. After changing a package reference or a target framework, restore once without it and commit the updated `packages.lock.json` files.

## Gating a commit on CI

Gate every commit that can change the build or the tests on GitHub CI, not with local builds and test runs. A commit that touches only docs or scripts outside the build needs no gate. Several agents often share one machine, and long local runs slow everyone down and stall agents. A local run is fine for debugging one test; report gate results from CI.

`ci.yml` runs on a push to `main`. Integrate the reviewed work into local `main`, then publish only that branch:

```
git -c push.followTags=false push origin main:main
gh run list --workflow ci.yml --branch main --commit <commit>
```

Record the run URL, exact tested SHA and verdict. Inspect all six jobs: four Release analyzer/package jobs and one ordinary test job per target framework. Do not treat a queued or running workflow as passed. A timing-sensitive failure may be retried once with `gh run rerun <id> --failed` before diagnosing it as reproducible.

For a completed run, the reporting mode of the existing helper prints every step result, build summaries, both test summaries and failing-test messages without pushing or dispatching:

```
pwsh -NoProfile -File scripts/ci-gate.ps1 -RunId <id>
```

Do not use the helper's `-Branch` mode: it publishes temporary branches, contrary to the main-only publication rule. If a matching run is missing, dispatch `gh workflow run ci.yml --ref main` and verify the resulting run's SHA. Keep work branches local and remove this session's eligible worktrees and branches after their changes are integrated and validated, retaining useful outputs.

## Tests

Expected results on both legs:

- Nothing fails.
- Every skipped test is a skip-guarded DAO test ("Requires Microsoft Access (DAO.DBEngine.120)"). Microsoft Access is absent on hosted CI, so those cases skip there. A local DAO host can run them with CI-built binaries when authorized; engine-specific skips remain evidence gaps.
- 3 explicit-only fuzz tests are not run.

The Microsoft Testing Platform summary counts guarded DAO cases and the three explicit-only fuzz harnesses together as skipped. Check the skip reasons against the current guarded cases; any other skip is a regression.

For a quick local loop, build in Release and run the test executable directly. `JetDatabaseWriter.Tests/bin/Release/<tf>/JetDatabaseWriter.Tests.exe -longRunning 300` runs one leg in about 1.5 minutes; add `-method "<Namespace.Class.Method>"` to run one test.

xUnit v3 on Microsoft Testing Platform:

- Use xUnit v3 (`xunit.v3`, stable 3.x), not the 4.x prerelease line. Test projects are executables: keep `<OutputType>Exe</OutputType>`.
- Use `using Xunit;`. Do not add `Xunit.Abstractions`, `xunit.runner.visualstudio`, `xunit.abstractions` or `xunit.assert`.
- The base command is `dotnet test --project JetDatabaseWriter.Tests`. Pass Microsoft Testing Platform options straight to `dotnet test`, with no `--` separator. Do not use VSTest filters (`--filter "FullyQualifiedName~..."`) or `--nologo`; use `--verbosity quiet` for quieter output.
  - One target framework: `-f net10.0`.
  - One method: `--filter-method "<Namespace.Class.Method>"`.
  - One class: `--filter-class "<Namespace.Class>"`.
  - One namespace: `--filter-namespace "<Namespace>"`.
  - Exclusions: `--filter-not-class`, `--filter-not-method` and `--filter-not-namespace`.
  - Traits: `--filter-trait Category=Fuzz` and `--filter-not-trait Category=Fuzz`.
  - Several values go after one switch, separated by spaces (`--filter-class Foo Bar`).
  - Other switches: `--list-tests`, `--stop-on-fail on`, and `-?`.
- Fuzz harnesses are `[Fact(Explicit = true)]` with `[Trait("Category", "Fuzz")]`. Run them only on purpose: `dotnet test --project JetDatabaseWriter.Tests --filter-trait Category=Fuzz --explicit only`.
- Test code must compile on both legs. Polyfills for newer BCL types go in `JetDatabaseWriter.Tests/Polyfills`, and `LibraryTarget.IsNetStandard` tells a test which library build it loaded. The scaffolding tests build for net10.0 only.
- Helpers that the tests and FormatProbe share go in `JetDatabaseWriter.TestSupport`, never in the library. Run child processes through `PowerShellProcessRunner`, not by reading a redirected stream to its end before `WaitForExit`, which ignores the timeout.
- Writing tests:
  - Prefer primary constructors for fixtures and output, for example `public class MyTests(DatabaseCache db, ITestOutputHelper output) : IClassFixture<DatabaseCache>`.
  - Test methods may return `Task` or `ValueTask`.
  - `xunit.runner.json` sets `parallelMode` to `all`, so every test runs in parallel with every other, including the tests of one class. Keep tests independent: give each its own database stream or uniquely named file. A test that cannot share the machine takes `[Fact(DisableParallelization = true)]`, and a class whose tests share state takes `[TestClass(DisableParallelization = true)]`.
  - `[InlineData]` is type-checked strictly; `TheoryData<T>`, `MemberData` and `ClassData` still work.
- Write the test first, and for a bug write a failing test first.

## Benchmarks

Run benchmarks on GitHub Actions (`.github/workflows/benchmarks.yml`), not on a shared development machine:

```
gh workflow run benchmarks.yml --ref main -f filter="<filter patterns>" -f baseline="<branch point SHA>" -f job=default
```

- The `filter` input takes BenchmarkDotNet `--filter` patterns separated by spaces, for example `"*AccessWriterBenchmarks* *AccessReaderRowDecodeBenchmarks*"`. Verify the dispatched run tests the settled `main` SHA.
- With `baseline`, the runner benchmarks the baseline commit and then the commit under test back to back on the same machine. Use a baseline already published in main history; never publish a work branch or tag to make it available. The comparison table (mean, error, allocations and head/baseline ratios) goes to the run summary.
- The `job` input accepts `short`, `medium` or `dry` in place of BenchmarkDotNet's default adaptive job.
- Dispatch returns immediately. A run can take an hour or more; inspect its status and require a completed verdict before claiming benchmark acceptance. The helper's `-RunId` reporting mode can summarize the completed run.
- The full results are in the run's `benchmark-results` artifact.
- Hosted runners are noisy: treat a difference of a few percent as noise unless it is well outside both error columns (BenchmarkDotNet's Error, half the 99.9% confidence interval).

BenchmarkDotNet practice, wherever the benchmarks run:

- BenchmarkDotNet warms up by default; do not add a separate warmup run.
- Keep the default adaptive job for release-quality numbers. Use `--job short` only for a focused refresh. Narrow a run with `--filter` (plus `dotnet run --no-restore`) instead of lowering iteration or warmup counts.
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

## Git

- Several agents often work in this repository at the same time, in the main checkout and in their own worktrees.
  - Work on a branch in your own worktree.
  - Never modify, remove or prune another worktree, branch or stash. The stash stack is shared, so never use a bare `git stash` or `git stash pop`.
  - Land work by fast-forwarding `main` to the reviewed branch.
- Commit titles follow Conventional Commits: `feat:`, `fix:`, `refactor:`, `perf:`, `test:`, `docs:`, `build:`, `ci:`, with `!` for a breaking change.
- Commit in isolated local work branches, integrate reviewed session work into local `main`, and publish only `main`. Never push work branches or publication tags. Preserve other active sessions' uncommitted work; only remove this session's eligible worktrees and local branches after integration and validation.

## docs/todo.md

`docs/todo.md` lists open work only. When a fix lands, the fixing commit, in the same pass:

- deletes the item and every reference to it;
- adds any new follow-ups as self-contained items (cause, repro, plan label);
- updates the design docs the fix invalidates.

Never add "Fixed" entries, test counts, history, or a narrative of what changed: the commit messages record fixes. Facts worth keeping belong in the README or `docs/design/`.
