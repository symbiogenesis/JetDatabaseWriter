# Static Analysis Modernization TODOs

Status: candidate backlog
Date: 2026-05-27

This note tracks a performance-first path for keeping static analysis useful
without making clean builds slower. The goal is not to add analyzers for their
own sake; the goal is a strict, maintainable, fast build where most diagnostics
point at real issues and every analyzer package either pays for itself locally
or moves to a slower CI/security lane.

## Current Posture

- [Directory.Build.props](../../Directory.Build.props) enables nullable,
  `WarningLevel 9999`, `AnalysisLevel latest-all`, .NET analyzers,
  warnings-as-errors, build-time code style, XML documentation generation, and
  checked arithmetic globally.
- [Directory.Build.props](../../Directory.Build.props) references
  `Microsoft.CodeAnalysis.BannedApiAnalyzers`, `Roslynator.Analyzers`,
  `SecurityCodeScan.VS2019`, and `StyleCop.Analyzers` for every project.
- [Directory.Packages.props](../../Directory.Packages.props) currently pins
  `StyleCop.Analyzers` to `1.2.0-beta.556`, with package-lock files resolving
  `StyleCop.Analyzers.Unstable` transitively.
- [stylecop.json](../../stylecop.json) is small and only configures StyleCop
  documentation settings.
- [.editorconfig](../../.editorconfig) is minimal and currently only hardens
  file-scoped namespace declarations for C#.
- Several StyleCop rules are globally suppressed in
  [Directory.Build.props](../../Directory.Build.props), so StyleCop is already
  a partial style/documentation layer rather than the central quality gate.
- [JetDatabaseWriter.Tests.csproj](../../JetDatabaseWriter.Tests/JetDatabaseWriter.Tests.csproj)
  disables `RunAnalyzersDuringBuild`, `EnforceCodeStyleInBuild`, and XML
  documentation generation for non-Release builds. That is the right shape for
  a fast test inner loop; Release remains strict.
- `dotnet outdated` reported no outdated direct dependencies on 2026-05-27.

The repo is already using SDK analyzers as the center of gravity. The remaining
questions are about local-build cost, analyzer scope, and whether older broad
packages should be retired, moved to CI, or replaced with smaller local policy.

## Measurement Notes

Analyzer timing is available from Roslyn when the compiler actually runs. Use a
forced rebuild for measurement; up-to-date builds can list analyzer inputs while
skipping the compiler pass.

Useful one-project timing command:

```powershell
dotnet build <project.csproj> --configuration Release --no-restore -t:Rebuild /p:ReportAnalyzer=true /p:TreatWarningsAsErrors=false -v:detailed
```

Useful solution smoke command:

```powershell
dotnet build JetDatabaseWriter.slnx --configuration Release --no-restore -m
```

Notes:

- SDK 10 already invokes MSBuild with max CPU count in observed `dotnet build`
  command lines. Keep `-m` in scripts for clarity, but do not expect it to make
  a single project analyzer pass magically more parallel.
- Roslyn analyzers run concurrently only when the analyzer implementation opts
  in. There is no repo setting that forces third-party analyzers to parallelize
  more aggressively.
- The analyzer report prints concurrent analyzer execution time. Elapsed build
  time can be lower than the sum because analyzer actions run in parallel.
- Changing severity from warning to error does not make a rule cheaper. Disabled
  rules, narrower scope, fewer analyzer packages, or fewer diagnostics emitted
  are the levers that affect analyzer cost.

## Measured Baseline

### Clean Solution Baseline

After fixing the unrelated Release test analyzer failures, a 2026-05-27 forced
strict Release rebuild of [JetDatabaseWriter.slnx](../../JetDatabaseWriter.slnx)
used SDK `10.0.300` and this command:

```powershell
dotnet build JetDatabaseWriter.slnx --configuration Release --no-restore -m -t:Rebuild /p:ReportAnalyzer=true -bl:obj/AnalyzerTiming/release-sln-rebuild-20260527-133323.binlog -v:detailed
```

Result: exit code `0`, outer stopwatch `00:00:18.8843371`, MSBuild reported
`Time Elapsed 00:00:18.55`, `0 Warning(s)`, and `0 Error(s)`. Detailed text log:
[release-sln-rebuild-20260527-133323.log](../../obj/AnalyzerTiming/release-sln-rebuild-20260527-133323.log).
Binary log:
[release-sln-rebuild-20260527-133323.binlog](../../obj/AnalyzerTiming/release-sln-rebuild-20260527-133323.binlog).

Analyzer totals by compiler invocation:

| Compiler invocation | Total analyzer execution time |
|---------------------|-------------------------------|
| `JetDatabaseWriter` `net10.0` | 20.485s |
| `JetDatabaseWriter` `netstandard2.1` | 18.199s |
| `JetDatabaseWriter.Scaffold` `net10.0` | 0.286s |
| `JetDatabaseWriter.Benchmarks` `net10.0` | 1.003s |
| `JetDatabaseWriter.FormatProbe` `net10.0` | 15.438s |
| `JetDatabaseWriter.Tests` `net10.0` | 21.727s |

### Library Project

A 2026-05-27 forced Release rebuild of
[JetDatabaseWriter.csproj](../../JetDatabaseWriter/JetDatabaseWriter.csproj)
with `-p:ReportAnalyzer=true` showed these analyzer execution-time groups for
the two target frameworks:

| Analyzer group | netstandard2.1 | net10.0 | Initial read |
|----------------|----------------|---------|--------------|
| SDK .NET analyzers | 13.935s | 11.156s | Keep, but `latest-all` is expensive and should be justified. |
| `SecurityCodeScan.VS2019` | 9.170s | 8.749s | High-priority local-build removal or CI-lane candidate. |
| SDK C# code-style analyzers | 7.022s | 6.268s | Large cost from `EnforceCodeStyleInBuild`; consider Debug/local relief. |
| `StyleCop.Analyzers` | 4.879s | 6.055s | High-priority retirement candidate. |
| `Roslynator.Analyzers` | 1.116s | 1.081s | Relatively cheap; tune before removing. |
| `Microsoft.CodeAnalysis.BannedApiAnalyzers` | less than 0.001s | less than 0.001s | Keep; nearly free and uniquely local. |

### Test Project

A 2026-05-27 forced Release rebuild of
[JetDatabaseWriter.Tests.csproj](../../JetDatabaseWriter.Tests/JetDatabaseWriter.Tests.csproj)
with `TreatWarningsAsErrors=false` produced about `19.356s` of concurrent
analyzer execution time.

| Analyzer group | Time | Initial read |
|----------------|------|--------------|
| SDK .NET analyzers | 6.651s | Largest group; `CA2000` dominated at about 3.159s. |
| `SecurityCodeScan.VS2019` | 5.014s | Taint and crypto analysis dominate; web-only rules were near zero but irrelevant. |
| `StyleCop.Analyzers` | 2.790s | Broad formatting/layout/documentation pass over many test files. |
| SDK C# .NET analyzers | 2.168s | `CA1508` dominated at about 1.782s. |
| `xunit.analyzers` | 1.003s | Worth keeping; diagnostics are test-framework-specific and high signal. |
| `Roslynator.Analyzers` | 0.279s | Cheap in tests. |
| `Microsoft.CodeAnalysis.BannedApiAnalyzers` | less than 0.001s | Keep. |

The initial strict Release solution baseline attempt on this date failed on
test-project analyzer diagnostics unrelated to this note, including `SA1216`,
`SA1204`, `xUnit1026`, `CA1062`, `CA1859`, and `SA1118`. Those were fixed
before the clean solution baseline above was captured.

### FormatProbe Spot Check

A warmed forced Release rebuild of
[JetDatabaseWriter.FormatProbe.csproj](../../JetDatabaseWriter.FormatProbe/JetDatabaseWriter.FormatProbe.csproj)
showed the same rough ranking at smaller scale: `SecurityCodeScan.VS2019` about
1.224s, SDK C# .NET analyzers about 1.161s with `CA1508` about 1.056s,
`StyleCop.Analyzers` about 0.560s, `Roslynator.Analyzers` about 0.340s, and
`Microsoft.CodeAnalysis.BannedApiAnalyzers` effectively free.

## Consolidation Decisions

### 1. Remove `Roslynator.Refactorings` From Build Package References First

Verdict: lowest-risk cleanup.

Why:

- `Roslynator.Refactorings` is useful editor tooling, not a Release-build
  diagnostic gate.
- The package is resolved into compiler analyzer inputs, including Workspaces
  and refactoring assemblies, but did not appear as an analyzer execution-time
  group in the timing report.
- Upstream guidance is to use IDE extensions when available; package delivery is
  primarily for environments where the extension cannot be used.

TODOs:

- [x] Remove the `Roslynator.Refactorings` package reference from
      [Directory.Build.props](../../Directory.Build.props).
- [x] Restore/build once and verify package-lock churn is limited to removing
      that analyzer/refactoring payload.
- [x] Keep `Roslynator.Analyzers` separately unless its diagnostics stop paying
      for their small measured cost.
- [ ] Use the Roslynator VS Code extension or command-line tooling for
      refactorings instead of project `PackageReference` delivery.

Completed on 2026-05-27: removed the global package reference and the unused
central package version pin. `dotnet restore JetDatabaseWriter.slnx` removed
only direct `Roslynator.Refactorings` lock-file entries, and
`dotnet build JetDatabaseWriter.slnx --configuration Release --no-restore -m`
passed in `50.8s`.

### 2. Remove or Move `SecurityCodeScan.VS2019` Out of Local Builds

Verdict: strongest heavy-package removal candidate.

Why:

- `SecurityCodeScan.VS2019` is expensive in both library and test measurements.
- The official `SecurityCodeScan.VS2019` package is at its latest version
  (`5.6.7`). `dotnet package search` also shows the older base
  `SecurityCodeScan` package and a third-party `AdaskoTheBeAsT.SecurityCodeScan.VS2022`
  repack, but not an obvious official modern successor that changes the
  cost/value equation.
- Some rules are relevant here: weak crypto, weak hashing, path traversal,
  unsafe deserialization, hardcoded secrets, process/command injection, and XML
  parser safety.
- Some rules are irrelevant for this library: cookies, CSRF, request validation,
  output cache, authorization attributes, Web.config, XSS, and open redirect.
- In the test-project timing, the web-only rules were nearly free; the expensive
  portion was mostly taint and crypto analysis. Disabling irrelevant web rules
  may reduce noise but is not likely to recover most of the measured time.

Replacement path:

- [ ] First remove `SecurityCodeScan.VS2019` from local builds and measure the
      same Release commands three times.
- [ ] Keep SDK security CA rules enabled unless a specific rule proves too slow
      or too noisy.
- [ ] Add cheap project-specific security bans to
      [BannedSymbols.txt](../../BannedSymbols.txt) when the desired rule is just
      "never call this API here".
- [ ] Move broad security/code-smell scanning to a slower lane: CodeQL,
      SonarAnalyzer/SonarCloud, Puma Scan, or a scheduled CI job.
- [ ] If a Roslyn security package is still wanted locally, trial
      `SonarAnalyzer.CSharp` or another security analyzer in isolation before
      adopting it. Assume it is costly until measured.

### 3. Gradually Retire `StyleCop.Analyzers`

Verdict: retire policy surface deliberately; do not replace it one-for-one.

Why:

- The stable StyleCop package is old, and this repo uses a 2023 beta shim that
  resolves `StyleCop.Analyzers.Unstable`.
- StyleCop costs several seconds per target framework in the measured library
  build and about 2.790s in the measured test build.
- The current `NoWarn` list already disables many StyleCop opinions, including
  documentation, ordering, naming, and layout rules.
- Current test-project Release failures include SA-only style policy such as
  using-static order, member ordering, and multiline argument formatting. Those
  may be useful team preferences, but they are not correctness checks.
- There is no clear actively maintained drop-in replacement for the whole
  StyleCop surface. `Menees.Analyzers` and StyleCopPlus-like ports may replace
  a few preferences, but adopting another broad StyleCop-like package would
  restart the same maintenance and performance problem.

Replacement path:

- [ ] List the StyleCop rules that still fire in a strict Release build.
- [ ] For each useful StyleCop rule, decide whether it belongs in
      [.editorconfig](../../.editorconfig), Roslynator configuration, SDK
      analyzer configuration, documentation policy, or nowhere.
- [ ] Move formatting and layout preferences to `.editorconfig` and
      `dotnet format` where SDK tooling can enforce them.
- [ ] Explicitly retire member-ordering and using-order rules unless they are
      worth several seconds of analyzer cost per target framework.
- [ ] Keep XML documentation policy only if it provides value beyond compiler
      warning `CS1591` and API review discipline.
- [ ] Remove StyleCop suppressions from `NoWarn` only when the corresponding
      policy has a replacement or is intentionally retired.
- [ ] Remove [stylecop.json](../../stylecop.json) and the StyleCop package only
      after a clean Release build proves the replacement policy is complete.

### 4. Tune SDK Code Style and `AnalysisLevel latest-all` Last

Verdict: biggest potential speed lever, but highest policy risk.

Why:

- SDK .NET analyzers and SDK C# code-style analyzers are the two largest
  measured analyzer groups.
- `AnalysisLevel latest-all` intentionally enables more rules than the default
  recommended set.
- `EnforceCodeStyleInBuild=true` makes IDE-style preferences part of every
  strict build.
- `CA2000` and `CA1508` are the current pattern-sensitive hotspots. `CA2000`
  can be expensive around helper-coordinated ownership transfer. `CA1508` can
  become expensive or awkward around complex control flow, parser logic, fuzz
  scaffolding, and state-machine-like code.

TODOs:

- [ ] Trial `AnalysisLevel latest` against `latest-all` and compare diagnostics
      and timing before changing policy.
- [ ] Consider category-specific analyzer modes instead of global `latest-all`,
      keeping security and reliability strong while relaxing style or design
      rules if they do not pay for themselves.
- [ ] Consider keeping `EnforceCodeStyleInBuild` for Release/CI while disabling
      it for Debug/local builds that are meant to be fast.
- [ ] Keep `dotnet format --verify-no-changes` as an explicit formatting gate
      if code-style analyzers move out of normal local builds.
- [ ] Prefer code-shape fixes for expensive but valuable rules before disabling
      them: direct `using`/`try`/`finally` ownership for `CA2000`, and simpler
      local control flow where `CA1508` gets confused.
- [ ] If a rule remains expensive and low value in tests or fuzz harnesses,
      narrow it by path/category in `.editorconfig` rather than globally.

### 5. Keep and Extend `Microsoft.CodeAnalysis.BannedApiAnalyzers`

Verdict: keep.

Why:

- It is effectively free in measured builds.
- It enforces local policy in [BannedSymbols.txt](../../BannedSymbols.txt),
  which general analyzers cannot fully replace.
- It is ideal for cheap, high-confidence bans: blocking APIs, weak crypto,
  unsafe serializers, ambiguous process-start overloads, and project-specific
  footguns.

TODOs:

- [ ] Prefer adding precise local bans over adding broad analyzer packages when
      the desired policy is symbol-based.
- [ ] Keep the list short and high-signal so `RS0030` remains trusted.

### 6. Keep `xunit.analyzers`

Verdict: keep.

Why:

- The test project already receives `xunit.analyzers` transitively from xUnit v3.
- The measured cost is about 1.003s in a large Release test-project rebuild,
  which is acceptable for framework-specific diagnostics.
- It catches real test issues (`xUnit1026`, fixture/source validation,
  cancellation-token guidance, blocking-task operations, assertion misuse) that
  generic analyzers do not understand as precisely.

TODOs:

- [ ] Do not suppress xUnit analyzers broadly to recover build time.
- [ ] Fix current xUnit warnings as ordinary test quality issues.

### 7. Keep `Roslynator.Analyzers` Unless Its Diagnostics Stop Paying Rent

Verdict: keep for now, tune later.

Why:

- It is much cheaper than SDK analyzers, SecurityCodeScan, and StyleCop in the
  measured builds.
- It provides C#-specific style, maintainability, async, documentation, and
  refactoring-adjacent diagnostics that partially overlap with, but do not fully
  duplicate, SDK analyzers.
- Removing `Roslynator.Refactorings` should not require removing
  `Roslynator.Analyzers`.

TODOs:

- [ ] After the heavier cleanup, review Roslynator diagnostics for overlap and
      disable any low-value rules explicitly.
- [ ] Keep Roslynator rule configuration in `.editorconfig` instead of broad
      source suppressions.

## Candidate Analyzer Ecosystem Triage

The `awesome-analyzers` list is useful as a discovery index, but most packages
should not be added to local builds under a speed-first goal. Treat new analyzer
packages as replacement experiments, CI-only experiments, or technology-specific
tools that must match code the repo actually uses.

### Reasonable Replacement or CI Trials

| Candidate | Fit for this repo | Local-build default |
|-----------|-------------------|---------------------|
| `Meziantou.Analyzer` | Broad, active, editorconfig-friendly; useful rules around culture, async disposal, regex, process start, stream reads, string comparison, and cancellation. | Do not add by default; trial only after removing heavier packages. |
| `SonarAnalyzer.CSharp` | Active broad bug/security/code-smell analyzer; possible slower-lane replacement for some SecurityCodeScan value. | Prefer CI or scheduled trial first. |
| CodeQL / SonarCloud / Puma Scan | Better suited for security/code-smell sweeps than every local compile. | CI or scheduled lane, not local build. |
| `IDisposableAnalyzers` | Relevant because streams/readers/writers and ownership boundaries matter. | Trial only if resource-ownership bugs justify measured cost or if it replaces other ownership checks. |
| `AsyncFixer` | Small async misuse analyzer; overlaps with SDK, Roslynator, xUnit, and banned APIs. | Trial only after async gaps are identified. |
| `ErrorProne.NET`, `Gu.Analyzers`, `SharpSource` | General correctness analyzers that may catch useful bugs. | CI-only or isolated branch trial; assume overlap/noise until measured. |
| `Exceptionator` / `SmartAnalyzers.ExceptionAnalyzer` | Exception-handling focus could be useful, but overlaps with SDK and existing policy. | Not a default local-build addition. |
| `Microsoft.CodeAnalysis.PublicApiAnalyzers` | Relevant to a reusable library if API compatibility baselines become a release policy. | Separate API-governance decision, not a build-speed optimization. |
| `.NET project file analyzers` | Could catch project-file issues, but does not address C# analyzer cost. | Optional CI/tooling check, not part of every C# compilation. |

### Poor Fits or Irrelevant Technologies

Do not add local analyzers for technologies this repo does not use:

- ASP.NET Core, MVC, WebForms, cookies, CSRF, request validation, output cache,
  routing, or authorization analyzers.
- Entity Framework / `DbContext` thread-safety analyzers.
- Moq, NSubstitute, FluentAssertions, Serilog, System.IO.Abstractions, OneOf,
  ClosedTypeHierarchy, AutoMapper/mapping generators, or other library-specific
  analyzers unless the repo adopts those libraries.
- `Asyncify`-style migration tooling. The public API is already async-first;
  the repo does not need a TAP migration analyzer in normal builds.
- Deprecated packages from the analyzer list, including Code Cracker,
  CSharpEssentials, RefactoringEssentials, VSDiagnostics, old heap-allocation
  analyzers, and other unmaintained analyzer collections.

### Meziantou Trial Rules

`Meziantou.Analyzer` is the best-looking general-purpose addition, but it is not
a drop-in replacement for any existing package. It partially overlaps SDK,
Roslynator, StyleCop, and security-adjacent rules without fully replacing them.

TODOs:

- [ ] Do not add `Meziantou.Analyzer` to the main branch until current analyzer
      cost has been measured and at least one heavier local analyzer surface has
      been removed or disabled in the same experiment.
- [ ] Start any trial with `MeziantouAnalysisMode=None` and enable only selected
      `MA` rules in [.editorconfig](../../.editorconfig), or start at suggestion
      severity if an inventory is desired.
- [ ] Candidate curated rules: string comparison and comparer rules,
      culture-sensitive formatting rules, regex timeout/source-generator rules,
      stream-read result checks, awaited-disposal checks, process-start rules,
      cancellation-token forwarding, explicit `ProcessStartInfo.UseShellExecute`,
      and limited XML documentation sanity checks.
- [ ] Reject the trial if median Release build time regresses more than the
      removed analyzer surface saved, unless the added diagnostics are
      intentionally worth the measured slowdown.

## Warning and Message Cost

Hypothesis: builds with many diagnostics can be slower than diagnostically clean
builds. This is plausible, but the mechanism matters.

Likely true:

- Emitting, formatting, logging, and transporting many warnings has nonzero cost
  in MSBuild, terminals, IDE error lists, CI logs, and language-server
  integrations.
- A zero-warning build is more pleasant and may be faster, especially when the
  alternative is hundreds or thousands of diagnostics.
- Hardening `.editorconfig` can help when it prevents warning debt, disables or
  narrows noisy rules, or lets the repo retire a package analyzer.

Easy to overestimate:

- Changing a rule from warning to error does not usually make the analyzer rule
  cheaper. Enabled analyzers still run.
- Adding more analyzer rules can slow builds down even if the repo is clean.
- `.editorconfig` does not make an enabled rule free. It speeds analysis only
  when it disables rules, narrows scope, or prevents recurring diagnostic churn.
- A clean build with more enabled analyzers can still be slower than a noisy
  build with fewer analyzers.

Practical model:

- Use `.editorconfig` to make the intended policy explicit.
- Keep high-signal rules enabled and severe enough to stop warning debt early.
- Disable or lower low-signal rules so they do not waste build, IDE, or review
  attention.
- Prefer removing analyzer packages, moving expensive checks to CI, or disabling
  noisy rules before adding new analyzer packages.
- Measure elapsed time, analyzer time, and diagnostic counts before treating
  analyzer changes as performance wins.

## Suggested Order of Work

- [x] Capture current clean Release build time, analyzer timing, warning count,
      and binary log after the unrelated test analyzer failures are fixed.
- [x] Remove `Roslynator.Refactorings` from build `PackageReference` items.
- [ ] Run a one-package-at-a-time local-build removal experiment for
      `SecurityCodeScan.VS2019`.
- [ ] Decide whether broad security scanning should move to CodeQL,
      SonarAnalyzer/SonarCloud, Puma Scan, or a scheduled CI lane.
- [ ] Inventory current StyleCop diagnostics in a strict Release build before
      removing StyleCop.
- [ ] Expand [.editorconfig](../../.editorconfig) only for obvious existing
      style policy that can replace StyleCop or reduce diagnostic churn.
- [ ] Move remaining useful StyleCop policy elsewhere or explicitly retire it.
- [ ] Remove StyleCop only after replacement policy is quiet under Release
      analyzer settings.
- [ ] Trial `AnalysisLevel latest` or category-specific analyzer modes against
      `latest-all` after old third-party package cleanup.
- [ ] Consider curated `Meziantou.Analyzer`, `AsyncFixer`,
      `Microsoft.VisualStudio.Threading.Analyzers`, `IDisposableAnalyzers`,
      `ErrorProne.NET`, or `SonarAnalyzer.CSharp` only after removal experiments
      show room in the analyzer budget.
- [ ] Update [README.md](../../README.md) when the analyzer stack changes.

## Experiment Checklist

For each analyzer package add/remove/tune experiment:

- [ ] Start from the same restore state and SDK.
- [ ] Run the same Release command at least three times and compare median
      elapsed time.
- [ ] Capture analyzer timing with `/p:ReportAnalyzer=true`.
- [ ] Capture warning/error counts.
- [ ] Keep package changes separate from rule-severity changes; they affect
      performance through different mechanisms.
- [ ] Prefer `-v:minimal` or quieter output when terminal/CI log volume appears
      to dominate perceived time.
- [ ] Record whether the experiment affects local Debug builds, Release builds,
      CI only, or IDE-only diagnostics.
