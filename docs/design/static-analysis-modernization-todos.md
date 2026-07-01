# Static Analysis Modernization TODOs

Status: initial pass shipped (2026-05-27); remaining items intentionally deferred
Date: 2026-05-27 (status refreshed 2026-06-30)

This note tracks a performance-first path for keeping static analysis useful
without making clean builds slower. The goal is not to add analyzers for their
own sake; the goal is a strict, maintainable, fast build where most diagnostics
point at real issues and every analyzer package either pays for itself locally
or moves to a slower CI/security lane.

**Completion status (2026-06-30):** the initial pass shipped. `Roslynator.Refactorings` (§1) and
`SecurityCodeScan.VS2019` (§2) were removed from local builds (see Current Posture), while
`StyleCop.Analyzers` (§3), `Roslynator.Analyzers`, and `BannedApiAnalyzers` were deliberately
retained (the last extended with security bans). The remaining unchecked `- [ ]` items — chiefly
the SDK `AnalysisLevel` / code-style tuning (§4) and the optional later `Meziantou.Analyzer` trial —
are **intentionally deferred future trials, not forgotten work**, and stay closed unless a new
build-time or coverage concern reopens them.

## Current Posture

- [Directory.Build.props](../../Directory.Build.props) enables nullable,
  `WarningLevel 9999`, `AnalysisLevel latest-all`, .NET analyzers,
  warnings-as-errors, build-time code style, XML documentation generation, and
  checked arithmetic globally.
- [Directory.Build.props](../../Directory.Build.props) currently references
  `Microsoft.CodeAnalysis.BannedApiAnalyzers`, `Roslynator.Analyzers`, and
  `StyleCop.Analyzers` for every project. `SecurityCodeScan.VS2019` was
  removed from local builds after the 2026-05-27 removal experiment below.
- [Directory.Packages.props](../../Directory.Packages.props) currently pins
  `StyleCop.Analyzers` to `1.2.0-beta.556`, with package-lock files resolving
  `StyleCop.Analyzers.Unstable` transitively.
- [stylecop.json](../../stylecop.json) is small and only configures StyleCop
  documentation settings.
- [.editorconfig](../../.editorconfig) now carries the StyleCop-oriented
  formatting and naming baseline from Rehan Saeed's EditorConfig project,
  repo-specific C# overrides to avoid high-churn style diagnostics in strict
  builds, explicit SDK security analyzer rules that cover injection, XML/XSLT
  safety, deserialization, crypto, insecure randomness, certificate validation,
  and web/security categories relevant to avoiding SCS local-build regressions,
  plus the local StyleCop/code-analysis suppressions that previously lived in
  broad MSBuild `NoWarn` lists.
- [BannedSymbols.txt](../../BannedSymbols.txt) is passed to
  `Microsoft.CodeAnalysis.BannedApiAnalyzers` as an `AdditionalFiles` input from
  [Directory.Build.props](../../Directory.Build.props), so `RS0030` diagnostics
  are active in strict builds.
- Several StyleCop/code-analysis rules are explicitly configured in
  [.editorconfig](../../.editorconfig), so StyleCop is retained as a partial
  style/documentation layer rather than the central quality gate.
- [JetDatabaseWriter.Tests.csproj](../../JetDatabaseWriter.Tests/JetDatabaseWriter.Tests.csproj)
  disables `RunAnalyzersDuringBuild`, `EnforceCodeStyleInBuild`, and XML
  documentation generation for non-Release builds. That is the right shape for
  a fast test inner loop; Release remains strict.
- `dotnet outdated` reported no outdated direct dependencies on 2026-05-27.

The repo is already using SDK analyzers as the center of gravity. The remaining
questions are about local-build cost, analyzer scope, which older broad
packages should stay local, and which checks belong in a slower CI/CD lane.

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

Verdict: removed from local builds. Bring it back only through an explicit
slower CI/security-lane decision.

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
- SecurityCodeScan's unique value is broad interprocedural taint analysis. That
  is not something [BannedSymbols.txt](../../BannedSymbols.txt) or curated
  `Meziantou.Analyzer` rules can reproduce. For this library, the useful local
  replacements are narrower: SDK security analyzers for common vulnerability
  classes, banned APIs for hard local policy, and Meziantou rules for process
  start, certificate validation, regex timeout, and adjacent correctness checks.

Rule-level coverage review from 2026-05-27:

| SCS rule | What it detects | Coverage or replacement read | Local recommendation |
|----------|-----------------|------------------------------|----------------------|
| `SCS0001` | Command injection through process execution. | Existing bans block ambiguous `Process.Start` overloads, `ProcessStartInfo(string, string)`, and the `ProcessStartInfo.Arguments` shell-style string. SDK security analyzers cover process-command injection patterns. Future Meziantou `MA0161`-`MA0163` can require safer `ProcessStartInfo`/`UseShellExecute` shape, but not full taint replacement. | Keep SCS out locally; keep the process bans and add curated Meziantou process rules later. |
| `SCS0002` | SQL injection. | SDK security analyzers cover SQL-injection patterns. BannedApi cannot model taint, but this pure file-format library should not acquire server-database client surfaces. | Keep SCS out locally; keep SDK coverage and project bans on SQL/ODBC/OleDb connection/command APIs unless a deliberate feature changes that policy. |
| `SCS0003` | XPath injection. | SDK security analyzers cover XPath-injection patterns. BannedApi could ban XPath APIs, but cannot distinguish safe literal queries from unsafe dynamic query construction. | Rely on SDK coverage; only add bans if XPath APIs are never intended in this codebase. |
| `SCS0004` | Disabled certificate validation. | SDK security analyzers cover common certificate-validation bypasses. Future Meziantou `MA0039` also flags custom certificate validation. BannedApi now bans TLS-validation callback APIs because this library should not own network trust policy. | Use SDK plus project bans; no SCS local build need. |
| `SCS0005` | Weak random number generation. | SDK `CA5394` already covers insecure randomness. BannedApi could ban `Random`, but this repo has legitimate fuzz/test use. | Keep SDK coverage and scoped suppressions for non-security randomness. |
| `SCS0006` | Weak hashing such as MD5 or SHA-1. | SDK crypto rules plus existing bans for `MD5` and `SHA1` cover this. Current suppressions are for spec-required legacy formats. | Covered locally without SCS. |
| `SCS0007` | XML external entity processing. | SDK XML security analyzers cover unsafe parser configuration. BannedApi cannot express "configured safely," but it can ban precise escape hatches such as `XmlUrlResolver` and `DtdProcessing.Parse`. | Use SDK coverage plus those precise XML escape-hatch bans; avoid broad file/XML API bans. |
| `SCS0008` | Cookie without `Secure`. | Web-only. BannedApi can only ban cookie APIs wholesale. The local project policy now bans `System.Web.HttpCookie` and ASP.NET controller base types to keep web surface out of the library. | No SCS local-build need; host applications own cookie policy. |
| `SCS0009` | Cookie without `HttpOnly`. | Web-only. Same replacement posture as `SCS0008`. | No SCS local-build need; host applications own cookie policy. |
| `SCS0010` | Weak cipher algorithms such as DES/3DES. | SDK crypto rules plus existing bans for `DES`, `RC2`, `Rijndael`, and `TripleDES` cover this. | Covered locally without SCS. |
| `SCS0011` | Unsafe XSLT settings. | SDK XML/XSLT security coverage is the better fit for taint/configuration. BannedApi now bans `XslCompiledTransform`, `XsltSettings.EnableScript`, and `XsltSettings.TrustedXslt` because XSLT execution is outside this library's scope. | Covered locally without SCS unless a deliberate XSLT feature is introduced. |
| `SCS0012` | ASP.NET controller action missing authorization annotations. | ASP.NET-specific. BannedApi now blocks ASP.NET controller base types as a project-scope guard, not an authorization analyzer replacement. | No fit for this library; host applications own authorization policy. |
| `SCS0013` | Weak or unauthenticated cipher modes. | SDK crypto rules already flag the relevant patterns; BannedApi now bans `CipherMode.CBC` and `CipherMode.ECB` so any manual mode assignment must carry an explicit `RS0030` compatibility suppression. Static `EncryptCbc`/`DecryptCbc` helper use remains covered by SDK crypto rules and existing compatibility suppressions. | Covered locally without SCS for enum mode selection; keep intentional Office/Jet suppressions narrow. |
| `SCS0015` | Hardcoded password passed to known password APIs. | Not well covered by BannedApi unless banning specific password setter APIs. Meziantou does not replace this. Secret scanning or CodeQL/Sonar-style scanning is a better slower-lane fit. | Low local value; use slower-lane secret scanning if desired. |
| `SCS0016` | Missing anti-forgery token on ASP.NET POST actions. | ASP.NET-specific. | No fit for this library. |
| `SCS0017` | ASP.NET request validation disabled by attribute. | ASP.NET-specific. | No fit for this library. |
| `SCS0018` | Path traversal. | SDK security taint analyzers cover file-path injection. BannedApi is a poor replacement because file/path APIs are central to the library and cannot encode trust boundaries. Meziantou does not replace this. | The most plausible lost SCS check, but host applications own user-controlled paths; rely on SDK/local API design. |
| `SCS0019` | ASP.NET output cache conflicts with authorization. | ASP.NET MVC-specific. | No fit for this library. |
| `SCS0021` | `validateRequest=false` in Web.config. | ASP.NET/Web.config-specific. | No fit for this library. |
| `SCS0022` | ASP.NET event validation disabled. | ASP.NET/Web.config-specific. | No fit for this library. |
| `SCS0023` | View state not encrypted. | ASP.NET WebForms-specific. | No fit for this library. |
| `SCS0024` | View state MAC disabled. | ASP.NET WebForms-specific. | No fit for this library. |
| `SCS0026` | LDAP distinguished-name injection. | SDK security analyzers cover LDAP-injection patterns. BannedApi now bans `DirectoryEntry` and `DirectorySearcher` because LDAP is outside this file-format library's scope. Meziantou does not replace this. | Covered locally as a project-scope ban; revisit only if LDAP support is deliberately introduced. |
| `SCS0027` | Open redirect. | Web-specific; SDK security analyzers cover common redirect vulnerabilities. BannedApi and Meziantou are poor replacements. | No fit for this library. |
| `SCS0028` | Insecure deserialization of untrusted data. | SDK deserialization rules plus BannedApi cover the highest-value local policy. The ban list now includes `BinaryFormatter`, `SoapFormatter`, `NetDataContractSerializer`, WebForms serializers, `JavaScriptSerializer`, and dangerous Newtonsoft `TypeNameHandling` enum values. Meziantou is not a replacement. | Covered locally without SCS for known unsafe serializers; use slower-lane scanning for taint/source-flow questions if desired. |
| `SCS0029` | Cross-site scripting. | Web-output-specific; SDK security analyzers cover common XSS classes. BannedApi and Meziantou are not useful replacements. | No fit for this library. |
| `SCS0030` | Request validation mode protects only pages, not all requests. | ASP.NET/Web.config-specific. | No fit for this library. |
| `SCS0031` | LDAP filter injection. | Same posture as `SCS0026`: SDK coverage plus project bans on LDAP entry/search APIs. Meziantou does not replace this. | Covered locally as a project-scope ban; revisit only if LDAP support is deliberately introduced. |
| `SCS0032` | ASP.NET Identity password minimum length too small. | ASP.NET Identity-specific. | No fit for this library. |
| `SCS0033` | ASP.NET Identity password complexity too weak. | ASP.NET Identity-specific. | No fit for this library. |
| `SCS0034` | ASP.NET Identity password required length not set. | ASP.NET Identity-specific. | No fit for this library. |

Actionable interpretation:

- Keep SDK security analyzers as the local default for injection, XML safety,
  deserialization, and crypto categories.
- Keep [BannedSymbols.txt](../../BannedSymbols.txt) focused on symbols that are
  never acceptable here, such as weak crypto types, `BinaryFormatter`, blocking
  APIs, legacy HTTP APIs, and ambiguous process-start overloads.
- Do not try to encode taint-flow checks in BannedApi. It is excellent for
  symbol policy, but it cannot model user-controlled input, sanitizer use, or
  required safe configuration.
- Treat Meziantou as a curated hygiene layer, not a SecurityCodeScan
  replacement. The useful overlap is process-start shape, certificate
  validation, regex timeout/source-generator guidance, stream-read checks,
  awaited disposal, string/culture checks, and cancellation-token forwarding.
- If broad taint/security scanning is desired, run it outside normal local
  builds through CodeQL, SonarAnalyzer/SonarCloud, Puma Scan, or the
  SecurityCodeScan standalone runner in a scheduled/manual lane.

Removal experiment completed on 2026-05-27 after the
`Roslynator.Refactorings` package removal. The removed-state command was:

```powershell
dotnet build JetDatabaseWriter.slnx --configuration Release --no-restore -m -t:Rebuild /p:ReportAnalyzer=true -bl:<binlog> -v:detailed
```

Control state with `SecurityCodeScan.VS2019` still present after
`Roslynator.Refactorings` removal:

| Run | Text log | MSBuild elapsed | Warnings | Errors |
|-----|----------|-----------------|----------|--------|
| 1 | [securitycodescan-control-run1-20260527-144507.log](../../obj/AnalyzerTiming/securitycodescan-control-run1-20260527-144507.log) | `00:00:56.82` | 0 | 0 |
| 2 | [securitycodescan-control-run2-20260527-144605.log](../../obj/AnalyzerTiming/securitycodescan-control-run2-20260527-144605.log) | `00:00:37.11` | 0 | 0 |
| 3 | [securitycodescan-control-run3-20260527-144644.log](../../obj/AnalyzerTiming/securitycodescan-control-run3-20260527-144644.log) | `00:00:29.88` | 0 | 0 |

Removed state:

| Run | Text log | Binary log | Outer stopwatch | MSBuild elapsed | Warnings | Errors |
|-----|----------|------------|-----------------|-----------------|----------|--------|
| 1 | [securitycodescan-removed-run1-20260527-152229.log](../../obj/AnalyzerTiming/securitycodescan-removed-run1-20260527-152229.log) | [securitycodescan-removed-run1-20260527-152229.binlog](../../obj/AnalyzerTiming/securitycodescan-removed-run1-20260527-152229.binlog) | `00:00:14.7702552` | `00:00:14.32` | 0 | 0 |
| 2 | [securitycodescan-removed-run2-20260527-152244.log](../../obj/AnalyzerTiming/securitycodescan-removed-run2-20260527-152244.log) | [securitycodescan-removed-run2-20260527-152244.binlog](../../obj/AnalyzerTiming/securitycodescan-removed-run2-20260527-152244.binlog) | `00:00:15.8959698` | `00:00:15.46` | 0 | 0 |
| 3 | [securitycodescan-removed-run3-20260527-152300.log](../../obj/AnalyzerTiming/securitycodescan-removed-run3-20260527-152300.log) | [securitycodescan-removed-run3-20260527-152300.binlog](../../obj/AnalyzerTiming/securitycodescan-removed-run3-20260527-152300.binlog) | `00:00:16.6763203` | `00:00:16.23` | 0 | 0 |

Result: the removed-state MSBuild median was `15.46s`, compared with the
control median of `37.11s`. That is a median improvement of `21.65s`, or about
`58%`, with no warnings or errors. The removed-state outer stopwatch median was
`15.90s`; the earlier control outer-stopwatch median was about `38.00s`.

The run 1 removed-state analyzer totals by compiler invocation were:

| Compiler invocation | Total analyzer execution time |
|---------------------|-------------------------------|
| `JetDatabaseWriter` `net10.0` | 10.881s |
| `JetDatabaseWriter` `netstandard2.1` | 11.203s |
| `JetDatabaseWriter.Scaffold` `net10.0` | 0.133s |
| `JetDatabaseWriter.Benchmarks` `net10.0` | 0.495s |
| `JetDatabaseWriter.FormatProbe` `net10.0` | 13.767s |
| `JetDatabaseWriter.Tests` `net10.0` | 15.627s |

Decision: keep `SecurityCodeScan.VS2019` out of local builds. The measured
speedup is large, the local warning count stayed at zero, and the rule-level
coverage review above shows that the useful local concerns are already covered
or better handled by SDK analyzers, focused BannedApi policy, and the later
curated Meziantou trial. Broad taint/security scanning remains a separate
slower-lane decision.

Replacement path:

- [x] Remove `SecurityCodeScan.VS2019` from the global build package references
      for the local-build removal experiment.
- [x] Run the same forced Release commands three times in the removed state and
      compare median elapsed time against the control median of `38.00s` from
      the post-`Roslynator.Refactorings` state.
- [x] Capture removed-state analyzer timing, warning/error counts, text logs,
      and binlogs under `obj/AnalyzerTiming`.
- [x] Keep SDK security CA rules enabled for the local-build removal decision.
- [x] Add cheap project-specific security bans to
      [BannedSymbols.txt](../../BannedSymbols.txt) only when the desired rule is
      just "never call this API here"; do not add broad bans for central library
      APIs such as file/path handling or XML unless the symbol is truly
      forbidden.
- [ ] During the later Meziantou trial, include process-start rules, custom
      certificate-validation rules, and regex-timeout rules as the
      security-adjacent overlap that usefully replaces part of SCS locally.
- [x] Defer the broad security/code-smell scanning lane decision until after
      local analyzer-removal experiments are complete. Candidate lanes remain
      CodeQL, SonarAnalyzer/SonarCloud, Puma Scan, SecurityCodeScan standalone,
      or a scheduled CI job.
- [ ] If a Roslyn security package is still wanted locally, trial
      `SonarAnalyzer.CSharp` or another security analyzer in isolation before
      adopting it. Assume it is costly until measured.
- [x] Update this section and the Suggested Order of Work. No
      [README.md](../../README.md) update was needed because it does not name
      `SecurityCodeScan.VS2019`.

Post-SCS local hardening completed on 2026-05-27:

- Wired [BannedSymbols.txt](../../BannedSymbols.txt) into
  `Microsoft.CodeAnalysis.BannedApiAnalyzers` as an `AdditionalFiles` input.
  This made the existing `RS0030` policy active in strict builds.
- Extended the ban list with high-signal project-scope bans for TLS validation
  callbacks, external XML/XSLT execution escape hatches, unsafe serializers,
  SQL/ODBC/OleDb client surfaces, LDAP APIs, and ASP.NET/cookie surface APIs.
- Added explicit SDK security rule severities to [.editorconfig](../../.editorconfig)
  for the injection, XML, deserialization, crypto, certificate-validation,
  insecure-randomness, and web/security categories that replace useful SCS
  local coverage.
- Removed stale `SCS####` pragmas from source. Spec-required MD5/SHA-1 call
  sites now suppress `RS0030` alongside the relevant SDK crypto rule instead.
- Added `CipherMode.CBC` and `CipherMode.ECB` bans as a cheap local stand-in
  for the enum-selection part of `SCS0013`; every existing assignment is now
  paired with an explicit Office/Jet compatibility `RS0030` suppression.
- Added process argument-string bans for `ProcessStartInfo(string, string)` and
  `ProcessStartInfo.Arguments`; existing process launches already use
  `ArgumentList`.
- Replaced newly active banned API hits in production code: byte-range lock
  retry no longer calls `Thread.Sleep`; calculated Access-compatible `DATE`,
  `NOW`, `TIME`, and `TIMER` functions use a narrowly suppressed
  `DateTime.Now` helper instead of hiding local-clock semantics behind
  `DateTimeOffset.Now.LocalDateTime`.
- Extended the ambiguous local-clock bans to include `DateTime.Today` and
  `DateTimeOffset.LocalDateTime`. `DateTimeOffset.Now` remains allowed because
  it preserves the offset; the forbidden shape is collapsing local time back to
  an ambiguous `DateTime`. UTC, offset-preserving `DateTimeOffset`, or an
  explicit clock remain the preferred default, with local-time suppressions
  reserved for external compatibility behavior.
- Verified with `dotnet build JetDatabaseWriter.slnx --configuration Release --no-restore -m`:
  build succeeded in `15.3s`.

### 3. Keep `StyleCop.Analyzers` for Now

Verdict: keep StyleCop in strict local builds for now. Defer any retirement,
replacement, or CI/CD-lane move until the broader analyzer-lane plan is
revisited.

Why:

- The stable StyleCop package is old, and this repo uses a 2023 beta shim that
  resolves `StyleCop.Analyzers.Unstable`.
- StyleCop costs several seconds per target framework in the measured library
  build and about 2.790s in the measured test build, but that cost is not enough
  on its own to justify removing a style policy the repo still wants.
- The current [.editorconfig](../../.editorconfig) suppressions already disable
  many StyleCop opinions, including documentation, ordering, naming, and layout
  rules, so the active policy is already curated rather than one-size-fits-all
  StyleCop.
- Remaining SA-only diagnostics are mostly style and documentation policy rather
  than correctness checks. That makes them a policy choice, not an urgent local
  analyzer-removal target.
- There is no clear actively maintained drop-in replacement for the whole
  StyleCop surface. `Menees.Analyzers` and StyleCopPlus-like ports may replace
  a few preferences, but adopting another broad StyleCop-like package would
  restart the same maintenance and performance question.

Deferred path:

- [x] Keep `StyleCop.Analyzers`, [stylecop.json](../../stylecop.json), and the
      current StyleCop suppressions in place for now.
- [x] Defer the StyleCop inventory/removal experiment until the broader CI/CD
      analyzer-lane decision is revisited.
- [ ] If StyleCop build cost becomes a priority again, inventory current
      StyleCop diagnostics in a strict Release build without assuming removal.
- [ ] Decide as part of CI/CD analyzer-lane planning whether StyleCop stays
      local, moves to CI, or gets narrowed by path or rule.
- [x] Move only obvious, low-cost style policy to [.editorconfig](../../.editorconfig)
  when it reduces churn independently of StyleCop.
- [ ] Remove StyleCop suppressions, [stylecop.json](../../stylecop.json), or the
      package only after a deliberate future decision changes this policy.

Completed on 2026-05-27 and finalized on 2026-05-30: expanded
[.editorconfig](../../.editorconfig) from the StyleCop-oriented Rehan Saeed
template, retained its MIT notice at the bottom, added JetDatabaseWriter-specific
overrides for mixed existing style, moved the global analyzer/style `NoWarn`
policy into explicit `dotnet_diagnostic` entries, and kept only `NU1605` in
[Directory.Build.props](../../Directory.Build.props). The 2026-05-30 strict
editorconfig pass kept the intended "only implicit when apparent" type style,
cleaned remaining Roslyn style drift, and validated with:

```powershell
dotnet format style JetDatabaseWriter.slnx --verify-no-changes --no-restore --verbosity minimal
dotnet build JetDatabaseWriter.slnx --configuration Release --no-restore -m
dotnet test --project JetDatabaseWriter.Tests --filter-not-trait Category=Fuzz
```

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

- [x] Prefer adding precise local bans over adding broad analyzer packages when
      the desired policy is symbol-based.
- [x] Keep the list short and high-signal so `RS0030` remains trusted.

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
- [x] Complete the one-package-at-a-time local-build removal experiment for
      `SecurityCodeScan.VS2019` by capturing removed-state timings and updating
      the final decision.
- [x] Defer the broad security-scanning lane decision for now; revisit CodeQL,
      SonarAnalyzer/SonarCloud, Puma Scan, or a scheduled CI lane after local
      analyzer-removal experiments are complete.
- [x] Keep `StyleCop.Analyzers` for now and defer StyleCop inventory/removal
      work until broader CI/CD analyzer-lane planning.
- [x] Expand [.editorconfig](../../.editorconfig) only for obvious existing
      style policy that can reduce diagnostic churn independently of StyleCop.
- [ ] Trial `AnalysisLevel latest` or category-specific analyzer modes against
      `latest-all` after the current third-party analyzer decisions settle.
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
