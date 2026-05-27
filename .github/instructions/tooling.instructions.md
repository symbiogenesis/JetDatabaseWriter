---
description: "Use when: working in this repository with tooling, diagnostics, benchmarks, static-analysis fixes, PowerShell rewrites, or non-Release test builds."
applyTo: "**"
---

# Repository Tooling Conventions

- Do not use Python for implementation, diagnostics, file inspection, ad hoc scripting, or session-only helper work in this repository. Prefer PowerShell, .NET tooling, repository test tasks, `rg`, and built-in VS Code tools.
- For bulk PowerShell rewrites, prefer `[System.IO.File]::WriteAllText(...)` or otherwise normalize trailing newlines. `Set-Content` after `Get-Content -Raw` can introduce an extra blank line at EOF.
- InferSharp/Pulse may miss `await using` disposal or resource ownership that flows through helper coordinators. Prefer direct `try` / `finally` disposal and direct resource-owner methods for analyzer-facing fixes.
- For writer snapshot readers, use `AccessReader.OpenUncachedAsync` so transient stream readers do not allocate LRU caches that Pulse reports as leaked.
- Checked arithmetic is enabled globally in `Directory.Build.props` through `<CheckForOverflowUnderflow>true</CheckForOverflowUnderflow>`. Narrowing casts whose source can exceed the target range, such as `(ushort)uintValue` or `(byte)(a + b)`, throw `OverflowException` at runtime. Use `unchecked { ... }` or mask explicitly, such as `value & 0xFFFFu`, especially in hash, CRC, XOR, and shift-heavy code.
- `JetDatabaseWriter.Tests` disables `RunAnalyzersDuringBuild`, `EnforceCodeStyleInBuild`, and `GenerateDocumentationFile` for non-Release builds. Keep Release builds strict unless the user explicitly asks for faster Release diagnostics.
- `JetDatabaseWriter.FormatProbe` does not currently disable analyzers in its project file; do not assume it has the test-project speed settings.
- BenchmarkDotNet does warming passes by default, so there's no need to perform a separate warmup run before benchmarking. If you want to disable warmup, set `WarmupCount=0` in the benchmark configuration.
