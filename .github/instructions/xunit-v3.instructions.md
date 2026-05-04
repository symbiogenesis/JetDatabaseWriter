---
description: "xUnit v3 conventions, gotchas, and how to run filtered tests in this repo (Microsoft Testing Platform). Use whenever writing, modifying, or running xUnit tests — or when the user asks about test patterns, fixtures, assertions, or filtering."
applyTo: "**/*Tests*/**,**/*.Tests.csproj"
---

# xUnit v3 in this repo

This repo uses **xUnit v3** (`xunit.v3` package, currently pinned to **3.2.2** — the latest stable release) running on the **Microsoft Testing Platform (MTP)**, not VSTest. The 4.x line is pre-release only (`4.0.0-pre.*`); this repo intentionally stays on the 3.x stable line, so do not suggest upgrading to 4.x. xUnit v3 is a **major breaking change** from v2 — many familiar packages, namespaces, and CLI patterns no longer exist. Read the gotchas below before writing or modifying tests.

MTP mode is enabled via [global.json](../../global.json):

```json
{ "test": { "runner": "Microsoft.Testing.Platform" } }
```

The `dotnet --version` SDK is 10.x, so MTP CLI options are passed **directly** to `dotnet test` — **do not** use the `--` separator (that form is only for SDK 8/9).

## Running tests — MTP filter switches

Use the `runTests` tool when possible. When invoking the CLI manually, prefer these MTP filter switches:

| Goal | Command |
|---|---|
| Run one fully-qualified test method | `dotnet test --project JetDatabaseWriter.Tests --filter-method "JetDatabaseWriter.Tests.Core.AccessReaderCatalogTests.ListTables_WhenDatabaseHasTables_ReturnsNonEmptyList"` |
| Run all tests in a class | `dotnet test --project JetDatabaseWriter.Tests --filter-class "JetDatabaseWriter.Tests.Core.AccessReaderCatalogTests"` |
| Run all tests in a namespace | `dotnet test --project JetDatabaseWriter.Tests --filter-namespace "JetDatabaseWriter.Tests.Internal"` |
| Exclude a class / method / namespace | `--filter-not-class`, `--filter-not-method`, `--filter-not-namespace` |
| Stop on first failure | `--stop-on-fail on` |
| List the available switches | `dotnet test --project JetDatabaseWriter.Tests -?` |

> The repo does not currently use xUnit `[Trait]` attributes, so `--filter-trait` / `--filter-not-trait` have nothing to match. They remain available if traits are added later.

Notes:
- Multiple values are space-separated after a single switch: `--filter-class Foo Bar` (not repeated like xUnit v2).
- Prefer **fully-qualified** names (`Namespace.Class.Method`) for unambiguous matches.
- The legacy VSTest `--filter "FullyQualifiedName~..."` syntax does **not** work in MTP mode.
- Do not use `dotnet test ... -- --filter-class ...` or other `--`-prefixed forwarding; on SDK 10 the args go directly to `dotnet test`.
- Use the built-in MTP `--list-tests` switch to discover tests. To see xUnit's native discovery/run banner alongside MTP output, use `--xunit-info`.
- `--nologo` is **not** a valid option under xUnit v3 with MTP and will cause `dotnet test` to fail with an unknown-option error. It was a VSTest-era switch; omit it. To quiet build output, use `--verbosity quiet` (a `dotnet test` build option) instead.

## Discovering tests

```pwsh
dotnet test --project JetDatabaseWriter.Tests --list-tests
```

Pipe through `Select-String` to grep for a partial name before constructing a filter.

## xUnit v3 namespace changes

The `Xunit.Abstractions` namespace/package **does not exist** in xUnit v3. It was an xUnit v2 artifact. Key differences:

- `ITestOutputHelper` → replaced by `ITestOutputHelper` in the `Xunit` namespace (from `xunit.v3.core`). The interface name is the same but lives in a different namespace and assembly.
- `IMessageSink`, `ISourceInformation`, and other v2 abstraction interfaces are gone entirely.
- Do **not** add a `using Xunit.Abstractions;` directive or reference the `xunit.abstractions` NuGet package — neither exists for v3.
- Test output in xUnit v3 is injected via constructor: `public MyTests(ITestOutputHelper output)` with `using Xunit;` only.

## Key differences from xUnit v2

### Package references

| v2 | v3 |
|---|---|
| `xunit` + `xunit.runner.visualstudio` | **`xunit.v3`** (single meta-package, includes runner) |
| `xunit.abstractions` | ❌ Does not exist |

Do **not** add `xunit.runner.visualstudio` — MTP replaces it entirely.

### Test project is an Exe

xUnit v3 test projects **must** use `<OutputType>Exe</OutputType>`. They are self-contained executables, not class libraries loaded by a separate runner. If `OutputType` is missing or set to `Library`, tests will not be discovered.

### Constructor injection — primary constructors preferred

xUnit v3 supports C# primary constructors for fixture and output injection:

```csharp
public class MyTests(DatabaseCache db, ITestOutputHelper output) : IClassFixture<DatabaseCache>
```

This replaces the v2 pattern of a constructor body + field assignment.

### IAsyncLifetime changes

In v2, `IAsyncLifetime` had `InitializeAsync()` and `DisposeAsync()`. In v3 the interface is the same but comes from `Xunit` namespace (not `Xunit.Abstractions`). Ensure you only have `using Xunit;`.

### Assertions — same API, new assembly

`Assert.*` methods are unchanged in signature but now live in `xunit.v3.assert` (pulled in transitively by the `xunit.v3` meta-package). No code changes needed, but do **not** reference `xunit.assert` (the v2 package) separately.

### TheoryData and generic theory data

`TheoryData<T>` / `TheoryData<T1,T2>` etc. still work identically. `MemberData` and `ClassData` are unchanged. However, `[InlineData]` now performs stricter compile-time type checking on arguments.

### No more `[Collection]` for parallelism control

xUnit v3 defaults to **per-test-method parallelism** (not per-class as in v2). To disable parallelism for a class, use:

```csharp
[Collection(DisableParallelization = true)]
```

The v2 pattern of named collections with `[CollectionDefinition]` still works but is no longer needed for simple "don't run these in parallel" scenarios.

### Async tests

All test methods can be `async Task` or `async ValueTask`. xUnit v3 also supports `async` disposable fixtures via `IAsyncDisposable` directly (no separate `IAsyncLifetime` needed for cleanup-only scenarios).

### Trait filtering

`[Trait("Category", "Fuzz")]` works the same as v2, but filtering is via MTP switches (`--filter-trait` / `--filter-not-trait`) rather than the old VSTest `--filter "Category=Fuzz"` syntax.

## Reference
- xUnit v3 MTP options: https://xunit.net/docs/getting-started/v3/microsoft-testing-platform
- MTP CLI reference: https://learn.microsoft.com/dotnet/core/testing/microsoft-testing-platform-cli-options
