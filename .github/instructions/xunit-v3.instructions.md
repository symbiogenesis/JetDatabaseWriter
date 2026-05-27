---
description: "Use when: writing, modifying, or running xUnit v3 tests in this repo; includes Microsoft Testing Platform commands and xUnit v3 gotchas."
applyTo: "**/*Tests*/**,**/*.Tests.csproj"
---

# xUnit v3 Tests

## Must Follow

- This repo uses xUnit v3 (`xunit.v3`, stable 3.x) on Microsoft Testing Platform, configured in [global.json](../../global.json). Do not suggest the xUnit 4.x prerelease line.
- Test projects are executables. Keep `<OutputType>Exe</OutputType>` in test project files.
- Use `using Xunit;`. Do not add `using Xunit.Abstractions;` or package references to `xunit.runner.visualstudio`, `xunit.abstractions`, or `xunit.assert`.
- Use `dotnet test --project JetDatabaseWriter.Tests` as the base CLI command, adding the MTP options listed below as needed.
- The repo uses SDK 10.x, so MTP options are passed directly to `dotnet test`. Never use a `--` separator before MTP options.

## Run Tests

| Goal | Command |
|---|---|
| Run one fully-qualified method | `dotnet test --project JetDatabaseWriter.Tests --filter-method "JetDatabaseWriter.Tests.Core.AccessReaderCatalogTests.ListTables_WhenDatabaseHasTables_ReturnsNonEmptyList"` |
| Run all tests in a class | `dotnet test --project JetDatabaseWriter.Tests --filter-class "JetDatabaseWriter.Tests.Core.AccessReaderCatalogTests"` |
| Run all tests in a namespace | `dotnet test --project JetDatabaseWriter.Tests --filter-namespace "JetDatabaseWriter.Tests.Internal"` |
| Exclude class / method / namespace | `--filter-not-class`, `--filter-not-method`, `--filter-not-namespace` |
| Run or exclude trait/category | `--filter-trait Category=Fuzz`, `--filter-not-trait Category=Fuzz` |
| Run explicit fuzz harnesses | `dotnet test --project JetDatabaseWriter.Tests --filter-trait Category=Fuzz --explicit only` |
| Stop on first failure | `dotnet test --project JetDatabaseWriter.Tests --stop-on-fail on` |
| List tests | `dotnet test --project JetDatabaseWriter.Tests --list-tests` |
| List switches | `dotnet test --project JetDatabaseWriter.Tests -?` |

- Prefer fully-qualified names (`Namespace.Class.Method`) for unambiguous filters.
- Multiple filter values are space-separated after one switch, for example `--filter-class Foo Bar`; do not repeat the same switch for each value.
- Discover tests with `--list-tests`, then pipe through `Select-String` for a partial name before constructing a filter.
- This repo uses `[Fact(Explicit = true)]` and `[Trait("Category", "Fuzz")]` for open-ended SharpFuzz harnesses. A plain `dotnet test --project JetDatabaseWriter.Tests` run skips them by default; run `test: fuzz` or `dotnet test --project JetDatabaseWriter.Tests --filter-trait Category=Fuzz --explicit only` only when you intend to run the open-ended harnesses.
- Use `--xunit-info` only when you need xUnit's native discovery/run banner.

## Writing Tests

- Prefer primary constructors for fixture and output injection:

```csharp
public class MyTests(DatabaseCache db, ITestOutputHelper output) : IClassFixture<DatabaseCache>
```

- `ITestOutputHelper`, `IAsyncLifetime`, assertions, theory data, and fixtures come from the xUnit v3 packages through the `Xunit` namespace.
- Test methods can return `Task` or `ValueTask`. Cleanup-only fixtures can use `IAsyncDisposable` directly.
- To disable parallelism for a class, use `[Collection(DisableParallelization = true)]`.
- `[InlineData]` has stricter compile-time type checking than xUnit v2; `TheoryData<T>`, `MemberData`, and `ClassData` remain valid.

## Avoid

- VSTest filter syntax such as `--filter "FullyQualifiedName~..."`.
- Forwarded MTP options such as `dotnet test ... -- --filter-class ...`.
- `--nologo`; under xUnit v3 with MTP it is an unknown option. Use `--verbosity quiet` if you need quieter build output.
- xUnit v2 package or namespace patterns.

## Reference

- xUnit v3 MTP options: https://xunit.net/docs/getting-started/v3/microsoft-testing-platform
- MTP CLI reference: https://learn.microsoft.com/dotnet/core/testing/microsoft-testing-platform-cli-options
