---
description: "How to run a filtered subset of xUnit v3 tests in this repo (Microsoft Testing Platform). Use when the user asks to run a specific test, class, namespace, or trait, or when only some tests need to be executed."
applyTo: "**/*Tests*/**,**/*.Tests.csproj"
---

# Running filtered xUnit v3 tests in this repo

This repo uses **xUnit v3** (`xunit.v3` package) running on the **Microsoft Testing Platform (MTP)**, not VSTest. MTP mode is enabled via [global.json](../../global.json):

```json
{ "test": { "runner": "Microsoft.Testing.Platform" } }
```

The `dotnet --version` SDK is 10.x, so MTP CLI options are passed **directly** to `dotnet test` — **do not** use the `--` separator (that form is only for SDK 8/9).

## Common filter switches

Use the `runTests` tool when possible. When invoking the CLI manually, prefer these MTP filter switches:

| Goal | Command |
|---|---|
| Run one fully-qualified test method | `dotnet test --project JetDatabaseWriter.Tests --filter-method "JetDatabaseWriter.Tests.Core.AccessReaderTests.ReadTable_ReturnsRows"` |
| Run all tests in a class | `dotnet test --project JetDatabaseWriter.Tests --filter-class "JetDatabaseWriter.Tests.Core.AccessReaderTests"` |
| Run all tests in a namespace | `dotnet test --project JetDatabaseWriter.Tests --filter-namespace "JetDatabaseWriter.Tests.Internal"` |
| Run by trait | `dotnet test --project JetDatabaseWriter.Tests --filter-trait "Category=Encryption"` |
| Exclude a class / method / namespace / trait | `--filter-not-class`, `--filter-not-method`, `--filter-not-namespace`, `--filter-not-trait` |
| Stop on first failure | `--stop-on-fail on` |
| List the available switches | `dotnet test --project JetDatabaseWriter.Tests -?` |

Notes:
- Multiple values are space-separated after a single switch: `--filter-class Foo Bar` (not repeated like xUnit v2).
- Names must be **fully qualified** (`Namespace.Class.Method`).
- The legacy VSTest `--filter "FullyQualifiedName~..."` syntax does **not** work in MTP mode.
- Do not use `dotnet test ... -- --xunit-list classes` or other `--`-prefixed forwarding; on SDK 10 the args go directly. (`--xunit-list` was removed; use `--list-tests` to discover tests.)

## Discovering tests

```pwsh
dotnet test --project JetDatabaseWriter.Tests --list-tests
```

Pipe through `Select-String` to grep for a partial name before constructing a filter.

## Reference
- xUnit v3 MTP options: https://xunit.net/docs/getting-started/v3/microsoft-testing-platform
- MTP CLI reference: https://learn.microsoft.com/dotnet/core/testing/microsoft-testing-platform-cli-options
