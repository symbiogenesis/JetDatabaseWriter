# JetDatabaseWriter

[![NuGet](https://img.shields.io/nuget/v/JetDatabaseWriter.svg)](https://www.nuget.org/packages/JetDatabaseWriter/)
[![Downloads](https://img.shields.io/nuget/dt/JetDatabaseWriter.svg)](https://www.nuget.org/packages/JetDatabaseWriter/)
[![Targets](https://img.shields.io/badge/targets-net10.0%20%7C%20netstandard2.1-blue)](#nuget-target-compatibility)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Fully managed .NET library for reading and writing Microsoft Access (JET/ACE) databases — no OleDB, ODBC, or ACE/Jet driver installation required.

Use it to query, migrate, or generate `.mdb` and `.accdb` files directly from your .NET applications and tools, without installing Microsoft Access.

The [supported operating envelope](docs/support-contract.md) defines the release scope, resource and recovery contracts, and evidence needed to broaden them.

## Contents

- [Installation](#installation)
- [Features](#features)
- [Quick Start](#quick-start)
- [Reading Data](#reading-data)
- [Writing Data](#writing-data)
- [Documentation](#documentation)
- [Encryption Support](#encryption-support)
- [Limitations](#limitations)

## Features

| Support | Feature | Description |
|:---:|---|---|
| ✅ | **Pure&nbsp;managed&nbsp;.NET** | Uses .NET APIs; no OleDB, ODBC, ACE/Jet driver, library P/Invoke, or native helper binaries |
| ✅ | **JET3/JET4&nbsp;&&nbsp;ACE** | Read, write and create Jet3/Jet4 `.mdb` and ACE `.accdb` databases. Pre-Jet3 formats are unsupported. Feature-specific limits apply; see [limitations](#limitations) and the [validation matrix](docs/design/writer-disk-format-validation-matrix.md). |
| ✅ | **Read&nbsp;&&nbsp;write** | Create databases and tables; insert/update/delete rows; add/drop/rename columns |
| ✅ | **Typed&nbsp;values** | `int`, `DateTime`, `decimal`, `Guid`, MEMO, OLE, Hyperlink — not just strings |
| ✅ | **POCO&nbsp;+&nbsp;LINQ** | `Rows<T>("...", o => …)` auto-infers an index; `FromIndex<T>(...)` to override; async LINQ (`Where`/`Take`/`FirstOrDefaultAsync`/…) over `IAsyncEnumerable<T>` |
| ✅ | **IQueryable** | `Query<T>(...)` is an `IQueryable<T>` with `Where`/`OrderBy`/`Skip`/`Take`/`Select`/`Include`+`ThenInclude` (relationship-inferred eager load) and async terminals (`ToListAsync`/`CountAsync`/`FirstAsync`/…); results are async-only |
| ✅ | **Async&#8209;first** | `ValueTask<T>` API, `OpenAsync(...)`, `await using` (`IAsyncDisposable`), `IProgress<T>` callbacks |
| ✅ | **Stream&#8209;based&nbsp;I/O** | Open from any seekable `Stream` (files, byte arrays, blobs, embedded resources) |
| ✅ | **Encryption** | Read/update built-in JET RC4 and ACE CryptoAPI, Standard AES and Agile AES; create JET RC4 or ACE Agile AES and maintain passwords. See [Encryption Support](#encryption-support) for provider and platform scope. |
| ✅ | **Schema&nbsp;features** | Create, rename and drop tables, columns and indexes; selectable text collations; defaults, validation and empty-string constraints; primary & foreign keys with cascade update/delete; linked tables. See [schema scope](docs/writing.md#schema-operations-and-preservation). |
| ✅ | **Complex&nbsp;columns** | Read, add, replace and remove attachments and multi-value items with native item IDs; maintain append-only Memo history (ACCDB) |
| ✅ | **Calculated&nbsp;columns** | Native ACCDB expressions, metadata and cached values; recompute dependent results on insert/update and populate newly added fields, with native error values and regional settings. See [calculated fields](docs/writing.md#calculated-columns). |
| ✅ | **Concurrency** | `.ldb` / `.laccdb` lockfile + BCL page-level byte-range locks where supported |
| ✅ | **Transactions** | Group or nest changes with `CommitAsync` / `RollbackAsync`; file transactions recover after process crashes (see [guarantees](docs/operations.md#transactions)) |
| ✅ | **Storage&nbsp;maintenance** | Access-style free-page reuse, free-page scrubbing, opt-in secure erase, and tail shrinking |
| ✅ | **Performance** | Configurable LRU page cache, default parallel read-ahead for eligible page scans, streams millions of rows without loading the file |

---

### Security

The parser enforces bounds on several page, long-value, attachment and encryption paths. Hostile-file auditing and fuzz coverage remain ongoing; the CVE review is a threat-model inventory, not proof that every attack surface is mitigated. See [the security work](PLAN.md#s--hostile-file-parsing-and-schema-preservation) and [the vulnerability review](docs/cve-vulnerability-analysis.md).

Access/text linked data requires a `LinkedSourceResolver` that supplies an authorized stream. The library reads that exact stream without opening catalog paths or filename-derived sidecars; see [linked-source configuration](docs/operations.md#configuration).

---

### Correctness

The test suite draws from and extends the coverage of [Jackcess](https://jackcess.sourceforge.io/), [mdbtools](https://github.com/mdbtools/mdbtools), [OpenMcdf](https://github.com/ironfede/openmcdf), and Microsoft's [Extensible Storage Engine](https://github.com/microsoft/Extensible-Storage-Engine) for analogous storage-engine risk categories, with additional coverage for corner cases, corruption resilience, and format variants.

Access compatibility is checked with Microsoft-authored fixtures and DAO tests. Coverage is incomplete; see the [writer disk-format validation matrix](docs/design/writer-disk-format-validation-matrix.md) for tested formats and native-engine results, and the [open requirements](PLAN.md) for remaining work.

Beyond functional tests, the codebase is validated by:

- **Strict compiler settings** — Nullable reference types, warnings-as-errors, `WarningLevel 9999`, `AnalysisLevel latest-all` and arithmetic overflow checks apply globally.

- **Static analysis** — Roslyn .NET analyzers, Roslynator, StyleCop and `.editorconfig` code-style rules are enforced as errors in Release library and test builds on every push to main and every pull request.

- **Verified recovery and parsing kernels** — Production recovery and value parsing use generated [Dafny code](verification/README.md) for bounded journal framing, replay identity, replacement field validation, byte authorization and long-value descriptor codecs. Separate models check selected undo, interrupted recovery, replacement and generation-ownership invariants for arbitrary finite model instances. CI verifies proofs, audits trust and checks generated freshness. Remaining C# correspondence and operating-system persistence are [separate obligations](docs/design/formal-verification.md). No verifier or solver ships with the library.

- **Continuous integration** — On every push to main and every pull request, [GitHub Actions](.github/workflows/ci.yml) builds the solution in Release and runs the test suite on Windows with .NET 10 and .NET 11, plus a separate .NET 10 lane for the `netstandard2.1` build. Release tags run the same checks and publish the package built by that run only after it passes.

- **Reproducible builds** — Deterministic compilation via [DotNet.ReproducibleBuilds](https://github.com/dotnet/reproducible-builds); reproducing builds requires matching locked dependencies, SDK, version and build inputs.

- **Access Compact & Repair checks** — DAO-guarded tests cover tables, indexes, relationships and complex values; the [validation matrix](docs/design/writer-disk-format-validation-matrix.md) lists the tested revision, native-engine results and unresolved gaps.

- **Index key fixture parity** — Access-authored fixtures validate long text/MEMO index keys with embedded line breaks: Jet4 (V2000 / V2003 / V2007) is byte-exact; V2010 ACE is byte-exact for the checked-in `Table11` / `Table11_desc` long rows. The V2010 encoder includes DAO-derived 65-character contribution tables for plain, auxiliary, row10, row11 and row12 long-row suffix contexts. Probes found zero mismatches across exported matrices and observed double-space sweeps. See [GeneralLegacyEncoderFixtureTests.cs](tests/JetDatabaseWriter.Tests/Indexes/Collation/GeneralLegacyEncoderFixtureTests.cs) and [GeneralEncoderFixtureTests.cs](tests/JetDatabaseWriter.Tests/Indexes/Collation/GeneralEncoderFixtureTests.cs).

- **Fuzz testing** — An explicit hosted workflow runs deterministic corpus mutations and truncations under per-process time and memory limits, retaining failure reproductions and logs.

- **Memory safety analysis** — [InferSharp v1.5](https://github.com/microsoft/infersharp/releases/tag/v1.5) analyzes both library targets in its [own workflow](.github/workflows/infersharp.yml), available only by manual dispatch (`gh workflow run infersharp.yml --ref main`). Unreviewed findings or empty translation fail the lane; partial method translation limits coverage. Text and SARIF reports and translation coverage are retained as `infersharp-<framework>` artifacts. The [reviewed exceptions](.github/infersharp-baseline.json) identify exact false positives, document ownership or synchronization evidence and pin reviewed source hashes. New findings, changed reviewed source or obsolete exceptions fail the gate.

---

## Quick Start

### Installation

```bash
dotnet add package JetDatabaseWriter
```

```powershell
Install-Package JetDatabaseWriter
```

### NuGet target compatibility

The package targets .NET 10 and .NET Standard 2.1. Applications on .NET Core 3.x and .NET 5–9 use the .NET Standard build; .NET Framework is not supported. See [runtime and NativeAOT guidance](docs/operations.md#runtime-compatibility) for details.

### Reading Data

Open an existing database, list its tables, and read rows into a small C# model. This example expects an `Orders` table; change the path, table name and properties to match your file.

```csharp
using JetDatabaseWriter;

await using var reader = await AccessReader.OpenAsync("database.mdb");

var tables = await reader.ListTablesAsync();
Console.WriteLine($"Tables: {string.Join(", ", tables)}");

var orders = await reader.ReadTableAsync<Order>("Orders", maxRows: 100);
foreach (var order in orders)
    Console.WriteLine($"#{order.OrderID}: {order.Freight:C}");

public class Order
{
    public int OrderID { get; set; }
    public decimal Freight { get; set; }
}
```

Properties match column names without regard to case. For large tables, stream rows with `await foreach (var order in reader.Rows<Order>("Orders"))` instead of loading a list. The [reading guide](docs/reading.md) covers filtering, indexes, relationships, attachments and other column types.

### Writing Data

Create a new database and insert a row using column names. The destination file must not already exist.

```csharp
using JetDatabaseWriter;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;

await using var writer = await AccessWriter.CreateDatabaseAsync(
    "contacts.mdb", DatabaseFormat.Jet4Mdb);

await writer.CreateTableAsync("Contacts", new[]
{
    new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true },
    new ColumnDefinition("Name", typeof(string), maxLength: 100),
});

await writer.InsertRowAsync("Contacts", new RowValues
{
    ["Id"] = 1,
    ["Name"] = "Alice",
});
```

To edit an existing file, open it with `AccessWriter.OpenAsync("contacts.mdb")`. Use one writer per file and dispose it when finished; `await using` does this automatically. The [writing guide](docs/writing.md) covers updates, deletes, schema changes and constraints, and the [operations guide](docs/operations.md#transactions) explains transactions.

## Documentation

See [PLAN.md](PLAN.md) for open work, dependencies and acceptance criteria across all workstreams, including formal verification.

| I want to… | Guide |
|---|---|
| Read, filter or stream data | [Reading and querying](docs/reading.md) |
| Insert or edit data and change schemas | [Writing data](docs/writing.md) |
| Use streams, configure options or handle failures | [Database operations](docs/operations.md) |
| Generate C# models from an existing database | [Scaffolding](docs/scaffolding.md) |
| Understand Access terminology and file formats | [Glossary](docs/glossary.md) and [design notes](docs/design/) |
| Assess recent proofs and algorithms against this library | [Proof and algorithm survey](docs/design/proof-survey.md) |

## Encryption Support

Pass a password when opening a protected database:

```csharp
using JetDatabaseWriter;

await using var reader = await AccessReader.OpenAsync(
    "protected.accdb", new AccessReaderOptions("your-password"));
```

Writers accept the password through `AccessWriterOptions`.

| Format | Current support |
|---|---|
| Jet4 password protection and RC4 encryption | Read, update, create, encrypt/decrypt and change passwords, including native security identity remasking and rebuilding indexed Owner/SID fields. |
| ACE Agile encryption | Read and update AES-128/192/256 with CBC or CFB8 and SHA-1/256/384/512; create and maintain using AES-256-CBC/SHA-512. Microsoft-compacted fixtures cover AES-192, CFB8, SHA-256 and SHA-384 as well as the default algorithms, including distinct password and page parameters. |
| Jet3 RC4 encryption and password protection | Native code-page passwords, page updates and maintenance; DAO 3.6 fixtures and read/write/compact checks cover Access 97. |
| ACE Standard and RC4 CryptoAPI providers | Read and update native pages, including 50,000-iteration Standard and compatibility AES. Microsoft DAO read/write/compact checks and a native Standard output fixture verify interoperability. |

`AccessWriter.CreateDatabaseAsync` encrypts a new database when `AccessWriterOptions.Password` is nonempty, before writing the initial database image. Use `AccessDatabaseEncryption.EncryptAsync`, `DecryptAsync` and `ChangePasswordAsync` for file maintenance. These fully managed operations stream pages into an adjacent temporary file and replace the original after successful completion. Use a containing directory protected from untrusted access; see [maintenance permissions and durability](docs/operations.md#encryption-options). Ordinary updates preserve the original encryption provider and key. Password maintenance preserves JET password-only mode; ACE password changes use fresh AES-256-CBC/SHA-512 encryption.

The support status covers the built-in providers listed above. Third-party extensible providers, non-AES Agile ciphers and Office encrypted compound packages are not supported database inputs. Custom workgroup identities are preserved; the library does not authenticate workgroup users. See the [native encryption evidence](docs/design/native-encryption-evidence.md) for fixture provenance and the [encryption options](docs/operations.md#encryption-options) for resource limits, replacement recovery and platform durability limits.

## Limitations

- **Keep the file stable while reading, and use one writer per file.** A reader caches data and does not refresh it or provide a consistent snapshot of a changing file. Reopen it after changes. Lockfiles help coordinate access but do not exclude every external writer on every platform.
- **Crash recovery requires the database and its journal.** File transactions and physical shrinking recover interrupted writes on writer reopen; keep the adjacent `.jdw-journal` with the database until recovery finishes. Custom streams provide [live rollback under the caller-stream contract](docs/design/caller-stream-recovery.md); power-loss guarantees remain unqualified. See [transaction guarantees](docs/operations.md#transactions).
- **Access feature coverage varies.** Third-party encryption providers, some index formats and some expressions are unsupported. For example, a default or validation rule using `DLookUp` refuses the write. See the [compatibility evidence](docs/design/writer-disk-format-validation-matrix.md) and [detailed limits](docs/operations.md#limitations).
- **Access size and encoding constraints still apply.** Tables are limited to 255 columns; rows must fit the supported page layout. Jet3 text must fit the database's code page. Large MEMO and binary OLE values can use separate pages; see [size limits](docs/operations.md#table-and-row-size).
- **Storage maintenance is limited.** `ShrinkDatabaseAsync` removes free pages at the end of a file; it does not perform a full Access Compact & Repair. Deleted data is not securely erased by default; see [storage maintenance](docs/writing.md#storage-maintenance-and-secure-erase).
- **This is a data library.** It supports LINQ queries, but does not execute SQL, saved Access queries, forms, reports, macros or VBA, and does not provide an ODBC driver. Complete preservation of Access application objects during edits is not established.

## How It Works

JetDatabaseWriter reads and writes the database's pages directly: the catalog identifies tables, table definitions describe columns and indexes, and data pages hold rows. Large values use separate page chains. See the [library structure](docs/design/library-structure.md) and [format notes](docs/design/) for the details.

## Contributing

Issues and pull requests are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for setup, development and validation guidance.

## License

MIT — see [LICENSE](LICENSE) for details.
