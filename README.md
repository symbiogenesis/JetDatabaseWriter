# JetDatabaseWriter

[![NuGet](https://img.shields.io/nuget/v/JetDatabaseReader.svg)](https://www.nuget.org/packages/JetDatabaseReader/)
[![Downloads](https://img.shields.io/nuget/dt/JetDatabaseReader.svg)](https://www.nuget.org/packages/JetDatabaseReader/)
[![.NET Standard](https://img.shields.io/badge/.NET%20Standard-2.1-blue)](https://docs.microsoft.com/en-us/dotnet/standard/net-standard)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Pure-managed .NET library for reading and writing Microsoft Access JET databases — no OleDB, ODBC, or ACE/Jet driver installation required.

> See [CHANGELOG.md](CHANGELOG.md) and the [migration guide](#migration-from-v1) for breaking changes from v1.

---

## Features

| | |
|---|---|
| ✅ **No native dependencies** | Pure C# — runs anywhere .NET runs |
| ✅ **Jet3 & Jet4 / ACE** | Access 97 through Access 2019 (`.mdb` / `.accdb`) |
| ✅ **Typed by default** | `int`, `DateTime`, `decimal`, `Guid` — not just strings |
| ✅ **All column types** | Text, Integer, Currency, Date/Time, GUID, MEMO, OLE Object, Decimal |
| ✅ **Streaming API** | Process millions of rows without loading the whole file |
| ✅ **Async support** | Async-first `ValueTask<T>` API for all major operations |
| ✅ **Async lifetime** | `OpenAsync(...)` + `await using` (`IAsyncDisposable`) for reader/writer |
| ✅ **Page cache** | 256-page LRU cache (~1 MB, configurable) |
| ✅ **Generic POCO mapping** | `ReadTable<T>()`, `StreamRows<T>()`, `InsertRow<T>()` — no manual casting |
| ✅ **Fluent query** | `Query().Where().Take().Execute()` — typed and string chains |
| ✅ **Progress reporting** | `IProgress<int>` callbacks on all long operations |
| ✅ **Non-Western text** | Code page auto-detected from the database header |
| ✅ **OLE Objects** | Detects embedded JPEG, PNG, PDF, ZIP, DOC, RTF |
| ✅ **Write support** | Create/drop tables, insert/update/delete rows (Jet4/ACE) |
| ✅ **Jet3 encryption** | Transparent page-level XOR decryption for Access 97 `.mdb` databases |
| ✅ **Password verification** | Jet4 `.mdb` and legacy password-only `.accdb` (ACE CompactDatabase `;pwd=`) |
| ✅ **Linked table metadata** | `ListLinkedTables()` returns source paths and foreign names |
| ✅ **Lockfile support** | Creates `.ldb` / `.laccdb` lockfile on open, deletes on disposal (opt-out) |

---

## Installation

```bash
dotnet add package JetDatabaseReader
```

```powershell
Install-Package JetDatabaseReader
```

### NuGet target compatibility

`JetDatabaseReader` targets **`netstandard2.0`**, which is consumed by every current .NET surface:

| Consumer | Minimum version |
|----------|----------------|
| .NET Framework | 4.6.1 |
| .NET Core | 2.0 |
| .NET | 5 / 6 / 7 / 8 / 9 |
| Mono / Xamarin | All |
| Unity | 2018.1+ |
| UWP | 10.0.16299+ |

---

## Quick Start

```csharp
using JetDatabaseReader;
using System.Threading;

public class Order
{
    public int OrderID { get; set; }
    public DateTime OrderDate { get; set; }
    public decimal Freight { get; set; }
}

using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
await using var reader = await AccessReader.OpenAsync("database.mdb", cancellationToken: cts.Token);

List<string> tables = await reader.ListTablesAsync(cts.Token);
Console.WriteLine($"Found {tables.Count} tables: {string.Join(", ", tables)}");

List<Order> orders = await reader.ReadTableAsync<Order>("Orders", maxRows: 100, cts.Token);
foreach (Order o in orders)
    Console.WriteLine($"#{o.OrderID}  {o.OrderDate:yyyy-MM-dd}  {o.Freight:C}");
```

Synchronous methods are still available, but `OpenAsync(...)` + `await using` is the recommended default for new code.

---

## Reading Data

### Generic POCO mapping — recommended

```csharp
public class Product
{
    public int ProductID { get; set; }
    public string ProductName { get; set; }
    public decimal UnitPrice { get; set; }
    public bool Discontinued { get; set; }
}

List<Product> products = await reader.ReadTableAsync<Product>("Products", maxRows: 100, cancellationToken);
decimal total = products.Where(p => !p.Discontinued).Sum(p => p.UnitPrice);
```

Property names are matched to column headers **case-insensitively**. Unmatched properties keep their default value. The type `T` must be a class with a parameterless constructor.

### Typed DataTable

```csharp
DataTable dt = await reader.ReadTableAsync("Products", cancellationToken: cancellationToken);
// dt.Columns["ProductID"].DataType    == typeof(int)
// dt.Columns["UnitPrice"].DataType    == typeof(decimal)
// dt.Columns["Discontinued"].DataType == typeof(bool)
```

### Table preview with schema

```csharp
TableResult preview = await reader.ReadTableAsync("Products", maxRows: 20, cancellationToken);
foreach (TableColumn col in preview.Schema)
{
    Type   clrType = col.Type;            // e.g. typeof(int), typeof(string)
    string display = col.Size.ToString(); // e.g. "4 bytes", "255 chars", "LVAL"
    Console.WriteLine($"{col.Name}: {clrType.Name} ({col.Size})");
}

// Convert to DataTable with CLR-typed columns
DataTable dt = preview.ToDataTable();
// dt.Columns["UnitPrice"].DataType == typeof(decimal)
```

### String variants — compatibility

```csharp
// All values as strings
StringTableResult preview = await reader.ReadTableAsStringsAsync("Products", maxRows: 20, cancellationToken);

// String preview with row access
string firstCell = preview.Rows[0][0];  // always a string
```

---

## Streaming Large Tables

### Generic streaming — recommended

```csharp
var progress = new Progress<int>(n => Console.Write($"\r{n:N0} rows"));

foreach (Product p in reader.StreamRows<Product>("Products", progress))
    Console.WriteLine($"{p.ProductName}: {p.UnitPrice:C}");
```

### Typed object array streaming

```csharp
foreach (object[] row in reader.StreamRows("BigTable"))
{
    int     id  = (int)row[0];
    decimal val = row[2] == DBNull.Value ? 0m : (decimal)row[2];
}
```

### String streaming — compatibility

```csharp
foreach (string[] row in reader.StreamRowsAsStrings("BigTable"))
    Console.WriteLine(string.Join(", ", row));
```

Null values in typed `object[]` rows surface as `DBNull.Value`.

---

## Fluent Query API

```csharp
// Generic POCO result
Order? first = reader.Query("Orders")
    .Where(row => row[2] is DateTime d && d.Year == 2024)
    .Take(10)
    .FirstOrDefault<Order>();

List<Order> recent = reader.Query("Orders")
    .Where(row => row[2] is DateTime d && d.Year == 2024)
    .Execute<Order>()
    .ToList();

// Object array chain
int count = reader.Query("OrderDetails")
    .Where(row => row[3] is decimal p && p > 100m)
    .Count();

// String chain
IEnumerable<string[]> rows = reader.Query("Orders")
    .WhereAsStrings(row => row[2].StartsWith("2024"))
    .Take(50)
    .ExecuteAsStrings();
```

---

## Async Operations

```csharp
using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
await using var reader = await AccessReader.OpenAsync("database.mdb", cancellationToken: cts.Token);

List<Order>                   orders = await reader.ReadTableAsync<Order>("Orders", 50, cts.Token);
List<string>                  tables = await reader.ListTablesAsync(cts.Token);
DataTable                     dt     = await reader.ReadTableAsync("Orders", cancellationToken: cts.Token);
TableResult                   typed  = await reader.ReadTableAsync("Orders", 50, cts.Token);
StringTableResult             str    = await reader.ReadTableAsStringsAsync("Orders", 50, cts.Token);
DatabaseStatistics            stats  = await reader.GetStatisticsAsync(cts.Token);
Dictionary<string, DataTable> all    = await reader.ReadAllTablesAsync(cancellationToken: cts.Token);
Dictionary<string, DataTable> allStr = await reader.ReadAllTablesAsStringsAsync(cancellationToken: cts.Token);
```

All async APIs return `ValueTask<T>` and can be awaited directly. Reader/writer instances also implement `IAsyncDisposable`, so prefer `await using`.

---

## Bulk Operations

```csharp
// Typed columns
Dictionary<string, DataTable> all = await reader.ReadAllTablesAsync(
    new Progress<string>(t => Console.WriteLine($"Reading {t}...")),
    cancellationToken);

// String columns (compatibility)
Dictionary<string, DataTable> allStr = await reader.ReadAllTablesAsStringsAsync(
    cancellationToken: cancellationToken);
```

---

## Writing Data

> Requires Jet4/ACE format — `.mdb` (Access 2000+) or `.accdb`.

```csharp
await using var writer = await AccessWriter.OpenAsync("database.mdb");
```

`AccessWriter.Open(...)` is still available for synchronous workflows.

### Create & drop tables

```csharp
writer.CreateTable("Contacts", new[]
{
    new ColumnDefinition("ContactID", typeof(int)),
    new ColumnDefinition("Name",      typeof(string), maxLength: 100),
    new ColumnDefinition("Email",     typeof(string), maxLength: 255),
    new ColumnDefinition("Score",     typeof(decimal)),
});

writer.DropTable("Contacts");
```

### Insert rows — generic POCO

```csharp
public class Contact
{
    public int ContactID { get; set; }
    public string Name { get; set; }
    public string Email { get; set; }
    public decimal Score { get; set; }
}

writer.InsertRow("Contacts", new Contact { ContactID = 1, Name = "Alice", Email = "alice@example.com", Score = 95.5m });

writer.InsertRows("Contacts", new[]
{
    new Contact { ContactID = 2, Name = "Bob",   Email = "bob@example.com",   Score = 88.0m },
    new Contact { ContactID = 3, Name = "Carol", Email = "carol@example.com", Score = 92.3m },
});
```

### Insert rows — object array

```csharp
writer.InsertRow("Contacts", new object[] { 4, "Dave", "dave@example.com", 77.1m });

writer.InsertRows("Contacts", new[]
{
    new object[] { 5, "Eve",   "eve@example.com",   91.0m },
    new object[] { 6, "Frank", "frank@example.com", 85.4m },
});
```

### Update & delete

```csharp
int updated = writer.UpdateRows("Contacts", "ContactID", 1,
    new Dictionary<string, object> { ["Score"] = 99.9m });

int deleted = writer.DeleteRows("Contacts", "ContactID", 3);
```

---

## Statistics & Metadata

```csharp
foreach (ColumnMetadata col in reader.GetColumnMetadata("Orders"))
    Console.WriteLine($"{col.Ordinal}. {col.Name} — {col.TypeName} ({col.ClrType.Name})");

// Table-level stats (single catalog scan)
foreach (TableStat s in reader.GetTableStats())
    Console.WriteLine($"{s.Name}: {s.RowCount:N0} rows, {s.ColumnCount} cols");

// First table preview + total table count
FirstTableResult first = reader.ReadFirstTable();
Console.WriteLine($"First: {first.TableName} ({first.TableCount} tables total)");

DatabaseStatistics s = await reader.GetStatisticsAsync(cancellationToken);
Console.WriteLine($"Version:   {s.Version}");
Console.WriteLine($"Size:      {s.DatabaseSizeBytes / 1024 / 1024} MB");
Console.WriteLine($"Tables:    {s.TableCount}  Rows: {s.TotalRows:N0}");
Console.WriteLine($"Cache hit: {s.PageCacheHitRate}%");
```

---

## Configuration

```csharp
var options = new AccessReaderOptions("secretPassword")
{
    PageCacheSize            = 512,    // pages in LRU cache (default: 256)
    ParallelPageReadsEnabled = true,   // parallel I/O (default: false)
    DiagnosticsEnabled       = false,  // verbose logging (default: false)
    ValidateOnOpen           = true,   // format check on open (default: true)
    FileAccess               = FileAccess.Read,   // default
    FileShare                = FileShare.Read,    // default: others may read, writes blocked
    // FileShare             = FileShare.ReadWrite // use when Access has the file open
    UseLockFile              = true,   // create .ldb/.laccdb lockfile (default: true)
    LinkedSourcePathAllowlist = new[] { @"C:\TrustedLinkedDatabases" },
    LinkedSourcePathValidator = (link, fullPath) => !link.IsOdbc,
};
await using var reader = await AccessReader.OpenAsync("database.mdb", options);

var writerOptions = new AccessWriterOptions("secretPassword")
{
    UseLockFile = true,              // create .ldb/.laccdb lockfile (default: true)
    RespectExistingLockFile = true,  // throw IOException if lockfile already exists (default: true)
};
await using var writer = await AccessWriter.OpenAsync("database.mdb", writerOptions);
```

---

## Error Handling

```csharp
try { var dt = await reader.ReadTableAsync("Orders"); }
catch (FileNotFoundException)   { /* file missing */ }
catch (UnauthorizedAccessException) { /* no password provided, or wrong password */ }
catch (InvalidDataException)    { /* corrupt or non-JET file */ }
catch (JetLimitationException)  { /* deleted-column gap, numeric overflow */ }
catch (NotSupportedException)   { /* write: CLR type not mappable to a Jet column, or table definition too large for one TDEF page */ }
catch (ObjectDisposedException) { /* reader already disposed */ }
```

---

## Limitations

| | |
|---|---|
| ⚠️ ACCDB AES encryption | Legacy password-only `.accdb` is supported; AES-encrypted Access 2007+ `.accdb` (CFB-wrapped) is detected, but page decryption is not yet supported |
| ⚠️ Complex fields (0x11/0x12) | Metadata and subtypes decoded via `MSysComplexColumns`; cell values still returned as raw bytes or `DBNull` (FK lookup into hidden system tables not yet implemented) |

---

## How It Works

Based on the [mdbtools format specification](https://github.com/mdbtools/mdbtools/blob/master/HACKING.md). The library parses JET pages directly:

1. **Page 0** — header: Jet3/Jet4 detection, code page, encryption flag
2. **Page 2** — `MSysObjects` catalog: table names → TDEF page numbers
3. **TDEF pages** — table definition chains: column descriptors + names
4. **Data pages** — row slot arrays → null mask + fixed/variable fields
5. **LVAL pages** — long-value chains for MEMO and OLE fields

---

## Contributing

Issues and pull requests welcome at [github.com/diegoripera/JetDatabaseReader](https://github.com/diegoripera/JetDatabaseReader).

## License

MIT — see [LICENSE](LICENSE) for details.
