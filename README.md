# JetDatabaseWriter

[![NuGet](https://img.shields.io/nuget/v/JetDatabaseReader.svg)](https://www.nuget.org/packages/JetDatabaseReader/)
[![Downloads](https://img.shields.io/nuget/dt/JetDatabaseReader.svg)](https://www.nuget.org/packages/JetDatabaseReader/)
[![.NET Standard](https://img.shields.io/badge/.NET%20Standard-2.0-blue)](https://docs.microsoft.com/en-us/dotnet/standard/net-standard)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Pure-managed .NET library for reading Microsoft Access JET databases — no OleDB, ODBC, or ACE/Jet driver installation required.

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
| ✅ **Async support** | Full `Task<T>`-based async for all major operations |
| ✅ **Page cache** | 256-page LRU cache (~1 MB, configurable) |
| ✅ **Generic POCO mapping** | `ReadTable<T>()`, `StreamRows<T>()`, `InsertRow<T>()` — no manual casting |
| ✅ **Fluent query** | `Query().Where().Take().Execute()` — typed and string chains |
| ✅ **Progress reporting** | `IProgress<int>` callbacks on all long operations |
| ✅ **Non-Western text** | Code page auto-detected from the database header |
| ✅ **OLE Objects** | Detects embedded JPEG, PNG, PDF, ZIP, DOC, RTF |
| ✅ **Write support** | Create/drop tables, insert/update/delete rows (Jet4/ACE) |

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

using var reader = AccessReader.Open("database.mdb");

List<string> tables = reader.ListTables();
Console.WriteLine($"Found {tables.Count} tables: {string.Join(", ", tables)}");

DataTable dt = reader.ReadTable("Orders");
foreach (DataRow row in dt.Rows)
{
    int     id   = (int)row["OrderID"];
    var     date = (DateTime)row["OrderDate"];
    decimal amt  = (decimal)row["Freight"];
    Console.WriteLine($"#{id}  {date:yyyy-MM-dd}  {amt:C}");
}
```

---

## Reading Data

### Typed DataTable — recommended

```csharp
DataTable dt = reader.ReadTable("Products");
// dt.Columns["ProductID"].DataType    == typeof(int)
// dt.Columns["UnitPrice"].DataType    == typeof(decimal)
// dt.Columns["Discontinued"].DataType == typeof(bool)
```

### Generic POCO mapping — strongly typed

```csharp
public class Product
{
    public int ProductID { get; set; }
    public string ProductName { get; set; }
    public decimal UnitPrice { get; set; }
    public bool Discontinued { get; set; }
}

List<Product> products = reader.ReadTable<Product>("Products", maxRows: 100);
decimal total = products.Where(p => !p.Discontinued).Sum(p => p.UnitPrice);
```

Property names are matched to column headers **case-insensitively**. Unmatched properties keep their default value. The type `T` must be a class with a parameterless constructor.

### String DataTable — compatibility

```csharp
DataTable dt = reader.ReadTableAsStringDataTable("Products");
// every column is typeof(string)
```

### Table preview with schema — typed

```csharp
TableResult preview = reader.ReadTable("Products", maxRows: 20);
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

### Table preview with schema — strings

```csharp
StringTableResult preview = reader.ReadTableAsStrings("Products", maxRows: 20);
string firstCell = preview.Rows[0][0];  // always a string

// Convert to DataTable — all columns typeof(string)
DataTable dt = preview.ToDataTable();
```

---

## Streaming Large Tables

### Typed streaming — recommended

```csharp
var progress = new Progress<int>(n => Console.Write($"\r{n:N0} rows"));

foreach (object[] row in reader.StreamRows("BigTable", progress))
{
    int     id  = (int)row[0];
    decimal val = row[2] == DBNull.Value ? 0m : (decimal)row[2];
}
```

### Generic streaming — strongly typed

```csharp
foreach (Product p in reader.StreamRows<Product>("Products"))
    Console.WriteLine($"{p.ProductName}: {p.UnitPrice:C}");
```

### String streaming — compatibility

```csharp
foreach (string[] row in reader.StreamRowsAsStrings("BigTable"))
    Console.WriteLine(string.Join(", ", row));
```

Null values in typed rows surface as `DBNull.Value`.

---

## Fluent Query API

```csharp
// Typed chain
object[] order = reader.Query("Orders")
    .Where(row => row[2] is DateTime d && d.Year == 2024)
    .Take(10)
    .FirstOrDefault();

int count = reader.Query("OrderDetails")
    .Where(row => row[3] is decimal p && p > 100m)
    .Count();

// String chain
IEnumerable<string[]> recent = reader.Query("Orders")
    .WhereAsStrings(row => row[2].StartsWith("2024"))
    .Take(50)
    .ExecuteAsStrings();
```

---

## Async Operations

```csharp
List<string>                  tables = await reader.ListTablesAsync();
DataTable                     dt     = await reader.ReadTableAsync("Orders");
TableResult                   typed  = await reader.ReadTableAsync("Orders", 50);
List<Order>                   orders = await reader.ReadTableAsync<Order>("Orders", 50);
StringTableResult             str    = await reader.ReadTableAsStringsAsync("Orders", 50);
DatabaseStatistics            stats  = await reader.GetStatisticsAsync();
Dictionary<string, DataTable> all    = await reader.ReadAllTablesAsync();
Dictionary<string, DataTable> allStr = await reader.ReadAllTablesAsStringsAsync();
```

---

## Bulk Operations

```csharp
// Typed columns
Dictionary<string, DataTable> all = reader.ReadAllTables(
    new Progress<string>(t => Console.WriteLine($"Reading {t}...")));

// String columns (compatibility)
Dictionary<string, DataTable> allStr = reader.ReadAllTablesAsStrings();
```

---

## Writing Data

> Requires Jet4/ACE format — `.mdb` (Access 2000+) or `.accdb`.

```csharp
using var writer = AccessWriter.Open("database.mdb");
```

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

### Insert rows

```csharp
// Single row — object array
writer.InsertRow("Contacts", new object[] { 1, "Alice", "alice@example.com", 95.5m });

// Multiple rows — object arrays
writer.InsertRows("Contacts", new[]
{
    new object[] { 2, "Bob",   "bob@example.com",   88.0m },
    new object[] { 3, "Carol", "carol@example.com", 92.3m },
});
```

### Generic insert — POCO

```csharp
public class Contact
{
    public int ContactID { get; set; }
    public string Name { get; set; }
    public string Email { get; set; }
    public decimal Score { get; set; }
}

writer.InsertRow("Contacts", new Contact { ContactID = 4, Name = "Dave", Email = "dave@example.com", Score = 77.1m });

writer.InsertRows("Contacts", new[]
{
    new Contact { ContactID = 5, Name = "Eve",   Email = "eve@example.com",   Score = 91.0m },
    new Contact { ContactID = 6, Name = "Frank", Email = "frank@example.com", Score = 85.4m },
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

DatabaseStatistics s = reader.GetStatistics();
Console.WriteLine($"Version:   {s.Version}");
Console.WriteLine($"Size:      {s.DatabaseSizeBytes / 1024 / 1024} MB");
Console.WriteLine($"Tables:    {s.TableCount}  Rows: {s.TotalRows:N0}");
Console.WriteLine($"Cache hit: {s.PageCacheHitRate}%");
```

---

## Configuration

```csharp
var options = new AccessReaderOptions
{
    PageCacheSize            = 512,    // pages in LRU cache (default: 256)
    ParallelPageReadsEnabled = true,   // parallel I/O (default: false)
    DiagnosticsEnabled       = false,  // verbose logging (default: false)
    ValidateOnOpen           = true,   // format check on open (default: true)
    FileAccess               = FileAccess.Read,   // default
    FileShare                = FileShare.Read,    // default: others may read, writes blocked
    // FileShare             = FileShare.ReadWrite // use when Access has the file open
};
using var reader = AccessReader.Open("database.mdb", options);
```

---

## Error Handling

```csharp
try { var dt = await reader.ReadTableAsync("Orders"); }
catch (FileNotFoundException)   { /* file missing */ }
catch (NotSupportedException)   { /* encrypted / password-protected */ }
catch (InvalidDataException)    { /* corrupt or non-JET file */ }
catch (JetLimitationException)  { /* deleted-column gap, numeric overflow */ }
catch (ObjectDisposedException) { /* reader already disposed */ }
```

---

## Limitations

| | |
|---|---|
| ❌ Encrypted databases | Remove password in Access (File › Info › Encrypt with Password) |
| ❌ Attachment fields (0x11) | Rare type added in Access 2007 |
| ❌ Linked tables | Only local tables are listed |
| ❌ Overflow rows | Rows spanning multiple pages are skipped |

---

## Migration from v1

```csharp
// Open
var r = new JetDatabaseReader("db.mdb");              // v1 ❌
var r = AccessReader.Open("db.mdb");                   // ✅

// Typed DataTable
var dt = r.ReadTable("Orders");                        // ✅ typed columns
var dt = r.ReadTableAsStringDataTable("Orders");       // ✅ string columns

// Preview — typed rows
TableResult t = r.ReadTable("T", 10);                  // ✅ Rows is List<object[]>

// Preview — string rows
StringTableResult s = r.ReadTableAsStrings("T", 10);   // ✅
string val = s.Rows[0][2];                             // always string

// ToDataTable
DataTable dtTyped = r.ReadTable("T", 100).ToDataTable();          // CLR-typed columns
DataTable dtStr   = r.ReadTableAsStrings("T", 100).ToDataTable();  // string columns

// Schema properties
col.Type      // System.Type e.g. typeof(int)
col.Size      // ColumnSize struct (.Value, .Unit, .ToString())

// Table stats
foreach (TableStat s in reader.GetTableStats()) { }

// First table
FirstTableResult r = reader.ReadFirstTable();           // + r.TableCount

// Streaming
foreach (object[] row in r.StreamRows("T")) { }        // ✅ typed
foreach (Product p in r.StreamRows<Product>("T")) { }   // ✅ generic
foreach (string[] row in r.StreamRowsAsStrings("T")) { } // ✅ string compat

// Generic POCO mapping (new)
List<Product> p = r.ReadTable<Product>("T", 100);       // ✅ read as List<T>
List<Product> p = await r.ReadTableAsync<Product>("T", 100); // ✅ async

// Writing (new — Jet4/ACE only)
using var w = AccessWriter.Open("db.mdb");
w.InsertRow("T", new object[] { 1, "text" });           // ✅ object array
w.InsertRow("T", new Product { ... });                  // ✅ generic POCO
w.InsertRows("T", products);                            // ✅ bulk generic

// Bulk
var all = r.ReadAllTables();                           // ✅ typed columns
var all = r.ReadAllTablesAsStrings();                  // ✅ string columns
```

Full details in [CHANGELOG.md](CHANGELOG.md).

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
