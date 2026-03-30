# Changelog

All notable changes to `JetDatabaseReader` are documented here.
This project follows [Semantic Versioning](https://semver.org/).

---

## [2.2.0] — 2026-04-01

### ⚠️ Breaking Changes

| Area | Before | After |
|------|--------|-------|
| **`TableResult.Rows`** | `List<List<string>>` — string rows | `List<object[]>` — typed CLR rows |
| **`ReadTable(string, int)`** | Returned string rows in `Rows` | Now returns typed rows in `Rows` |
| **`ReadTable(string, int, bool)`** | Bool flag selecting typed vs string mode | **Removed** — use `ReadTable` (typed) or `ReadTableAsStrings` (strings) |
| **`ReadTableAsync(string, int, bool)`** | Async counterpart of removed overload | **Removed** |
| **`FirstTableResult`** | Extended `TableResult` | Now extends `StringTableResult` |

### ✨ New Type: `StringTableResult`

Dedicated result class for string-mode reads, returned by `ReadTableAsStrings`. Mirrors `TableResult` but `Rows` is `List<List<string>>`.

| Property | Type | Description |
|----------|------|-------------|
| `Headers` | `List<string>` | Column names |
| `Rows` | `List<List<string>>` | String rows |
| `Schema` | `List<TableColumn>` | Per-column schema |
| `TableName` | `string` | Source table name |
| `RowCount` | `int` | Computed row count |
| `ToDataTable()` | `DataTable` | All columns `typeof(string)` |

### ✨ New Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `ReadTableAsStrings(string, int)` | `StringTableResult` | Sampled string-mode read |
| `ReadTableAsStringsAsync(string, int)` | `Task<StringTableResult>` | Async variant |
| `TableResult.ToDataTable()` | `DataTable` | Convert typed result — column types from `Schema` |
| `StringTableResult.ToDataTable()` | `DataTable` | Convert string result — all columns `typeof(string)` |

### 🔧 Improvements & Bug Fixes

- **ACCDB encryption false positive fixed** — The Jet4 encryption flag at offset `0x62` is now only checked for `ver == 1` (Access 2000–2003 `.mdb`). Access 2007+ format databases (`ver >= 2`) set unrelated bits at that offset and were incorrectly rejected with `NotSupportedException`. All `.accdb` files and `.mdb` files saved in Access 2007–2019 format are now readable.
- **Model files split** — `ColumnSizeUnit`, `ColumnSize`, and `TableColumn` each have their own `.cs` file.

### 📦 Migration Guide

```csharp
// ── ReadTable — Rows is now typed ─────────────────────────────────────
// Before (v2.1): string rows in Rows
TableResult r = reader.ReadTable("Orders", 10);
string val = r.Rows[0][2];                 // List<List<string>>

// After (v2.2): typed rows in Rows
TableResult r = reader.ReadTable("Orders", 10);
object val = r.Rows[0][2];                 // List<object[]>

// ── ReadTableAsStrings — dedicated string API ─────────────────────────
StringTableResult sr = reader.ReadTableAsStrings("Orders", 10);
string val = sr.Rows[0][2];                // List<List<string>>

// ── bool overload removed ─────────────────────────────────────────────
// Before ❌
TableResult r = reader.ReadTable("Orders", 10, typedValues: true);
// After ✅
TableResult        r  = reader.ReadTable("Orders", 10);
StringTableResult  sr = reader.ReadTableAsStrings("Orders", 10);

// ── ToDataTable ───────────────────────────────────────────────────────
DataTable typed   = reader.ReadTable("Orders", 100).ToDataTable();
DataTable strings = reader.ReadTableAsStrings("Orders", 100).ToDataTable();
```

---

## [2.1.0] — 2026-03-30

### ⚠️ Breaking Changes

| Area | Before | After |
|------|--------|-------|
| **`TableColumn.TypeName`** | `string` — e.g. `"Long Integer"` | **Removed** — replaced by `Type` (`System.Type`) |
| **`TableColumn.SizeDesc`** | `string` — e.g. `"4 bytes"` | **Removed** — replaced by `Size` (`ColumnSize` struct) |
| **`ReadFirstTable()`** | Returned `TableResult` | Now returns `FirstTableResult` (subclass of `TableResult`) |
| **`GetTableStats()`** | Returned `List<(string Name, long RowCount, int ColumnCount)>` | Now returns `List<TableStat>` |

### ✨ New Types

| Type | Description |
|------|-------------|
| `ColumnSizeUnit` | Enum: `Bits`, `Bytes`, `Chars`, `Variable`, `Lval` |
| `ColumnSize` | Readonly struct — `Value` (`int?`) + `Unit` (`ColumnSizeUnit`). Factory methods: `FromBits`, `FromBytes`, `FromChars`. Sentinels: `Variable`, `Lval`. `ToString()` produces a human-readable description. |
| `FirstTableResult` | Extends `TableResult` with `TableCount` (`int`) — the total number of user tables in the database. Returned by `ReadFirstTable()`. |
| `TableStat` | Named class with `Name` (`string`), `RowCount` (`long`), `ColumnCount` (`int`). Returned as element of `List<TableStat>` from `GetTableStats()`. |

### 🔧 Improvements

- **`TableResult`** gains `TableName` (`string`) — the table this result was read from — and `RowCount` (`int`) computed property.
- **`TableColumn.Type`** (`System.Type`) — exact CLR type, consistent with `ColumnMetadata.ClrType`.
- **`TableColumn.Size`** (`ColumnSize`) — structured size with programmatic access to numeric value and unit; `ToString()` preserves the previous human-readable output.

### 📦 Migration Guide

```csharp
// ── TableColumn schema properties ────────────────────────────────────
// Before
string typeName = col.TypeName;   // "Long Integer"
string sizeDesc = col.SizeDesc;   // "4 bytes"
// After
Type   clrType  = col.Type;                    // typeof(int)
int?   bytes    = col.Size.Value;              // 4
string display  = col.Size.ToString();         // "4 bytes"
bool   isVar    = col.Size.Unit == ColumnSizeUnit.Variable;

// ── ReadFirstTable ────────────────────────────────────────────────────
// Before
TableResult      r = reader.ReadFirstTable();
// After
FirstTableResult r = reader.ReadFirstTable();
int total = r.TableCount;   // new property on FirstTableResult

// ── GetTableStats ─────────────────────────────────────────────────────
// Before — tuple list
foreach (var (name, rows, cols) in reader.GetTableStats()) { ... }
// After — named class
foreach (TableStat s in reader.GetTableStats())
    Console.WriteLine($"{s.Name}: {s.RowCount} rows, {s.ColumnCount} cols");
```

---

## [2.0.1] — 2026-03-29

### ⚠️ Breaking Changes

| Before | After | Notes |
|--------|-------|-------|
| `TablePreviewResult` | `TableResult` | Renamed for clarity — remove the `Preview` prefix |
| `TablePreviewColumn` | `TableColumn` | Renamed for clarity — remove the `Preview` prefix |

### 📦 Migration Guide

```csharp
// Before
TablePreviewResult p = r.ReadTable("Orders", 10);
foreach (TablePreviewColumn col in p.Schema)
    Console.WriteLine($"{col.Name}: {col.TypeName} ({col.SizeDesc})");

// After
TableResult p = r.ReadTable("Orders", 10);
foreach (TableColumn col in p.Schema)
    Console.WriteLine($"{col.Name}: {col.TypeName} ({col.SizeDesc})");
```

---

## [2.0.0] — 2026-03-28

### ⚠️ Breaking Changes

| Area | v1 behaviour | v2 behaviour |
|------|-------------|-------------|
| **Constructor** | `new JetDatabaseReader(path)` | `AccessReader.Open(path)` — factory method required |
| **`ReadTable()`** | Returned `(headers, rows, schema)` tuple | Now an overload — `ReadTable(string, int)` returns `TableResult` |
| **`ReadTableAsDataTable()`** | Returned `DataTable` with `string` columns | **Renamed** to `ReadTableAsStringDataTable()` |
| **`StreamRows()`** | Returned `IEnumerable<string[]>` | Now returns `IEnumerable<object[]>` with native CLR types |
| **`ReadAllTables()`** | Returned `DataTable` with `string` columns | Now returns `DataTable` with typed CLR columns |
| **`ReadAllTablesAsync()`** | Same string behaviour | Now returns typed CLR columns |

### ✨ New Methods

| Method | Description |
|--------|-------------|
| `ReadTable()` | Primary read method — typed `DataTable` (replaces `ReadTableAsDataTableTyped`) |
| `ReadTable(string, int)` | Sampled-rows overload — returns `TablePreviewResult` (headers, rows, schema) |
| `ReadTableAsync()` | Async typed `DataTable` |
| `ReadTableAsync(string, int)` | Async sampled-rows overload — returns `Task<TablePreviewResult>` |
| `StreamRowsAsStrings()` | Compatibility streaming — `IEnumerable<string[]>` |
| `ReadAllTablesAsStrings()` | Bulk read with string columns |
| `ReadAllTablesAsStringsAsync()` | Async bulk read with string columns |
| `TableQuery.Where(Func<object[], bool>)` | Typed row predicate |
| `TableQuery.WhereAsStrings(Func<string[], bool>)` | String row predicate |
| `TableQuery.Execute()` | Returns `IEnumerable<object[]>` |
| `TableQuery.ExecuteAsStrings()` | Returns `IEnumerable<string[]>` |
| `TableQuery.FirstOrDefault()` | Returns first `object[]` or null |
| `TableQuery.FirstOrDefaultAsStrings()` | Returns first `string[]` or null |
| `TableQuery.Count()` / `CountAsStrings()` | Count per chain |
| `GetColumnMetadata()` | Rich per-column metadata with CLR type |
| `GetStatistics()` / `GetStatisticsAsync()` | Database-level statistics + cache hit rate |
| `TablePreviewResult` | Result type for sampled-rows overload — `Headers`, `Rows`, `Schema` |
| `TablePreviewColumn` | Schema entry — `Name`, `TypeName` (`string`), `SizeDesc` (`string`) |

### 🔧 Improvements

- **`FileShare` default changed to `FileShare.Read`** — other processes may read but not write while the database is open; pass `FileShare.ReadWrite` explicitly when Microsoft Access has the file open
- LRU page cache (256-page default, ~1 MB for Jet4 pages)
- Parallel page reads option (`ParallelPageReadsEnabled`)
- `AccessReaderOptions` configuration object (`PageCacheSize`, `FileAccess`, `FileShare`, `ValidateOnOpen`)
- `DatabaseStatistics` and `ColumnMetadata` types
- `IAccessReader` interface — fully testable and mockable
- Full XML documentation on all public members

### 📦 Migration Guide

```csharp
// ── Open ────────────────────────────────────────────────────────────
// v1
using var r = new JetDatabaseReader("db.mdb");
// v2
using var r = AccessReader.Open("db.mdb");

// ── Read typed DataTable ─────────────────────────────────────────────
// v1 — no equivalent (all columns were strings)
// v2
DataTable dt = r.ReadTable("Orders");
int id = (int)dt.Rows[0]["OrderID"];

// ── Read string DataTable (compatibility) ────────────────────────────
// v1
DataTable dt = r.ReadTableAsDataTable("Orders");
// v2
DataTable dt = r.ReadTableAsStringDataTable("Orders");

// ── Sample with schema ──────────────────────────────────────────────
// v1
var (h, rows, schema) = r.ReadTable("Orders", maxRows: 10);
// v2
TablePreviewResult p = r.ReadTable("Orders", 10);
// p.Headers / p.Rows / p.Schema[i].Name, .TypeName, .SizeDesc

// ── Stream rows (typed) ──────────────────────────────────────────────
// v1
foreach (string[] row in r.StreamRows("Orders")) { ... }
// v2 — typed
foreach (object[] row in r.StreamRows("Orders")) { int id = (int)row[0]; }
// v2 — compat
foreach (string[] row in r.StreamRowsAsStrings("Orders")) { ... }

// ── Bulk read ────────────────────────────────────────────────────────
// v1 — returned string columns
var tables = r.ReadAllTables();
// v2 — returns typed columns
var tables = r.ReadAllTables();
// v2 — compat strings
var tables = r.ReadAllTablesAsStrings();
```

---

## [1.0.0] — 2026-03-27

- Pure-managed JET3/Jet4 reader (no OleDB/ODBC/ACE)
- All standard column types (Text, Integer, Currency, GUID, MEMO, OLE)
- Multi-page LVAL chain support
- OLE Object magic-byte detection (JPEG, PNG, PDF, ZIP, DOC, RTF)
- Compressed Unicode (Jet4) decoding
- Code page auto-detection (non-Western text)
- Encryption detection (`NotSupportedException`)
- Streaming API (`StreamRows`)
- `IProgress<int>` callbacks
- 256-page LRU cache
