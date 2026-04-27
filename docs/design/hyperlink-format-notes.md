# Hyperlink column format notes

## Source of truth
- Jackcess `ColumnImpl` (Apache 2.0): `HYPERLINK_FLAG_MASK = (byte)0x80` is OR'd into the TDEF column-flag byte at `OFFSET_COLUMN_FLAGS` (15 for Jet4 / ACE, 13 for Jet3) for any MEMO column tagged as Hyperlink. Read-time: `boolean hyperlink = ((flags & HYPERLINK_FLAG_MASK) != 0)`. Write-time: `if (col.isHyperlink()) flags |= HYPERLINK_FLAG_MASK;` in `getColumnBitFlags`.
- Microsoft documentation ("Hyperlink data type" / DAO `dbHyperlink`): the value of a hyperlink field is a single MEMO string of up to four `#`-delimited parts — `displaytext # address # subaddress # screentip` — with literal `#` characters in any part escaped as `%23`.

## On-disk layout

| Aspect | Value |
|--------|-------|
| Column type code | `T_MEMO` (`0x0C`) |
| Storage | inline (≤ 64 bytes) or LVAL chain — identical to a plain MEMO |
| Flag bit | `0x80` on the column-descriptor flag byte (`_colFlagsOff`) |
| Coexisting flag bits | `0x01 FLAG_FIXED` cleared (MEMO is variable), `0x02 FLAG_NULL_ALLOWED` per `IsNullable`, `0x04 FLAG_AUTO_LONG` not applicable, `0x40 AUTO_NUMBER_GUID` not applicable |
| Compressed-unicode bit | independent (`extra_flags` byte at `_colFlagsOff + 1`); honoured for the hyperlink payload exactly as for any MEMO |
| Persisted column properties | none specific — Microsoft Access stores the Hyperlink data-format affordance via `MSysObjects.LvProp` `Format = "Hyperlink"` on some files, but the flag bit is sufficient on its own and is what `Database → Table → Design View` reads |

## Value layer

```text
displaytext '#' address '#' subaddress '#' screentip
```

- All four parts are optional; trailing empty parts are emitted only when an inner part has content (`"text#http://example.com"` is preferred over `"text#http://example.com##"`).
- A bare value with no `#` is treated as `address` (display empty). Matches DAO `Hyperlink.Address` parser.
- Embedded `#` characters in any part are escaped as `%23` on write and decoded on read.
- A value with five or more `#`-delimited segments (rare but legal: `screentip` containing an unescaped `#`) joins parts 4..N back into the screentip slot so no information is lost on round-trip.

## Library mapping

| Surface | Behaviour |
|---------|-----------|
| `ColumnDefinition.IsHyperlink = true` | Forces `T_MEMO` and OR's `0x80` into the emitted column-flag byte. Throws `ArgumentException` if the column resolves to a non-MEMO type (e.g. `string` with `MaxLength ≤ 255`). |
| `ColumnDefinition(name, typeof(Hyperlink))` | Equivalent shorthand: also produces `T_MEMO` with the hyperlink bit set. |
| `ColumnMetadata.IsHyperlink` | Reflects the bit observed on disk. |
| `ColumnMetadata.ClrType` | `typeof(Hyperlink)` for hyperlink columns; `typeof(string)` for plain MEMO. |
| `ColumnMetadata.TypeName` | `"Hyperlink"` for hyperlink columns; `"Memo"` for plain MEMO. |
| `ReadDataTableAsync` | `DataColumn.DataType = typeof(Hyperlink)` for flagged columns; cell values are `Hyperlink` instances. |
| `Rows`, `Rows<T>`, `ReadTableAsync<T>` | Yield `Hyperlink` for flagged columns. POCO mapping accepts either a `Hyperlink` property or a `string` property — `RowMapper` handles the bidirectional conversion. |
| `RowsAsStrings`, `ReadTableAsStringsAsync` | Unchanged: yield the raw `#`-delimited form so consumers that explicitly opted into the string surface do not see a behaviour change. |
| Writer encoding | `Convert.ToString(value, InvariantCulture)` invokes `Hyperlink.ToString()` to produce the canonical encoded form before MEMO compression / LVAL emission. Plain `string` values pass through verbatim. |
| Schema-evolution (`AddColumnAsync` / `RenameColumnAsync` / `DropColumnAsync`) | The flag bit travels with `ColumnInfo.Flags`, so it survives table-rewrite operations without further plumbing. |

## What is intentionally not modelled

- Microsoft Access's "Append Only" hyperlink history (driven by complex-column version-history flat tables, not the hyperlink bit). Tracked separately under `complex-columns-format-notes.md`.
- Code-page-specific tail bytes that some Access UI versions append to the screentip part — they are preserved verbatim by the lossless `string.Join("#", parts, 3, ...)` fallback in the parser.
- The Office data-format property string `"Hyperlink"` on `MSysObjects.LvProp`. The flag bit is sufficient for round-trip and for Microsoft Access to render the column as a hyperlink; persisting the duplicate property string adds no observable behaviour and is omitted to keep the writer terse.
