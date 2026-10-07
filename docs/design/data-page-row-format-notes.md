# Data-page rows — format notes

How a data page (page type `0x01`) lays out its rows, and the Access
behaviours that the library got wrong until the catalog-lookup fix (deleted
slots that share a live row's offset, and overflow rows) and the
jet3-long-rows fix (the Jet3 jump table), and the usage-map rows that list a
table's and an index's pages. The page layout is the same in
Jet3, Jet4 and ACE apart from the header offsets. The reference is
[Jackcess](https://github.com/jahlborn/jackcess) `TableImpl` (`findRowStart`,
`findRowEnd`, `positionAtRowData`, `deleteRow`, `updateRow`), checked against
the Access-authored fixtures in `JetDatabaseWriter.Tests/Databases`.

## Long-value integrity

Stored MEMO/OLE payload lengths are exact. An inline payload is bounded by its column slice, not the remaining page; a single-row payload must contain its declared bytes. A chain must supply its declared length without truncation or cycling. Exact reads throw InvalidDataException for malformed payloads and propagate storage I/O failures. They never manufacture placeholder text or substitute an empty payload for unreadable data.

## Row-offset slots

After the page header, a data page holds a table of 16-bit row-offset slots,
one per row index: Jet3 keeps the row count at offset 8 and the slots from
offset 10, Jet4 and ACE at 12 and 14 (`DataPageLayout`). Rows are written from
the end of the page downwards. Each slot is:

| Bits | Meaning |
|---|---|
| `0x1FFF` | The row's start offset on the page |
| `0x8000` | The row is deleted |
| `0x4000` | The row is an overflow row's header (see below) |

`Constants.DataPage` holds these as `RowOffsetMask`, `DeletedRowFlag` and
`OverflowRowFlag`.

## Where a row ends

A row has no length field. It runs from its start offset up to the **next
greater offset of any slot on the page**, deleted slots included, or to the end
of the page for the highest row. Equal offsets must be skipped: Access leaves
deleted slots pointing at the same offset as a live row (indexTestV1997.mdb
page 18 has row 21 live at offset 756 and rows 22-26 deleted at 756, probably
left by page compaction). Taking the neighbour of whichever equal entry a binary
search lands on gave such a row zero bytes, so every read skipped it.
`DataPageRows.FindNextRowStart` is an upper-bound search over the sorted
offsets, and `TryGetSlotBound` / `UsageMap.TryGetRowBound` use a strict
"greater than" scan.

## Inside a row

A row starts with `num_cols`, then the fixed area, then the variable-length
values, and ends with a trailer (forward order):

| Field | Jet3 | Jet4/ACE |
|---|---|---|
| EOD: the offset where the variable data ends, stored right after it | 1 byte | 2 bytes |
| Variable-column offsets, last column first | 1 byte each | 2 bytes each |
| Jump table (Jet3 only, below) | `(rowLength - 1) / 256` bytes | none |
| `var_len`: the number of variable columns | 1 byte | 2 bytes |
| Null mask, one bit per column | `ceil(num_cols / 8)` bytes | same |

`RowFieldSizes` holds the per-format field sizes, and
`RowDecodePlan.TryParseRowLayout` / `ResolveColumnSlice` read the trailer for
every read path (typed, string and direct decoders, catalog scans, the
writer's snapshots and long-value walks).

The fixed area holds each fixed column at its descriptor's `offset_F`.
Access reserves every fixed column's slot, even when the last fixed columns
are Null: every Jet3 fixture does, and Jackcess notes that Access expects it.
`RowEncoder.SerializeRow` ends the fixed area after the last fixed column that
holds a value (or the slot of a Null Attachment or Complex column), so a
writer row whose last fixed columns are Null is shorter than Access's. This
library's reader, Jackcess and mdbtools accept both, because a Null fixed
column is read from the null mask; whether Access accepts the shorter form is
unchecked.

A row never spans pages, so it is at most the page size less the data-page
header and one row-offset slot: 2,036 bytes on Jet3 and 4,080 on Jet4/ACE
(`DataPageLayout.MaxRowLength`). MEMO values over 1,024 bytes and OLE
values over 256 bytes go to LVAL pages and take a 12-byte header in the row.
`TableRowStore.EncodeRow` serializes the row with a zeroed placeholder for
each such header, with no I/O. While the row is longer than a page, it
moves the largest MEMO or byte-array OLE value still inline (the first of
equal ones in column order) to a placeholder too and measures again:
moving a value shrinks the row by its payload's length, so it moves values
until their payloads cover the excess before it serializes the row again.
Calculated columns with a Memo or OLE result take part, measured by their
wrapped payload, and compressed text by its compressed bytes. An OLE value
given as a string stays inline, as UTF-8 bytes. Access moves long values
out of a full row too; which ones it picks is unchecked.
`TableRowStore.InsertRowDataLocAsync` calls `EncodeRow` before it writes
any LVAL or data page, so a row still too long with every such value moved
throws `JetLimitationException` with the file unchanged. (The LVAL pages
used to be written first, and stayed behind, unreferenced, after the
throw.) Before the check existed, the writer appended an empty data page
and then threw `ArgumentOutOfRangeException` from the copy, or
`InvalidDataException` after copying a row one byte too long over the
row-offset table; `DataPageInserter.WriteRowToPageAsync` now checks the
free space before it touches the page. Jackcess's `MAX_ROW_SIZE` (recalled
as 2,012 and 4,060) and Access's documented 2,000 / 4,000 characters per
record are lower; whether Access rejects rows between those and the page
capacity is unchecked.

An update rewrites a row by deleting it and inserting the new version, so
`RelationshipEnforcer.PlanCascadeUpdatesAsync`, which writes nothing, calls
`EncodeRow` at its end for every child row the cascades rewrite, with the
key changes of every relationship that reaches the row, and
`TableDataWriter.UpdateRowsAsync` then calls it for every new version of its
own rows. It waits for the plan because a self-relationship's cascaded key
goes into the update's own new rows, so each is measured as it is written:
a row whose old key would not fit beside the update's other assignments, but
whose shorter cascaded key does, is updated. No row is deleted before then,
so the encoder's refusals, a row too long or a value such as a Currency out
of range, leave the file unchanged.

### The Jet3 jump table

A Jet3 offset is one byte, so a row longer than 256 bytes needs the high part
of its offsets from a jump table between the offset table and `var_len`. The
rule follows mdbtools `mdb_crack_row3` and Jackcess
`TableImpl.readJumpTableVarColOffsets`:

- The row has `J = (rowLength - 1) / 256` entries, jump table included, so a
  row of exactly 256 bytes has none. Entry `k`, for the boundary
  `256 * (k + 1)`, sits `k + 1` bytes before `var_len`: the entry for 256 is
  next to `var_len`, the highest boundary's entry comes first in the row.
- Each entry is the index of the first offset at or past its boundary,
  counting the EOD as index `var_len`. Offset `i` is its low byte plus 256
  times the number of entries that index `i` has reached (Jackcess consumes
  them in order while `entry == i`).
- When the EOD lies below the last entry's boundary (`eodPosition / 256 < J`),
  that entry is a dummy and readers drop it. Access 97 writes `0xFF` there.

Only one Access-authored row in the fixtures has a jump table: MSP_PROJECTS
in test2V1997.mdb, 290 bytes with EOD 255 and a single `0xFF` dummy. No
fixture row has a real entry, so real entries are checked against the
mdbtools/Jackcess rule. `Jet3JumpTable` holds the rule; until the reader used
it, rows whose variable data crossed offset 256 decoded truncated, empty or
shifted, and a 256-byte row was misparsed, because the reader skipped
`rowLength / 256` bytes and never read them.

`RowEncoder.SerializeRow` writes the same layout. It sizes the jump table as
the smallest `J` with `(lengthWithoutJumps + J - 1) / 256 == J`
(`Jet3JumpTable.CountForLength`): a row whose other parts take 511 bytes
gets one entry (512 bytes in all), and one of 512 bytes gets two. It stores
the low byte of the EOD and of each offset. `Jet3JumpTable.Write` fills the entries and writes `0xFF` for a
dummy; rebuilt from its offsets, the MSP_PROJECTS trailer comes out as
Access's bytes. Before, the encoder wrote the one-byte fields with a checked
cast, so any Jet3 row whose EOD or an offset reached 256 threw
`OverflowException`: a table of 200 Long columns, Text values adding up to
about 250 bytes, or a `MSysObjects` row whose inline LvProp blob carried four
or five CLR defaults.

Limits of the Jet3 row:

- `num_cols` and `var_len` are one byte, so a Jet3 table holds at most 255
  columns (Access's field limit too). `CreateTableAsync` and `AddColumnAsync`
  reject a 256th column with `JetLimitationException` before writing
  anything, and `SerializeRow` rejects a row of a wider table.
- With 255 variable columns, the EOD's index is 255, the same byte as a
  dummy. One dummy is always the last entry, which readers drop; a row that
  would need two (an EOD below the second-to-last boundary, which takes a
  trailer of about 290 bytes) throws `JetLimitationException`.

Tables with no variable columns keep the writer's EOD, jump table and
`var_len = 0` trailer (the jump entries name the EOD, index 0), which every
reader skips because the TDEF has no variable columns. Access writes no
trailer for such rows: nwind.mdb's fixed-only table has 24-byte rows.

## Overflow rows

When an update makes a row too large for its page, Access moves the row's bytes
to a new slot, usually on another page owned by the same table, and leaves the
original slot behind as the row's **header**:

- The header slot is flagged `0x4000`. Its first four bytes are a pointer: the
  target row index (1 byte) followed by the target page number (3 bytes,
  little-endian). `Constants.DataPage.OverflowPointerSize` is 4.
- The moved bytes' slot is flagged `0x8000`, so a scan never reads it on its own.
- Index entries name the header slot, and the TDEF `num_rows` counts the row
  once, at the header (NorthwindTraders.accdb's `MSysObjects` declares 239 rows:
  199 live rows plus 40 headers).
- The target can itself be an overflow pointer; Jackcess keeps following while
  the target slot carries `0x4000` and ignores the target's deleted flag.

Access-authored fixtures with overflow rows: NorthwindTraders.accdb (40
`MSysObjects` rows, among them Orders, Employees, Products and Welcome), the
Jet3 nwind.mdb (Categories, Customers, Employees and Suppliers in
`MSysObjects`), overflowTestV2000/V2003 (rows 3 and 5 of Table1),
testOleV2007 and extDateTestV2019 (user rows), testIndexCodes*,
testIndexProperties* and queryTestV2007 (catalog rows).

### How the library reads them

`DataPageRows.ComputeRowDirectory` returns, in slot order, every live row plus
every header (a slot with `0x4000` and without `0x8000`) flagged
`RowBound.IsOverflowPointer`. `OwnedDataPages.TryResolveOverflowRowAsync` follows a
header to the row data, up to `Constants.DataPage.MaxOverflowHops` (8) hops, and
requires the target to be a data page of the same table and a slot the page
has. A pointer that cannot be resolved (shorter than four bytes, past the end of
the file, into another table, or a cycle) skips the row during read-only
traversal, as an undecodable row is skipped. With `DiagnosticsEnabled`, the reader traces the table-definition page, header slot and offset, and the reason it could not resolve the pointer. `StrictParsing` governs value parsing only.

Every read path uses the directory: the `TableReader` scans,
`RowDecoder` (catalog and flat-table scans), the index seek in `IndexRowReader`,
and the writer's `OwnedDataPages.ForEachLiveTableRowAsync`, which visits an
overflow row at its header with `RowLocation.PageNumber` / `RowIndex` naming the
header and `DataPageNumber` / `DataRowIndex` naming the moved bytes.
`EnumerateLiveRowBounds` stays live-only, for usage-map and LVAL pages, which
never hold overflow rows.

Owned-page discovery (`ValidateOwnedDataPagesAsync`) counts each header as one
row, so a table with overflow rows validates its usage map instead of falling
back to a whole-file scan.

### How the writer changes them

- **Delete** (`TableRowStore.MarkRowDeletedAsync`) flags the header `0xC000`, as
  Jackcess `deleteRow` does. With `DeletedRowDataMode.Clear` or
  `SecureEraseMode.DeletedRowsAndFreedPages` it also zeroes the moved bytes and
  the header's pointer and every validated intermediate pointer slot, and under secure erase frees the row's long values. Intermediate slots are cleared only after the entire chain resolves to a row of the same table; invalid pointers never authorize clearing another table's bytes.
- **Update** is a delete plus an insert, so the rewritten row is written fresh
  and the old header is flagged `0xC000`. The writer never creates overflow rows.
- Index entries and row identity use the header (`RowLocation.PageNumber` /
  `RowIndex`); anything that reads or patches the row's bytes uses
  `DataPageNumber` (`TryReadColumnValuesTypedAsync`, and the complex-reference
  reads and writes in `ComplexColumnManager` when an attachment or multi-value
  item is added or a row's complex children are deleted).
- A cascade that seeks the child table's foreign-key index lands on the header,
  and `RelationshipChildRowLocator` resolves it to the moved bytes, so cascade
  deletes and updates reach overflow rows on the seek path as well as on the
  snapshot path.
- `DropTableAsync` and schema rewrites free the long values of overflow rows
  too (`TableSchemaEditor.ReclaimTableStoragePagesAsync`).

## Usage-map rows

A TDEF names two usage-map rows, each by a 1-byte row index and a 3-byte page
number: `used_pages`, the table's data pages, and `free_pages`, those with room
for a row. Each real index's descriptor names a third (`used_pages`, its
tree's pages). The rows sit on a usage-map page, a data page whose rows are
maps. New writer-created Jet4 and ACE tables have one, with the owned-pages
and free-space rows at rows 0 and 1 and real index `n` at row `n + 2`, 69
bytes each (`CatalogArtifactWriter`, `DataPageInserter`); the writer keeps
no Jet3 table or index usage maps.

Existing Access-authored maps can use larger rows and different index row
numbers. Index maintenance follows each descriptor's page and row pointer,
updates that row within its actual bounds, and preserves every unrelated
row, including owned-pages, free-space and long-value maps. In
`NorthwindTraders.accdb`, `MSysObjects` (TDEF 2) uses map page 6: owned
row 0 is 381 bytes, free-space row 1 is 69 bytes, and its two indexes point
to rows 16 and 17. Rows 2 through 15 must not be repurposed as index maps.

A row's first byte is its type:

| Type | Layout |
|---|---|
| `0x00` INLINE | `start_page` (4 bytes), then a bitmap whose bit `k` is page `start_page + k`: 64 bytes, so 512 pages, in a 69-byte row |
| `0x01` REFERENCE | 4-byte pointers, 17 in a 69-byte row; pointer `i` names a bitmap page or is 0 |

A REFERENCE bitmap page has page type `0x05`, then `01 00 00`, then a bitmap
whose bit `k` is page `i * (pageSize - 4) * 8 + k`. Each pointer covers 32,736
pages of 4 KB, so 17 reach page 556,511, past the 524,288 pages of a 2 GB file.

`UsageMapEditor` keeps the writer's rows complete:

- `DataPageInserter.MarkPageInOwnedMapAsync` marks every data page the writer
  appends in the owned-pages and free-space rows (`MarkPageAsync`). The bit is
  set when the INLINE window holds the page. A row that lists no page yet
  moves its window to the page (`start_page` 0 for a page below 512, otherwise
  the page rounded down to a multiple of 8); a row that lists pages is promoted
  to REFERENCE, keeping them, with a bitmap page for each window it needs.
  Jackcess's `UsageMap` promotes the same way, but first moves a window whose
  pages, the new one included, still fit one. Before, a page outside the
  window was left out, so both maps of a table past 512 pages lost pages and
  the reader fell back to the whole-file owned-page pass; and when the window
  started at page 0, the first page past it moved the window without moving
  the bits already set, so they named other pages.
- An index row is written from the pages of a rebuilt tree (`WriteRowAsync`):
  INLINE when one window holds them, REFERENCE otherwise. A row that is
  REFERENCE already stays REFERENCE and keeps its bitmap pages, so rebuilding
  an index again takes no new page. Before, a tree spread over two windows
  threw `NotSupportedException`.
- `DropTableAsync` and the schema rewrites free the bitmap pages of the table's
  REFERENCE rows with its other pages
  (`TableSchemaEditor.ReclaimTableStoragePagesAsync`).

The writer never clears a bit in the free-space row, where Access lists only
the pages with room (binIdxTestV2010's free-space row lists only the last of
its table's four pages).

### What Access writes

The owned-pages rows of the Access-authored fixtures, read raw with PowerShell
(92 files, the encrypted ones skipped; 1,689 table definitions):

- 1,653 are INLINE, 33 TDEFs name no usage map, and 3 rows are REFERENCE. In
  binIdxTestV2010.accdb and testEmoticonsV2010.accdb, row 0 of usage-map page
  85, the owned-pages row of the table at TDEF page 84, has type `0x01` and
  pointer 0 naming page 87, which starts `05 01 00 00` and lists that table's
  data pages (86, 88, 89 and 90 in binIdxTestV2010.accdb); in
  testRefGlobalV2000.mdb an empty table's REFERENCE row has no pointer. Their
  free-space rows are INLINE. The writer's REFERENCE rows and bitmap pages
  follow this layout.
- Every page that an owned-pages row lists, 6,937 in all, is a data page whose
  owner field names that table. None of the 7,953 LVAL pages in those files is
  listed: Access keeps a table's LVAL pages out of its owned-pages map, and so
  does the writer. Access lists them in each long-value column's own usage maps
  (calculated-columns-format-notes.md), which the writer does not keep.
- INLINE rows run to the next row, and the bitmap takes the whole row: 1,507 of
  the 1,525 Jet4 and ACE ones are 69 bytes, the rest 85 to 381; the Jet3 ones
  are 133 bytes or more (a 128-byte bitmap, 1,024 pages).

Whether Access reads the writer's REFERENCE rows is unchecked (under
"Unchecked against Access" in docs/todo.md).

Writer snapshots require complete row traversal and use a write-back decode
plan. The declared directory must fit its page, and every live slot must start
past the directory, inside the page, without sharing another live slot's
offset. Deleted slots may alias offsets, and deleted overflow targets remain
valid. An unresolved overflow pointer, a live row too short for its
column-count field, or a layout that cannot be decoded raises a contextual
"MalformedValue" corruption error before row updates or schema rewrites. The
snapshot cannot omit that row and reinsert only the readable rows. Public read
APIs retain their own decode-fault policy.
