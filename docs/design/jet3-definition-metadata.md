# Jet3 definition metadata

The byte checks here use Microsoft DAO 3.6 through an x86 host. They distinguish
native-authored metadata, writer output compared with that metadata, and native
acceptance of changes to an existing native-owned table. They do not establish
the separate fresh Jet3 system-catalog bootstrap or usage-map requirements.

## Physical indexes and statistics

Jet3 real-index flags omit the Jet4/ACE `0x80` bit. The modeled bits are
`0x01` (unique), `0x02` (ignore Nulls), and `0x08` (required). A newly declared
primary key uses `0x09`; an ordinary index uses zero. Sort direction belongs to
each column-map entry, not these flags. Existing native flags are retained
verbatim when a surviving index is rebuilt during Add/Drop/Rename.

Each real index has an eight-byte statistics slot before the column descriptors.
The second UInt32 is not a live table-row count. An independently native-created
empty index starts with `(0, 0)`. Inserting keys `10, 10, Null, 20` produces
`(0, 3)` for an ordinary index and `(0, 2)` with IgnoreNulls. Native updates and
deletes can leave this estimate unchanged. Compact recalculates both words;
the first word can count indexed rows rather than table rows when Null keys are
omitted. These observations do not assign a complete optimizer contract to the
two words.

The writer preserves both words across row mutations and schema rewrites, and
initializes new slots to zero. DAO accepts the zero statistics with valid index
pages, performs ordered reads and mutations, and recalculates them during
Compact & Repair. Table-level live row counts remain maintained independently.
The Jet4/ACE counter policy is separate.

`Jet3IndexDefinitionTests` covers native flags, new zero slots, stale-statistic
preservation, row mutation, schema edits, rollback and native compact. Native
relationship statistics and their compacted comparison also have independent
coverage in `DaoRelationshipMetadataTests`.

## Column descriptors

New DAO 3.6 user columns of twelve supported kinds have these bytes at offsets
7–12 in a CP1252, General 97 database:

| Offset | Native value | Writer policy |
|---|---|---|
| 7–8 | `01 00` | Initialize new user columns to 1; preserve existing bytes |
| 9–10 | `09 04` | Use the column's preserved sort order, or General 97 for new columns |
| 11–12 | `E4 04` | Use the database code page for new columns; preserve existing bytes |

Older Microsoft-authored files and system columns also contain zero at 7–8.
Its general meaning remains uninterpreted. A rewrite copies the original raw
descriptor before patching only the fields whose physical positions change;
it does not normalize offsets 7–12 to the new-column defaults.

`Jet3DescriptorPreservationTests` compares new descriptors with a live native
oracle and a hash-pinned DAO-authored fixture. It verifies preserved metadata
through Add/Drop/Rename and row changes, including native reopen, mutation and
compact. The frozen fixture's producer, creation recipe and hash are recorded
beside `Databases/Jet3/NativeDescriptorMetadata.mdb`.

## Page-zero tail

The 256 two-byte slots at page-zero offsets `0x600`–`0x7FF` have high bytes of
1 in the native files examined. Their low bytes vary: the first low byte was
3 after native creation and 1 after native compact; an existing Access 97
fixture also has another nonzero low byte. Their complete meaning is unknown.
New writer headers initialize the high bytes to 1 and leave the uninterpreted
low bytes zero. Editing an existing database preserves the entire tail,
including its unknown low bytes. The bootstrap golden specimen records this
specific high-byte correction, rather than copying an arbitrary native tail.

## Currency and decimal declarations

Jet3 has Currency (`0x05`), not the Jet4 Numeric type (`0x10`). DAO 3.6 refuses
`DECIMAL(18,4)` DDL with error 3292. A managed decimal declaration can use
Currency only when every value permitted by its precision and scale fits:
at most fourteen integer digits and four fractional digits. Wider declarations
are refused before allocation or mutation. Currency retains four fractional
digits even when the managed declaration specifies a smaller scale.

`DaoJet3CurrencyDefinitionTests` compares complete Currency descriptors and raw
ascending/descending keys with independently native-authored output. It covers
safe decimal declarations and the full explicit Currency range
`-922337203685477.5808` through `922337203685477.5807`, duplicate and Null keys,
ordering, native insert/update/delete and compacted readback for both newly
created and writer-added columns. The ordinary
`Jet3DecimalColumnTests` additionally cover declaration refusal, boundary values
and all write modes.

## Catalog prerequisites for native mutation

On an existing native Jet3 database, new tables use the database's own owner and
inheritable container permissions. The owner placeholder is the unmasked
`02 04` identity; an explicit inheritable owner entry takes precedence.
Permission masks are copied unchanged and non-inheritable entries are excluded.
`NativeJet3CatalogSecurityTests` checks exact inheritance, restrictive masks,
malformed-owner/duplicate-identity refusal and preservation of caller work.
The separate minimal Jet3 bootstrap still lacks the complete native catalog.

Jet3 catalog `LvProp` values use external LVAL storage even for a small property
block. Independently setting `Field.Required = true` in DAO produces the same
43-byte `KKD` payload as the writer, but DAO stores a 12-byte single-page LVAL
descriptor (`2B 00 00 40`, row pointer, zero token). An inline descriptor
(`2B 00 00 80`) with the identical payload lets DAO read existing primary-key
rows but causes error 3049 on insert and update. Changing only the descriptor
and external storage restores those operations without relocating the catalog
row or changing the property bytes. The writer prepares an external value for
all Jet3 catalog-property insertion and update paths; allocation occurs after
row validation and participates in statement/caller rollback.

`Jet3CatalogPropertyStorageTests` pins the native payload and external descriptor
across creation, Add/Drop/Rename, table-rule edits, refusal and rollback in every
write mode. The native index matrix then verifies new primary, unique, normal,
descending, IgnoreNulls and Required indexes through native seek, mutation,
constraint errors and compact. These checks establish definition and property
storage compatibility on native hosts, not fresh Jet3 database bootstrap.
