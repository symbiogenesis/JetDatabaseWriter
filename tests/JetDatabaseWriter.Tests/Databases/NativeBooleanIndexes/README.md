# Native Boolean physical indexes

These controlled DAO-authored files establish ordinary Boolean physical key and bookmark bytes. They contain one user table, `BoolItems`, with `Id LONG` and `Flag BOOLEAN`. No application objects, links, saved queries, macros or VBA were authored or invoked.

On 2026-10-22, x64 `DAO.DBEngine.120` version 16.0, `ACEDAO.DLL` file version 16.0.20527.20088, created Jet4 with version option 64 and ACE with version option 128. The x86 Windows PowerShell host activated `DAO.DBEngine.36` version 3.6, `dao360.dll` file version 10.0.26100.5074, to create Jet3 with version option 32. All used locale `;LANGID=0x0409;CP=1252;COUNTRY=0`. Native processes ran hidden in an absolute owned temporary directory; the producer restored both PowerShell location and process current directory in `finally` and preserved inherited system/profile environment variables.

The producer appended native DAO fields (`dbLong = 4`, `dbBoolean = 1`), then executed only these literal controlled insert statements:

```sql
INSERT INTO BoolItems (Id,Flag) VALUES (1,False)
INSERT INTO BoolItems (Id,Flag) VALUES (2,True)
INSERT INTO BoolItems (Id,Flag) VALUES (3,Null)
INSERT INTO BoolItems (Id,Flag) VALUES (4,False)
INSERT INTO BoolItems (Id,Flag) VALUES (5,True)
INSERT INTO BoolItems (Id) VALUES (6)
```

Each engine accepted four single-column `Flag` indexes, created through DAO `CreateIndex`, `CreateField`, `Indexes.Append` and native `IgnoreNulls`/descending field attributes:

| Name | Direction | IgnoreNulls |
|---|---|---|
| AscAllRows | Ascending | False |
| AscIgnoreNulls | Ascending | True |
| DescAllRows | Descending | False |
| DescIgnoreNulls | Descending | True |

The producer closed the database and called that engine's `CompactDatabase` into a separate file. Both original and compacted files reopened read-only with all four descriptors and six rows. A fixed literal metadata/control query, `SELECT [Id],[Flag],IsNull([Flag]) AS [NullFlag] FROM [BoolItems] ORDER BY [Id]`, returned True only for Ids 2 and 5, False for Ids 1, 3, 4 and 6, and `IsNull = 0` for every Boolean. In particular, explicit native Null and omitted Flag became False; IgnoreNulls retained all six entries. The initial x86 producer completed creation and compact but lacked the `Get-FileHash` cmdlet; a separate x86 native verification reopened and checked both files and computed their SHA-256 using .NET cryptography.

Independent raw-field and composed index/bookmark inspection matched all 144 entries across the six files. The complete native key bytes, including the non-null entry flag, are identical across formats, native compact and both IgnoreNulls variants:

| Value | Ascending | Descending |
|---|---|---|
| True | `7F00` | `80FF` |
| False | `7FFF` | `8000` |

Every bookmark is the three-byte big-endian data-page number followed by the zero-based row byte. Ids 1 through 6 occupy rows 0 through 5 of the data page below. For example, Jet4 source ascending True/Id 2 has complete canonical key-plus-bookmark bytes `7F00 00001601`; False/Id 1 has `7FFF 00001600`. Native page prefix compression remains in the frozen files.

| File | Bytes | Data page | SHA-256 |
|---|---:|---:|---|
| Jet3-Boolean.mdb | 57,344 | 23 | `7062F0A54807195FF772095BA80F86E4B475D2E62408020C86F9707A057ADE54` |
| Jet3-Boolean-compacted.mdb | 57,344 | 21 | `31222827F48795DC5B8D4CDC03AD089819BC2A88C5A8B46C53A5DE2D21D6BA8A` |
| Jet4-Boolean.mdb | 110,592 | 22 | `89F8B0E8A90618CB87EB10EF4F517AF293B7F4840D4B56FED89F94C88E7FE607` |
| Jet4-Boolean-compacted.mdb | 110,592 | 20 | `1BEF6777E2BF2CC5C6BEE5F188D991E445666F8994408385BB3535DBEED475CD` |
| Ace-Boolean.accdb | 204,800 | 45 | `F7A09D655CB21D8E0AD09B4FBC2265FDD67BBAA6BBA5BA600D6B34B680C48C3E` |
| Ace-Boolean-compacted.accdb | 204,800 | 43 | `C2B0A7712FEA1EF26865BE60EA29EDA01B757A0DE8C787210087C39A3A8C36FB` |

The portable frozen tests pin these hashes, decode each present Boolean mask independently, compare every native key and exact bookmark target, and run independent source key/row correspondence and the active all-index path. Separate mutations exercise missing, wrong, duplicate and false-as-null keys, invalid/opposite-value bookmarks, schema-absent False values, admission failures, cancellation and I/O errors.

This evidence supports independent audit of ordinary Boolean masks. The [native schema-evolution controls](SchemaEvolution/README.md) additionally establish False keys for old absent ordinary Boolean slots, regardless of the appended column default. Raw absent observations remain unchanged. Calculated Boolean keys remain unsupported. Broad writer/index-creation admission, complete source ownership, incoming references and reclamation remain separate requirements.
