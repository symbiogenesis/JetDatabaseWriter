# Native appended Boolean controls

These files extend isolated copies of the [ordinary Boolean specimens](../README.md), preserving those original controls. On 2026-10-22 the same native engine/host versions appended an ordinary Boolean `LateFlag` to `BoolItems`, with its DAO `DefaultValue` set to the literal `False` or `True` before `Fields.Append`. Jet3 used x86 DAO3.6; Jet4 and ACE used x64 DAO120. The original six rows retained physical `NumCols = 2`; the new field has `ColNum = 2`. Thus its old slots are absent according to the native row header, with no stored field bytes. Native read-only `SELECT [Id],[LateFlag],IsNull([LateFlag]) AS [NullFlag] FROM [BoolItems] ORDER BY [Id]` returned False and `IsNull = 0` for every old row in both default controls.

The producer then inserted four literal controls into the indexed copy:

```sql
INSERT INTO BoolItems (Id) VALUES (7)
INSERT INTO BoolItems (Id,LateFlag) VALUES (8,False)
INSERT INTO BoolItems (Id,LateFlag) VALUES (9,True)
INSERT INTO BoolItems (Id,LateFlag) VALUES (10,Null)
```

These new rows have physical `NumCols = 3` and present Boolean masks. Id 7 honors the new-row default, becoming True only in `DefaultTrue` files. Ids 8, 9 and 10 read False, True and False respectively in both controls. The default never changes the old six values.

All three native engines accepted `LateAscAllRows`, `LateAscIgnoreNulls`, `LateDescAllRows` and `LateDescIgnoreNulls`, each indexing only `LateFlag`. Every index contains all ten rows, including all old absent slots as ordinary non-null False keys: ascending `7FFF`, descending `8000`. True keys are ascending `7F00`, descending `80FF`. Exact keys and native bookmark targets agree with independent row Ids in every index. The four original `Flag` indexes also remain present.

The producer closed each indexed file and called the matching engine's `CompactDatabase` into a separate file. Native read-only reopen confirmed identical logical values, defaults and all eight index descriptors. Native compact materialized every old row to `NumCols = 3` with a present False mask. Independent raw inspection and composed bookmark traversal checked all 480 `LateFlag` entries across the twelve indexed/compacted files, without modifying any source. Before-index snapshots also showed the old six slots absent and native False; those redundant snapshots and the exact native producer/verification logs are retained with the session evidence rather than duplicated here.

All native processes ran hidden with an absolute owned temporary working directory, restored both PowerShell location and process current directory in `finally`, and preserved inherited system/profile environment variables. Only controlled table schema, literal data statements and native index/compact APIs were used. No saved query, macro or VBA was authored or invoked.

| File | Bytes | SHA-256 |
|---|---:|---|
| Ace-LateDefaultFalse-compacted.accdb | 221184 | `81B31622868DBBDC2FDEFA6FCFB18BEE483FD246BBAEB833A77B552E741ABF19` |
| Ace-LateDefaultFalse-indexed.accdb | 221184 | `211CA67DBBC5A4FE839FB9D73C7D4DF251C96811CD0CF1518EF000FB6607EF0A` |
| Ace-LateDefaultTrue-compacted.accdb | 221184 | `D80C82547ADF73F10810FCBA7BB623583349A313B61AD3E657143F2787710EA8` |
| Ace-LateDefaultTrue-indexed.accdb | 221184 | `81A6504CFADB7A939E8B092D87DE634A5364F6A92AE72E80C59A417C162702AB` |
| Jet3-LateDefaultFalse-compacted.mdb | 65536 | `A9E7B14D467E171A3D423F5374135F750935B333238A175E99F0249F21D0E325` |
| Jet3-LateDefaultFalse-indexed.mdb | 65536 | `C41A9AD264DA318150B230C12441E8DCF7C2A87198FFB157EC5BA838BB51A30E` |
| Jet3-LateDefaultTrue-compacted.mdb | 65536 | `9E30CA34071C423AC8CE06B0606969683653ABE2A028E4E3292E106F0A842D05` |
| Jet3-LateDefaultTrue-indexed.mdb | 65536 | `EF4739E32B6CD11E62855399F012183DFE8535E2E521463C2B40DBB63D43ED36` |
| Jet4-LateDefaultFalse-compacted.mdb | 126976 | `9A93969007196ADA77B6867A2690D0EB89AEB4887A1C0A78E290F147F0C1FD03` |
| Jet4-LateDefaultFalse-indexed.mdb | 126976 | `EB2D2EE3F76B461E1321C3D57410151B32508C7446D953578A6A17D8AD5A124B` |
| Jet4-LateDefaultTrue-compacted.mdb | 126976 | `8BF6C3C2DFCC0A08FD4075E24A53FC2CB791C8AF22D755B290D1CF8227A9D259` |
| Jet4-LateDefaultTrue-indexed.mdb | 126976 | `127B3782D67D0FD8FBB2C7F15D980692220D52CF2F055ED0C86342806825E85E` |

Portable tests pin every hash, retain raw `Kind = Absent` for old indexed-source slots, compare exact native key/row correspondence, and exercise the active covering and all-eight-index path. Separate public-read checks compare `Rows`, `RowsAsStrings`, nullable Boolean POCOs, typed DataTables and string DataTables with the native results in strict and lenient modes, including old False values under a True default. Public reader behavior already matches these controls; the audit adds the same native absent-False rule without changing raw observations or evaluating defaults.

This evidence covers ordinary appended Boolean columns in the observed native schemas. Calculated columns, general writer index admission, complete source ownership, unknown incoming references and reclamation remain separate requirements. These controls do not establish arbitrary deleted-field histories or corruption recovery.
