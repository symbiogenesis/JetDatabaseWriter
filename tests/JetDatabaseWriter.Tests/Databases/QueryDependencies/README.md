# Native saved-query dependency specimens

These controlled specimens were authored on 2026-10-29 by the Microsoft DAO3.6 engine (`DAO.DBEngine.36`, engine version `3.6`) through x86 Windows PowerShell. The producer is [`scripts/new-native-query-dependency-fixtures.ps1`](../../../../scripts/new-native-query-dependency-fixtures.ps1). Both databases use `LANGID=0x0409`, code page 1252 and country 0. Version option 32 produces Jet3; option 64 produces Jet4.

The installed `dao360.dll` is the Microsoft DAO3.6 Object Library with file/product version `10.0.26100.5074`, distributed as part of Microsoft Windows Operating System under `Program Files (x86)/Common Files/Microsoft Shared/DAO`. These are native DAO/Jet engine controls; no Access desktop application installation or UI behavior is established.

| Specimen | Bytes | SHA-256 |
|---|---:|---|
| NativeQueryDependenciesV1997.mdb | 57,344 | `562B9826FF109EE92D062D6E38AA0134A9E8DA49432AC29D0FFE810252623AF2` |
| NativeQueryDependenciesV2000.mdb | 98,304 | `08EF602CDB30A80C01E73068E646FB8EB39B641FEEDA82A375E6ABFADDD071BC` |

The complete user data consists of `T(Id LONG, Note LONG, Keep LONG)` with one row `(1,2,9)`, and `Lookup` with seven Long fields (`Direct`, `Sql`, `Saved`, `Chain`, `Parameters`, `Union`, `CrossTab`) and one row containing seven ones. Each lookup field has native `RowSourceType=Table/Query`; its `RowSource` is respectively `T`, an aliased SELECT from T, `BaseLookup`, `SavedLookup`, `ParameterLookup`, `UnionLookup` or `CrossTabLookup`.

| Saved query | Native DAO Type | Definition |
|---|---:|---|
| BaseLookup | 0 | Aliased SELECT of T.Id and T.Note |
| SavedLookup | 0 | Aliased SELECT of BaseLookup.Id and BaseLookup.Note |
| ParameterLookup | 0 | Long parameter Minimum; SELECT of T.Id and T.Note where Note exceeds Minimum |
| UnionLookup | 128 | UNION ALL of two SELECTs of T.Id and T.Note |
| CrossTabLookup | 16 | Count of T.Id grouped by Keep and pivoted by Note |
| UpdateDefinition | 48 | UPDATE increments Note for Id 1 |
| AppendDefinition | 64 | INSERT SELECT copies T with Id increased by 100 |
| DeleteDefinition | 32 | DELETE where Note is negative |
| MakeTableDefinition | 80 | SELECT INTO NeverCreated from T.Id and T.Note |

Only controlled table creation and seed inserts were executed. Every saved query was authored through `CreateQueryDef` and closed without invocation. No VBA, macros, forms, reports, links, pass-through connections or external references were authored. Byte scans under ASCII and both UTF-16 alignments found no producing user name, machine name, profile path, URL or AutoExec token.

`NativeLookupPropertyTests` pins each frozen hash and preserves all saved-query catalog values, complete `MSysQueries` row values, query-specific `MSysACEs` permission values and raw lookup properties through column rename/drop and target-table deletion in every write mode. It checks that the action queries have not changed the seed data or created NeverCreated. These frozen controls require no native engine on CI.

Fresh DAO3.6 controls independently establish native preservation through column rename, case-only rename, column drop and referenced-table deletion on both generations. Native Type and SQL metadata match before and after native edits and CompactDatabase, and again after the equivalent writer edits and compact. The same controls for ACE remain guarded by DAO120 availability. Saved queries are never executed during validation. This evidence covers these query forms and metadata; it does not establish Access UI Name AutoCorrect behavior, arbitrary application semantics, complete shared-ACL storage preservation or raw external LVAL framing.
