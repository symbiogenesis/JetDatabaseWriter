# Native SQL-specific query specimens

These controlled databases were authored on 2026-10-30 by Microsoft DAO3.6 (`DAO.DBEngine.36`, engine version `3.6`) through x86 Windows PowerShell. The Microsoft DAO3.6 Object Library `dao360.dll` has file/product version `10.0.26100.5074`. The producer is [`scripts/new-native-sql-specific-query-fixtures.ps1`](../../../../scripts/new-native-sql-specific-query-fixtures.ps1). Version options 32 and 64 produce Jet3 and Jet4 respectively; both use `LANGID=0x0409`, code page 1252 and country 0.

| Specimen | Bytes | SHA-256 |
|---|---:|---|
| NativeSqlSpecificQueriesV1997.mdb | 69,632 | `46013C4F886BB678CFA6C223F9C64354743863454D931619CE887758BF6983E2` |
| NativeSqlSpecificQueriesV2000.mdb | 122,880 | `2E560387DCA67386442272FC275D1EC5F75FECE495FAB400A9C0770D9AF21158` |

The only user table is `T(Id LONG, Note LONG, Keep LONG)`, with one row `(1,2,9)`. Native `CreateQueryDef` authored these stored definitions, then the database was closed and reopened read-only to verify the resulting types:

| Query | Native DAO Type | SQL |
|---|---:|---|
| CreateDefinition | 96 | `CREATE TABLE [NeverCreated] ([Id] LONG, [Note] LONG)` |
| PassThroughSelect | 112 | `SELECT [Id], [Note] FROM [T] WHERE [Id] = 1` |
| PassThroughAction | 144 | `DELETE FROM [T] WHERE [Id] = 1` |

Both pass-through definitions store `Connect=ODBC;DSN=JDW_Inert_NotInstalled;DATABASE=JDW_MetadataOnly;` and `ODBCTimeout=17`. The select has `ReturnsRecords=True` and `MaxRecords=23`; the action has `ReturnsRecords=False`. The DSN is an intentionally unconfigured inert name, with no driver, credentials, server, URL, or filesystem path. No pass-through connection was opened. Reading `SQL`, `Type`, connection/limit flags and properties does not invoke the saved query.

The DDL definition has a native Memo `Description` containing `Inert stored DDL; ` repeated 160 times. Each pass-through definition has `Inert pass-through metadata; ` repeated 120 times. These descriptions require native long-value/property storage rather than only short SQL text. Only explicit table creation and the one seed insert were executed; no saved definition was invoked. No VBA, macros, forms, reports, linked tables or external database files were authored. Byte scans under ASCII, UTF-16LE and UTF-16BE found no producing user name, machine name, profile path, producer directory, URL or AutoExec token.

`NativeSqlSpecificQueryPreservationTests` pins both hashes and compares every query's complete catalog values, all `MSysQueries` row values and each query's permission rows through column rename, case-only rename, column drop and referenced-table deletion in all three public write modes. It verifies that stored DDL has not created NeverCreated and stored action SQL has not changed the surviving row. An independent negative control changes one stored DSN character in each native specimen and requires the metadata snapshot to detect the change.

Targeted DAO3.6 controls compare the same metadata after independent native edits, native compact and reopen, equivalent writer edits, and writer-output compact and reopen. They verify exact SQL, native types, Connect, ReturnsRecords, ODBCTimeout, MaxRecords and long Description properties without executing the definitions. This establishes preservation of these named Jet3/Jet4 SQL-specific forms; it does not establish pass-through execution, network access, ACE variants, Access UI Name AutoCorrect behavior, arbitrary application semantics, complete shared-ACL storage preservation or raw external LVAL framing.
