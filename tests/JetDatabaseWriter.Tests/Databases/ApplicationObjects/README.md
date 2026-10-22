# Controlled native application objects

`NativeInertApplicationObjects.accdb` was authored on 2026-10-22 by x64 Microsoft Access 16.0.20430.20182 (`Access.Application` version 16.0) in a new private database. SHA-256: `C809AA9371DF43F0901F6AA27C5ADDEEAAA7BEDA07C37578F2863F3CCF77364F` (380,928 bytes).

The complete controlled content is:

| Object | Native Id | Type | Definition |
|---|---:|---:|---|
| Items | 64 | 1 | `Id LONG PRIMARY KEY, Label TEXT(40)`; one row `(1, 'retained')` |
| EvidenceQuery | -2147483638 | 5 | `SELECT [Id], [Label] FROM [Items] WHERE [Id] > 0 ORDER BY [Id];` |
| EvidenceForm | -2147483637 | -32768 | Blank form, Caption `Pinned evidence form` |
| EvidenceReport | -2147483636 | -32764 | Blank report, Caption `Pinned evidence report` |

Other objects are Access-created system/storage/navigation tables and containers. No links, macros, modules, VBA, event handlers, or external references were authored. The saved query was never executed. The database and native definition exports were checked for the producing user name, machine name, user-profile paths, URLs, AutoExec and VBA project markers; none occurred. Form/report exports have no code section or event-handler properties.

The native producer ran hidden, first verified that the newly activated `Access.Application` had no current database, then called `NewCurrentDatabase` with an absolute private path. DAO created Items and its row and saved EvidenceQuery through `CreateQueryDef`; only the query's SQL property was read. Access `CreateForm`/`CreateReport` authored the two blank objects. Each object was saved under its default name, closed and renamed with `DoCmd.Rename`. No data or object code was executed. All COM processes used an absolute temporary working directory with both PowerShell location and process current directory restored afterwards; inherited system/profile environment variables were preserved. Fixture-authoring paths are parameters of the private producer workspace, not database links or embedded paths.

Native `SaveAsText` definition bytes were identical for the original, the complete private assembly and that assembly after DAO120 CompactDatabase and reopen:

| Export | Bytes | SHA-256 |
|---|---:|---|
| EvidenceForm | 3,082 | `DDFB30D98C9D5C8EF0BD7108C4435CE682373BCC5A85473308B5DBDF7995A1BE` |
| EvidenceReport | 58,822 | `3FC20861F4E721A4C694C30D6E86F0892B62281BAB1226C444944AEF1047AD4C` |

The focused ordinary test checks the pinned specimen hash and identities, complete original-prefix/TDEF comparison, full active catalog values and application-storage row bytes. The guarded DAO120 test checks the fixed query's SQL metadata and application-object names/types before and after native compact, without executing the saved query. These healthy inert objects establish bounded preservation evidence; they do not establish macro/module execution, arbitrary application semantics, security/replication support, damaged-source discovery or permission to reclaim opaque storage.
