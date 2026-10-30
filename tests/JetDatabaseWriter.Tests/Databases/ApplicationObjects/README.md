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

## Inert macro and module extension

`NativeInertCodeObjects.accdb` extends a private copy of the original fixture with two named objects. The original fixture remains unchanged. The additions were authored on 2026-10-22 by x64 Microsoft Access 16.0.20527.20088 (`Access.Application` version 16.0), hosted by x64 PowerShell 7. SHA-256: `EA1899DD97A8492C30B2F720201DD3A13046BEC23915C21A7DD938A12041FEE4` (393,216 bytes).

| Object | Native Id | Type | Definition |
|---|---:|---:|---|
| EvidenceMacro | -2147483634 | -32766 | One `StopMacro` action, never invoked |
| EvidenceModule | -2147483635 | -32761 | `Option Compare Database`, `Option Explicit`, and `Public Const EvidenceMarker As String = "Pinned inert module 20261022"` |

Access `LoadFromText` imported the macro and standard module into an owned temporary copy with automation security set to force-disable macros. The macro input was UTF-16LE and the module input ASCII; the module also supplied `Attribute VB_Name = "EvidenceModule"`. Native `SaveAsText` verified both definitions after import. The macro export includes Access's `PublishOption =1`. The module contains no procedures, functions or event hooks. No AutoExec, event handler or custom external reference was authored, and no embedded macro, VBA or saved query was invoked. The fixture was checked for the producing user name, machine name, user-profile path, producer directory and AutoExec; none occurred. The hidden producer checked that the newly activated application had no current database before opening its owned copy. It preserved inherited system/profile environment variables and restored both its temporary PowerShell location and process current directory in `finally`.

The complete private assembly processed all 23 local tables and 42 catalog objects, preserved the 96-page original prefix and produced a 250-page destination. DAO120 then compacted the assembled file and reopened the result. Access `SaveAsText` exported these definitions from separate source and assembled copies and the compacted result. All three exports for each object were byte-identical:

| Export | Bytes | SHA-256 |
|---|---:|---|
| EvidenceForm | 3,082 | `09AD36B341096AED3EDD6FEB5662BEF9332509C8D0968732D9DE6B744F7538DD` |
| EvidenceReport | 58,826 | `FEA02C62F5B854B9115505B7D37B14991746440B6ED925404008699868231A1A` |
| EvidenceMacro | 180 | `0DD716423EEF49B015B4AB64465A2B8E360A68EE9F61A9C6695F413E9278945E` |
| EvidenceModule | 114 | `3CCC1F71A401CE4D72C46C18CBD6B9905745F31D79C1DFDCEEB0CD6A32C16E1C` |

These exports used the same current Access build for all three files; the earlier fixture's export hashes above record its original producer build. Native serialization across different Access builds is not claimed to be identical.

The ordinary frozen test requires no COM. It pins this fixture's hash and exact macro/module Id and Type, compares every active `MSysObjects` value, and compares every row value, including long-value bytes, in `MSysAccessStorage`, `MSysQueries` and the form's opaque data table. The complete assembly also independently compares its original prefix, reconstructed TDEF references and destination storage inventory. The DAO120-guarded test checks the `Scripts` and `Modules` document names before assembly, after assembly and after native compact; it never invokes them. The `SaveAsText` comparisons are recorded native producer evidence rather than an automated test guarded only by DAO availability: Access automation and DAO have distinct installation requirements.

This extends healthy inert application-object preservation evidence to a named macro and a nonempty standard module. It does not establish code execution, arbitrary application behavior, replication or security completeness, damaged-object reconstruction, complete damaged-source discovery, incoming-reference ownership or permission to reclaim opaque pages.

## Public mutations

`NativeApplicationObjectPreservationTests` uses both pinned specimens for public row insert/update/delete, add/rename/drop of an unrelated Items column, and create/drop of another table in Direct, AutoCommit and ExplicitCommit modes. It compares every unedited catalog field value (including exact decoded timestamp ticks and Kind), complete unaffected local/system table values and binary payloads, and selected original owning TDEF/data/index pages byte-for-byte. It also compares every original MSysACEs row value, stored long-value descriptors bound to stable catalog object IDs or unchanged row bookmarks, and exact reachable external LVAL row images: native page tokens, directory slots, chain pointers, padding and terminal pointers. Both frozen fixtures contain external LVAL rows and permission rows. A temporary change to a referenced LVAL page token left decoded values unchanged but failed the physical comparison in every mutation case; the injection was removed. Physical shared ACL pages and broader LVAL/security variants remain outside this evidence. The existing fixtures and hashes remain unchanged.

Fresh DAO120 controls check inert object identities, saved-query SQL and edited rows through native reopen and compact without invoking application content. These controls remain guarded and were skipped on the current host; they add no fresh native execution evidence. The managed frozen-fixture checks do not establish arbitrary application behavior, broader native variants, replication/security completeness or permission to reclaim opaque storage.
