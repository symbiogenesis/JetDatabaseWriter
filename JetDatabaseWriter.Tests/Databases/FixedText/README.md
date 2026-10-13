# Native fixed Text fixtures

These unencrypted, password-free files were created on 2026-10-12 by Microsoft
DAO, independently of JetDatabaseWriter. They contain a table named `FixedText`
with `Value` (Text, DAO size 6, `Attributes = 1`, `AllowZeroLength = true`), `Id`
(Long) and `Spare` (Long, value 7). No user indexes or relationships are present.

| File | Producer | CreateDatabase options | Stored fixed Text width | SHA-256 |
|---|---|---|---|---|
| NativeFixedTextJet3Cp1252.mdb | x86 DAO.DBEngine.36, engine 3.6 | 32; LANGID=0x0409, CP=1252 | 6 bytes | 4C950521365C0C5AB04972A63C0209FFAB90DA0FFD0C5ADB0BE9D6869EF7A1E5 |
| NativeFixedTextJet3Cp932.mdb | x86 DAO.DBEngine.36, engine 3.6 | 32; LANGID=0x0409, CP=932 | 6 bytes | BA967685BF1CECDEF9B72B8D8E1A2A1830E07F46E77C7B35462AC0A0B4EA5576 |
| NativeFixedTextJet4.mdb | x64 DAO.DBEngine.120, engine 16.0 | 64; LANGID=0x0409, CP=1252 | 12 bytes | 0F804D39322865FAEE3EAD9FE0EB9C04FD21AE452F962AC6B45F5571D98A166C |
| NativeFixedTextAce.accdb | x64 DAO.DBEngine.120, engine 16.0 | 128; LANGID=0x0409, CP=1252 | 12 bytes | 4A08A87ACE1A4F7793DA9905B08FD08773029FA1B1A639BD03D57F5415B5EA65 |

The DAO36 module was `dao360.dll`, file version 10.0.26100.5074. The DAO120
module was `ACEDAO.DLL`, file version 16.0.20430.20146. The CP932 database uses
General 97 collation; it does not establish Japanese collation support.

To reproduce, use the corresponding bitness-matched PowerShell COM host and
DAO engine, call `CreateDatabase(path, ';LANGID=0x0409;CP=<codepage>;COUNTRY=0',
options)`, create the fields described above and append the table. Insert each
row through `OpenRecordset('FixedText')`, `AddNew`, field `Value` assignments,
then `Update`; close the recordset and database. The original DAO-created
files are frozen here, rather than the compacted copies.

| Id | CP1252 Jet3 | CP932 Jet3 | Jet4 / ACE |
|---|---|---|---|
| 1 | Six spaces | Six spaces | Six spaces |
| 2 | `A` + five spaces | `A` + five spaces | `A` + five spaces |
| 3 | `ABCDEF` | `ABCDEF` | `ABCDEF` |
| 4 | `A`, U+0000, `B` + three spaces | Same | Same |
| 5 | Null | Null | Null |
| 6 | Absent | `漢字` + two spaces | `漢字` + four spaces |
| 7 | Absent | Absent | U+FEFF, `A` + four spaces |

Rows 1 and 2 were assigned empty text and `A`; DAO supplied the padding. Row 5
omitted the `Value` assignment. `Id` was assigned explicitly for each row.

DAO36 has an ANSI COM interface on this host, whose ACP is Windows-1252.
Passing a Japanese .NET string directly substitutes question marks before it
reaches the engine. For CP932 row 6, the four source bytes `8A BF 8E 9A` were
passed as their Windows-1252 character view: U+0160, U+00BF, U+017D, U+0161.
DAO stores those bytes and pads the six-byte fixed slot with `20 20`.
The CP932 decoder reads the resulting slot as `漢字` plus two spaces. DAO's
getter exposes the same byte-oriented Windows-1252 character view on this
host; this is native byte/storage evidence, not native Unicode semantic
equality through that COM interface.

DAO36 CompactDatabase without an explicit destination locale changes these
CP932 files' header code page to the host's CP1252. Passing
`;LANGID=0x0409;CP=932;COUNTRY=0` explicitly preserves CP932. Native read and
compact succeeded for unrelated row updates and new fixed Text rows in both
Jet3 fixtures, and the library read the exact expected text from the compacted
outputs with the destination locale preserved. DAO120 read and compact
succeeded for AddColumn, DropColumn, RenameColumn, unrelated updates and new
fixed Text rows in both Unicode fixtures, including U+FEFF and embedded NUL.

The schema preservation tests compare surviving descriptor bytes and fixed
slots across all four files. Their Jet3 schema rewrite cases establish library
preservation only. Native DAO36 rewrite acceptance remains unresolved:
Add/Drop/RenameColumn also fails for an independently DAO-created table with
only two Long fields (`Id`, `Spare`), exposing zero fields and refusing reads
with "no read permission" after a writer rewrite. The fixed Text decoder fix
does not resolve that generic schema/permissions issue. Malformed unassigned
raw Text preservation also remains outside this fixture evidence.
