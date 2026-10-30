# Native date serial identity

Microsoft 365 Access Runtime `DAO.DBEngine.120`, `ACEDAO.DLL`
16.0.20527.20112, authored these independent source and compacted specimens on
2026-10-30. The producing host was Microsoft PowerShell 7.6.9 x64 on Windows
Arm64. `scripts/new-native-date-serial-fixtures.ps1` retains the native authoring
and compact operations.

| File | SHA-256 |
| --- | --- |
| `native-date-serials.accdb` | `51A349923E719E5320F32577994BFDD493E7A191F533546EE6B7C7FBD47340B4` |
| `native-date-serials-compacted.accdb` | `79A1A5878E325695FD4B8AF42C9275DA7AD7FDD28EFCEC84F7A60724E81FA07C` |

`T.At` contains native serials -1/24, +1/24, zero and the result of
`#1:00 AM#+0.0000000001`. `T.CalculatedAt` uses `TimeSerial(-1,0,0)` and stores
-1/24. `AtIndex` distinguishes the negative, zero and positive serials and the
additional submillisecond fraction. Native `CDbl` reads retain these distinct
identities, although .NET DateTime projects the negative and positive fractions
to the same displayed time, and rounds away the extra submillisecond fraction.

The source and native compacted files are preserved without managed edits.
Focused tests independently verify unrelated updates, schema rewrites, raw
serials, public DateTime projection, index order and native read/update/compact.
