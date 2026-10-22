# Native row-boundary controls

These fresh controlled files were authored on 2026-10-22 through x86 DAO.DBEngine.36 version 3.6 (Jet3, CreateDatabase option 32) and x64 DAO.DBEngine.120 version 16.0 (Jet4 option 64; ACE option 128). Producer DLL versions were dao360.dll 10.0.26100.5074 and ACEDAO.DLL 16.0.20430.20182. Locale: `;LANGID=0x0409;CP=1252;COUNTRY=0`. They contain no links, saved queries, application code, macros or modules. UTF-16 and ASCII scans found no producing user/machine names, user-profile paths, URLs, AutoExec or VBA project markers.

| File | Bytes | SHA-256 |
|---|---:|---|
| NativeRowBoundariesJet3.mdb | 86,016 | `DE9DD7E0A6B13A276589D51CC5D650EDAB45050718C012C9B932F6538E6C1639` |
| NativeRowBoundariesJet4.mdb | 163,840 | `786F3C2A2ED5AACDCF46CBB16D906CA959649FA297D5A71C24801E4D0DE33857` |
| NativeRowBoundariesAce.accdb | 258,048 | `554CE2012AAFE36FEC80A3A528F57A8511374F64D9152E1124EB44EBEFA14581` |

The native producer and guarded regression independently create the same fixed-schema shape: Id LONG plus dbBinary fields split into widths 255 and a final remainder, all binary fields explicitly `Attributes=1` (dbFixedField). A value-byte total of 2000 is accepted on Jet3, while 2001 is refused at TableDefs.Append with DAO 3047/HRESULT 0x800A0BE7 (Record is too large). For this exact schema shape, Jet4/ACE accept 4051 and refuse 4052 at the same operation/code. This is not a universal limit for other field counts/types. Stored accepted raw row sizes are 2003 and 4056 respectively, including native count/assignment bytes. Earlier bounded probes covered surrounding totals and narrowed the first refusal one byte at a time.

Four user tables provide the controlled content:

- Fixed2000 (Jet3) or Fixed4051 (Jet4/ACE): Id 1, each binary field filled with byte 0xA0 plus its zero-based field suffix.
- FixedOnly: Id 16909060, Small 1286, Flag True. Native Jet3 raw bytes are exactly `0304030201060507`; Jet4/ACE are `030004030201060507`. No variable offset trailer is present.
- VariableEdges: Id 240..255, A consisting of Id copies of `a`, B=`b`, C=`c`; Id 1240..1255, A containing 255 copies of `a`, B containing Id-1000 copies of `b`, C=`c`. Jet3 single-byte offsets cross 255/256 and 511/512, including jump-table insertion when total row length crosses 256 and 512. Jet4/ACE controls retain their native uncompressed UTF-16 storage.
- DroppedFixed: originally Id,A,Removed,B LONG and Note TEXT(50). Row 1 was inserted as `(1,16909060,84281096,151653132,'before')`; native ALTER TABLE dropped Removed; row 2 `(2,16909060,151653132,'after')` was then inserted. B retains native column number 3 and FixedOff 12. The first row retains old Removed bytes `08070605` in the opaque fixed gap. Native new-row reserved bytes are opaque, not assumed zero; raw copy preserves them exactly.

The focused tests compare complete native raw rows and independently expected visible field slices, then complete private assembly and its independent prefix/TDEF/catalog/allocation comparisons. They require exact original-row byte preservation before native compact. DAO reopens and compacts the staged files and checks all controlled values afterwards; native compact may normalize dropped slots, so no raw-byte equality is asserted across native compact. Producer processes and guarded native scripts run in absolute temporary working directories with PowerShell and process current directories restored and inherited environment retained. This is bounded healthy native layout/raw-copy evidence, not all F8 variants, arbitrary record-shape limits or permission to reclaim unknown storage.
