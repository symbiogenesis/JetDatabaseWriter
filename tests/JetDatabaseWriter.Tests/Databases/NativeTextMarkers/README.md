# Native Text marker controls

These specimens were produced on 2026-10-22 with x64 DAO120, ACEDAO.DLL 16.0.20527.20088. The writer created the ACE schema; DAO inserted all seven data rows using explicit constant ChrW expressions. DAO then read each original Text and cached result and called CompactDatabase into a distinct output. No database-supplied query, macro, VBA, link or provider was executed. Native processes ran in absolute temporary working directories with inherited system/profile environment variables.

| Specimen | Bytes | SHA-256 |
|---|---:|---|
| native-calculated.accdb | 196608 | 0A7109CE897614EA0AF897E5882532DBFCD35B08F7A2AD5340551412424369CA |
| native-compacted.accdb | 192512 | 8098D75FA0BDD45FDCF1D29E0103B2AAC83FF7564D971EB0D944B0603FC93D47 |

Items has Id LONG PRIMARY KEY, Content TEXT(255) with Unicode compression disabled, and Cached calculated TEXT(40), expression `[Content]`. Ids are 0 through 6. Content values, expressed as code units, are `FEFF`, `FEFF tail`, `FEFF 4E2D`, `FEFF 03A9`, `FEFF 0100`, `FEFF 0200` and `FEFF 0000 tail`. Every Content value survives native read and compact exactly.

The independently observed native Cached results are empty string, `FEFF tail`, `002D 004E`, `00A9 0003`, `0001`, `0002` and `tail`. These native cached conversions do not preserve every source Content value. The library must read those stored results faithfully rather than invent the intended expression output. Source cached payloads include `FF FE 00 FF FE 00 74 61 69 6C` (escaped FEFF plus tail) and `FF FE 00 01` (native terminal low byte with an implicit zero high byte). Native compaction normalizes some envelope payloads while retaining the independently observed cached results.

Frozen ordinary tests check hashes, exact raw payload controls, typed/string/POCO/DataTable reads and unchanged specimen bytes. Guarded tests regenerate equivalent native data and verify both source and compacted results. Writer insert/update reproduces the measured native leading-FEFF calculated Text conversion, including cases whose cached result differs from the source text. `CalculatedTextLeadingMarkerDaoTests` checks exact writer payloads and cached values; guarded controls compare independently native-authored values with writer insert/update, native recalculation and compact. Unverified compressed ordinary Text shapes still refuse; the independently verified ordinary Text subset is described below. Ordinary uncompressed Text remains writable. These controls do not establish every Text/Memo variant or reclamation and durability acceptance.

## Ordinary compressed Text

These additional specimens were produced on 2026-10-29 using x86 DAO3.6, `dao360.dll` 10.0.26100.5074. The library created a Jet4 schema with `Items(Id LONG, Content TEXT(255))`; the native engine authored every data row through `Recordset.AddNew` and `Update`, read each stored value, and called `CompactDatabase` into a separate file. These are writer-created schemas with native-authored data, not entirely Microsoft-authored databases. No database-supplied code or external data source was executed.

| Specimen | Bytes | SHA-256 |
|---|---:|---|
| native-compressed.mdb | 122880 | 273A0B52E5D628D05CFF57749A7EB0D7ADA69202842E2B22840131AB5777E9DD |
| native-compressed-compacted.mdb | 81920 | 39C7378259246B61A6A18D720C001C6A7972A05D3432F7B7D76D8197783B18E2 |

The Text descriptor's compression bit is set in both specimens. Ids 0 through 5 were submitted as U+FEFF followed by 5, 39 and 254 `b` characters, `tail!`, `a`, and U+4E2D plus `tail`. Native reads before and after compact preserve the first four and final values exactly. The short `FEFF a` input already reopens as `a` before compact; tests preserve this observed result rather than reconstructing the submitted input.

The first four raw field payloads are exactly `FF FE 00 FF FE 00` followed by their nonzero Latin-1 tail bytes. The short input stores `FF FE 61 00`, and the wide-tail control stores `FF FE 00 FF FE 2D 4E 00 74 61 69 6C`. A separately appended DAO `UnicodeCompression=True` property on a native-created column did not set its descriptor compression bit, so property-only controls are excluded from these specimens and compression claims.

`NativeCompressedTextMarkerTests` pins both hashes, descriptor flags, exact escape controls and all public Text read surfaces. The writer supports leading FEFF ordinary compressed Text with 6 through 255 UTF-16 units and a nonzero Latin-1 tail, within the descriptor's declared character limit; it never truncates an escape to make it fit. A contiguous wide prefix may follow FEFF: each unit exceeds U+00FF, has a nonzero low byte and is not a surrogate. At least four Latin-1 units follow a wide prefix; without a wide prefix, at least five follow FEFF. The payload is `FF FE 00 FF FE [UTF-16LE wide prefix] 00 [Latin-1 tail]`.

Fresh DAO3.6 controls independently author all accepted test values in a writer-created empty schema and compare complete native payloads before and after compact with writer payloads. Prefix controls include U+4E2D, U+03A9 and both together; 250 wide units plus four Latin-1 units and one wide unit plus 253 Latin-1 units exercise the 255-unit limit. Native checks also verify writer-created values, updates and compact, including all 255 nonzero Latin-1 tail characters. Short values/tails, NUL, low-byte-zero wide units, surrogates and wide units after the Latin-1 tail retain preallocation refusal for ordinary compressed Text. Calculated Text follows its separate native cached-conversion rules above. DAO3.6 Jet4 checks ran locally; fresh DAO120 Jet4/ACE controls remain guarded and require an equipped host. These fresh controls add no new frozen specimen or broad mixed-character guarantee.
