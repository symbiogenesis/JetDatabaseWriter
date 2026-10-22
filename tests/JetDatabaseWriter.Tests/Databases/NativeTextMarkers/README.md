# Native Text marker controls

These specimens were produced on 2026-10-22 with x64 DAO120, ACEDAO.DLL 16.0.20527.20088. The writer created the ACE schema; DAO inserted all seven data rows using explicit constant ChrW expressions. DAO then read each original Text and cached result and called CompactDatabase into a distinct output. No database-supplied query, macro, VBA, link or provider was executed. Native processes ran in absolute temporary working directories with inherited system/profile environment variables.

| Specimen | Bytes | SHA-256 |
|---|---:|---|
| native-calculated.accdb | 196608 | 0A7109CE897614EA0AF897E5882532DBFCD35B08F7A2AD5340551412424369CA |
| native-compacted.accdb | 192512 | 8098D75FA0BDD45FDCF1D29E0103B2AAC83FF7564D971EB0D944B0603FC93D47 |

Items has Id LONG PRIMARY KEY, Content TEXT(255) with Unicode compression disabled, and Cached calculated TEXT(40), expression `[Content]`. Ids are 0 through 6. Content values, expressed as code units, are `FEFF`, `FEFF tail`, `FEFF 4E2D`, `FEFF 03A9`, `FEFF 0100`, `FEFF 0200` and `FEFF 0000 tail`. Every Content value survives native read and compact exactly.

The independently observed native Cached results are empty string, `FEFF tail`, `002D 004E`, `00A9 0003`, `0001`, `0002` and `tail`. These native cached conversions do not preserve every source Content value. The library must read those stored results faithfully rather than invent the intended expression output. Source cached payloads include `FF FE 00 FF FE 00 74 61 69 6C` (escaped FEFF plus tail) and `FF FE 00 01` (native terminal low byte with an implicit zero high byte). Native compaction normalizes some envelope payloads while retaining the independently observed cached results.

Frozen ordinary tests check hashes, exact raw payload controls, typed/string/POCO/DataTable reads and unchanged specimen bytes. Guarded tests regenerate equivalent native data and verify both source and compacted results. Writer insert/update refuses leading FEFF calculated or compressed ordinary Text pending a complete native-safe encoding. Ordinary uncompressed Text remains writable. These controls do not establish every Text/Memo variant or reclamation and durability acceptance.
