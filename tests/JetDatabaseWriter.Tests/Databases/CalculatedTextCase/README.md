# Native calculated Text casing

These two databases were authored on 2026-10-30 by x64 Microsoft DAO120,
`ACEDAO.DLL` 16.0.20527.20112, using `DAO.DBEngine.120` version 16.0.
DAO created the source database with `;LANGID=0x0409;CP=1252;COUNTRY=0`.
It created table `T(Id LONG, Content TEXT(243))`, then appended calculated
Text fields `Upper = UCase([Content])` and `Lower = LCase([Content])`.

The native recordset inserted all 63,487 nonzero, nonsurrogate UTF-16 BMP
code units in ascending order, in chunks of at most 240 code units. Ids
start at zero. DAO read the three Text fields independently and compacted
the database into a separate output. No library row encoding or casing
implementation produced these specimens. No saved query, macro, VBA,
external connection or database-provided code was executed.

| Specimen | Bytes | SHA-256 |
|---|---:|---|
| native-case.accdb | 724992 | FC3613EC3A98F15FFD4AA7D429641E3C0DD1F82A4FAE623599ACC5BBD2119A3A |
| native-case-compacted.accdb | 724992 | 46E46BADA9CDF9C2D77245A63F9BEFAB0B9086C30805E0640D19DA19A4207AD3 |
| native-supplementary-case.accdb | 212992 | 2CBA417BA8561A386C6AC6FF23CC868BBBC8AB73134863C1AE2A20554225C69D |
| native-supplementary-case-compacted.accdb | 212992 | 427A1CB1FFFB73341817F42E681F2F8F32E19C8021DCC496F79970082A3B9180 |

Each native function changes 973 code units and retains every other sampled
code unit. Casing is one code unit to one code unit. In particular, native
UCase retains micro sign, dotless i, long s and final sigma, and native
LCase retains capital sharp s and the Ohm, Kelvin and Angstrom signs.
Newer Unicode Georgian and Cherokee case mappings are absent.

The two supplementary fixtures use the same table and native authoring steps.
They contain all 1,536 supplementary scalars categorized as uppercase,
lowercase or titlecase letters by the capture runtime. Only the 40 Deseret
lowercase letters and 40 uppercase letters change; other sampled scalars
retain their identity.

A separate native diagnostic enumerated **every** supplementary scalar,
U+10000 through U+10FFFF, without filtering by runtime Unicode categories.
Both the original and compacted database were read back through DAO,
checking exact source coverage and every upper/lower result. All 1,048,576
scalars were present; only the same Deseret ranges changed. These larger
18,079,744-byte diagnostic files are retained as session artifacts, with
SHA-256 `8391909BBF76675EB96458DAADDFC9A781B60E55FCA34CB7E603ACD13331B355`
(source) and `FFE0D862C3235A339566B5686A5B4C418F5A2805820FFCD00FBDAD88A9532821`
(compacted). A further native control inserted all 2,048 surrogate units
separated by `|` to prevent pairing, plus NUL; both case functions retained
every code unit.

`CalculatedTextCasing` stores the native nonidentity mappings as compact
first/last/stride/delta ranges. `CalculatedTextCaseTests` checks both pinned
BMP specimens and both supplementary specimens against that table, avoiding
dependence on host NLS/ICU or .NET Unicode-table updates. The evaluator walks
valid surrogate pairs as scalars and retains unpaired units. Other database
locales and native engine versions are separate qualification contexts.
