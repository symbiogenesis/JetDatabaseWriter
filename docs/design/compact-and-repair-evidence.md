# Compact & Repair damage controls

## Scope and producing engines

These are native behavior observations, not acceptance of an implemented managed
Compact & Repair API. Each control was created by Microsoft DAO with 100 known
rows, a primary key, a secondary index and long Memo values. Jet3 and Jet4 used
32-bit DAO3.6 (engine version 3.6); ACE used 64-bit DAO120 (engine version 16.0,
ACEDAO.DLL 16.0.20430.20146). These are particular engine/specimen observations,
not a claim about every JET/ACE variant or all damage of a given kind.

Each damaged input differs from its control at exactly one independently
verified byte. Native compaction writes a distinct destination. Input and
control hashes remain unchanged. Output inspection checks known row identities
and full values, index definitions, primary-key Null enforcement and
`MSysCompactErrors`; duplicate probes use populated or explicitly seeded validation
copies. Jet3/Jet4 empty primary-index-damage outputs have only the Null probe.
A successful call or an unchanged row count is insufficient.
Constraint mutations use separate validation copies.

| Control | Page size | Table-definition root | SHA-256 |
|---|---:|---:|---|
| Jet3 | 2,048 | 20 | `E982B4BBA169EC8908C7D1DBFED8DD25AAF453AC845222A18462BC2FB6500FE1` |
| Jet4 | 4,096 | 16 | `7984BC61A7ABB028390F2B7A9074850BEE5025587DFDF51C7A0B8F683AA81B02` |
| ACE | 4,096 | 42 | `AF3729233D88EBF39D09FBAE31FDF200F7C55DD16E0CCF1A4CC3E4ACCD7C3FB9` |

The controls, damaged inputs, compacted outputs, scripts, hashes and JSON
observations are retained in the `compact-20261016-ca7e2e58` session evidence
bundle. They are not committed fixtures. Healthy controls retain all 100 exact
rows and both indexes; duplicate primary keys fail with DAO 3022 and Null keys
with DAO 3314. The controls cover neither encryption nor application/security
objects; those remain separate acceptance requirements.

## Independently specified damage

All offsets below are absolute byte offsets. The table records native output;
it does not authorize the managed implementation to discard readable content.

| Format | Damage | Page / offset | Native compact result |
|---|---|---|---|
| Jet3 | Primary-key leaf type `04` to `7F` | 23 / 47,104 | Success; zero of 100 rows; both index definitions retained |
| Jet4 | Primary-key leaf type `04` to `7F` | 18 / 73,728 | Success; zero of 100 rows; both index definitions retained |
| ACE | Primary-key leaf type `04` to `7F` | 44 / 180,224 | Success; zero of 100 rows; both index definitions retained |
| Jet3 | Secondary index leaf type `04` to `7F` | 90 / 184,320 | Success; all 100 exact rows and both indexes recovered |
| Jet4 | Secondary index leaf type `04` to `7F` | 70 / 286,720 | Success; all 100 exact rows and both indexes recovered |
| ACE | Secondary index leaf type `04` to `7F` | 45 / 184,320 | Success; all 100 exact rows and both indexes recovered |
| Jet3 | DATA page type `01` to `7F` | 26 / 53,248 | Success; 86 exact surviving rows plus 14 placeholder rows; primary key removed |
| Jet4 | DATA page type `01` to `7F` | 21 / 86,016 | Success; 85 exact surviving rows plus 15 placeholder rows; primary key removed |
| ACE | DATA page type `01` to `7F` | 49 / 200,704 | Success; 81 exact surviving rows plus 19 placeholder rows; primary key removed |

No `MSysCompactErrors` table was present in these outputs, including the
zero-row and placeholder cases. The DATA-page placeholders have Null numeric
identities and literal sixteen-character `################` Text/Memo values.
Their primary-key uniqueness constraint is gone: a duplicate is accepted while
Null insertion remains refused by the retained field requirement. Counting 100
returned records would conceal the loss of 14, 15 or 19 actual rows.

For the damaged Jet3/Jet4 primary-key specimens, a read-only native walk through
the intact secondary index still returns all 100 known identities, while the
damaged primary-key walk returns none. The logical index names, roles and
physical slots were verified separately. Index role therefore matters when
comparing native recovery; a primary-key root and a secondary leaf are not
interchangeable damage controls.

Separate count-hint controls change only each native TDEF's stored row-count
field from 100 to 1000. Strict public reads, native reads and native compaction
still retain all 100 exact known rows on Jet3, Jet4 and ACE. The ACE output also
retains both index seeks and duplicate/Null enforcement. A stored count mismatch
is therefore advisory evidence, not proof that readable rows must be refused or
that a repair has recovered every object.

## Consequences for managed repair

Native success, reopen success, preserved index names, absence of
`MSysCompactErrors` and matching row counts do not prove complete recovery.
Validate content and constraints independently. A damaged index must not make
independently readable DATA rows disappear from the recovery plan. Do not invent
Nulls, native-looking placeholders or values for lost rows. Partial recovery
must identify omissions and constraint changes and must never automatically
replace the source.

The public full-table reader at revision
`a9d8a4f8b71cb70d78eee88869f0ea8ee919b416` was also exercised against all twelve
native healthy/damaged inputs in strict and lenient modes, using the exact
CI-packaged library. Both modes retain all 100 faithful rows for the damaged
index cases. For a wrong-type DATA page, both return only the faithful surviving
86/85/81 rows without an exception or a corruption warning in the captured
`LastDiagnostics`, standard output or standard error. This baseline completeness
gap is addressed for readable maps with an unknown page type and matching owner;
strict reads refuse and lenient reads trace the incomplete result. The
ordinary reader is not a managed-repair validator. Preserving page-ownership damage
through usage-map fallback and auditing all independently reachable storage are
necessary before using a successful scan as validation.
