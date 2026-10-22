# Native ordinary Memo controls

`native.mdb` (Jet4) and `native.accdb` (ACE) are the existing DAO-authored
32-row `Items(Id LONG, Content MEMO)` controls described in `provenance.json`.
The empty schema was library-created; DAO inserted every row and Memo payload.
Their pinned SHA-256 values are:

| File | SHA-256 |
|---|---|
| native.mdb | F1D9A911859401DB5CAD80D40F637CE0819DA273318F37A388890FD16A8A4FC2 |
| native.accdb | 6CAC20346F956E10297D0D0EA994DA84BB28B4BDBAC2FC206689EBF4B6446C95 |

## Odd declared chained lengths

On 2026-10-22, isolated copies of each pinned file were damaged by decrementing
only the low byte of one ordinary chained Memo descriptor's declared length.
Pointers, data pages, row masks, descriptor mode, token and actual payload bytes
were left unchanged. `PlainChainedMemoDecodePolicyTests` reproduces these exact
mutations and verifies the resulting whole-file hashes; extra binary copies are
unnecessary.

| Row Id | Absolute file offset | Original byte | Damaged byte | Original declared bytes | Damaged declared bytes | Original text |
|---|---:|---|---|---:|---:|---|
| 6 | 85835 | A0 | 9F | 4000 | 3999 | 2000 copies of `b` |
| 30 | 85219 | 02 | 01 | 4098 | 4097 | U+FEFF followed by 2048 copies of `b` |

Both formats use the same offsets in these specimens. These are plain UTF-16LE
chained payloads; the leading FF FE in Id 30 is literal U+FEFF, not a compression
marker. The final stored high byte still exists beyond the shortened declared
length. The raw-byte APIs return exactly the odd declared bytes, while the
complete-chain maintenance audit also observes the trailing byte.

| Derived copy | SHA-256 |
|---|---|
| odd-id6.mdb | 967643A8DE722A68665D768CE7CF48C2A32691BE751DBAC1EBD688D2CB705740 |
| odd-id30.mdb | 2B09D9A0D0DFE4BE22A9B2A7ACB6BBACCF042F7082E950AFC284B01651534CB0 |
| odd-id6.accdb | BEB4CE081039601EEA0CFCE5A0336DEF5EC3C575699477CCC13F931F9F8AF402 |
| odd-id30.accdb | AB59FEB6D3A3010A05B252E236EB8A957F177C8E8E9B4BBBB62B8FB107352178 |

### Independent native observations

The controls were read and compacted with `DAO.DBEngine.120`, engine 16.0,
x64 ACEDAO.DLL 16.0.20527.20088, from
`C:\Program Files\Microsoft Office\root\VFS\ProgramFilesCommonX64\Microsoft Shared\Office16\ACEDAO.DLL`.
The host was x64 PowerShell 7 on Windows ARM64. Each process used an absolute
external temporary working directory; PowerShell location and process current
directory were set there and restored in `finally`. The inherited Windows
environment was retained. Only a literal snapshot query
`SELECT [Id],[Content] FROM [Items] ORDER BY [Id]` and DAO `CompactDatabase`
were used. No saved query, expression, macro or module was invoked. Every input
hash was checked unchanged after the native operations.

The results were identical for Jet4 and ACE:

| Input | Native direct read | Native compacted result |
|---|---|---|
| Unmodified control | All 32 values exact | All 32 values exact |
| Odd Id 6 | Content is native Null (verified before any string cast) | All 32 rows remain; Content becomes the literal 16-character string `################` |
| Odd Id 30 | A 2048-character prefix (U+FEFF and 2047 copies of `b`) | Id 30 is absent; only 31 rows remain |

The UTF-16LE SHA-256 of Id 6 in the undamaged source and compacted control is
`EE9AC42A4DF88DA02589DE2B8CCC0477CC2FADC4E14A589FE4A909BDD21C3C6A`.
For undamaged Id 30 it is
`592A08D66676F6A5C237A4CBDFD8D53CE628FCB6631908F34F317923BF28DCF9`.
The damaged Id 30 prefix hashes to
`D6B040B90101EEC1F6DB9C9EF597DBC73352DFC272B8DA1743C10A8EB8CF8C8A`;
the native compacted Id 6 placeholder hashes to
`835DAC4B7C84CE354FA39CC62D86394DDFBBB34C08E3BE352E0C6AF95DE0D061`.

The native compacted files from this observation had these SHA-256 values:

| Output | SHA-256 |
|---|---|
| control-compacted.mdb | A365B36FBD3918CA508042A154F9DFA7EC5433D3E31568DCBE56BA8A05B1AC79 |
| odd-id6-compacted.mdb | FB7AB8FBE751FB210D33FCE1BF352C23F268579E7C69B3F466EE33D428E19586 |
| odd-id30-compacted.mdb | 3C8B402AC4878046F0FBD7BEA204D1785F06A4F6C8445EE7A3E9CDB54ED2B013 |
| control-compacted.accdb | D81F424A23F70CAA67C56F6E880A0C33E952810CFBFF829CEEF7980566544B47 |
| odd-id6-compacted.accdb | E5150F7B048180830E9241DC3E85E20B2348DE36A7C987FDF04B45DE86A1B016 |
| odd-id30-compacted.accdb | E3332D36048A6B91E277197DCD974FAEA18867F087029EFF75BB3CED35EE145F |

These compacted hashes identify the observed outputs, not deterministic DAO
reproduction expectations. Successful native compact did not preserve the
damaged value or even its row. The library therefore diagnoses the incomplete
plain UTF-16 code unit through its strict/lenient long-value policy and refuses
unrelated write-back. It preserves stored raw bytes for diagnosis. It does not
emulate the native Null, placeholder or dropped row.

This evidence covers ordinary plain chained Memo in these Jet4/ACE specimens.
It does not classify malformed inline/single-row compressed Memo, calculated
values, Jet3 ANSI bytes, invalid surrogate pairs or other damage patterns.
