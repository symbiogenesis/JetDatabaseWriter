# Microsoft Access "long row" indexed-text key encoding

Reverse-engineering notes on how Microsoft Access (Jet 4 `.mdb` and ACE
`.accdb`) encodes single-column text-index leaf entries when the indexed
value exceeds the canonical 127-character cap.

The reference fixtures are the Jackcess `testIndexCodesV{2000,2003,2007,2010}`
corpus (Apache 2.0). The widely-mirrored upstream comment is:

> `TODO long rows not handled completely yet in V2010 — seems to truncate
> entry at 508 bytes with some trailing 2 byte seq`
> *(Jackcess `IndexCodesTest.findRow`)*

---

## Summary

| Format | Strategy | Max entry | Char cap | Status |
|--------|----------|----------:|:--------:|:------:|
| V2000 / V2003 / V2007 (Jet 4, "General Legacy") | 2-chunk split at first line break | 291 B | 255 across both chunks | ✅ byte-exact |
| V2010 (ACE, "General") | Continuous encode, 510-byte hard cap, 2-byte suffix | 510 B | 255 (fixed) | 🟡 508/510 bytes correct |

---

## V2000 / V2003 / V2007: 2-chunk algorithm ✅

### CR/LF is NOT a synthetic separator

What was previously documented as a "4-byte separator" is actually the
normal inline encoding of the embedded CR/LF in the source text:

| Char | General Legacy | General |
|------|:-:|:-:|
| CR (U+000D) | `08 07` (SIMPLE) | `07 09` (SIMPLE) |
| LF (U+000A) | `08 04` (SIMPLE) | `07 06` (SIMPLE) |

### Algorithm

When `len(val) > 127` AND the value contains a CR or LF:

1. Split at the first CR or LF. Skip the CRLF pair if present.
2. Encode chunk #1 `val[0..splitAt)` through the per-codepoint state machine.
3. Encode the CR+LF characters normally (they produce `08 07 08 04`).
4. Encode chunk #2 `val[resumeAt..min(255, len))`, trailing spaces trimmed.
5. Append END_TEXT (`0x01`) + unified extras/unprintable/crazy + END_EXTRA_TEXT (`0x00`).
6. Descending: one's-complement everything after the flag, append `0x00`.

Character offsets for extras/unprintable/crazy are **continuous** across
chunks — a single `charOffset` counter runs over the full inline emission.

### Empirical sample — V2000, `Table11.DataIndex` (ascending), row 2

Source: 901 chars, 5 identical 180-char lines separated by `\r\n`.
On-disk leaf entry: **278 bytes**.

```
7F                          ← flag (ascending non-null)
[178 bytes]                 ← chunk #1: val[0..179)
08 07 08 04                 ← CR + LF (inline, not a separator)
[74 bytes]                  ← chunk #2: val[181..255)
01                          ← END_TEXT
02 02 02 02 02 02 02 02 02 02 02  ← extras (Á at position 12)
0E 01 01 01 80 07 06 82    ← unprintable codes (- at position 0)
00                          ← END_EXTRA_TEXT
```

### Validation

Byte-exact for all 3 long rows × 2 directions × 3 format versions = 18
leaf entries. Implementation: `GeneralLegacyTextIndexEncoder.EncodeTwoChunks()`.

---

## V2010: Continuous encoding with 510-byte cap 🟡

V2010 uses a fundamentally different strategy — no chunk split, no
trailing-space trim, different code tables ("General" vs "General Legacy").

### Algorithm

1. Encode up to **255 characters continuously** (CR/LF encode as normal
   inline bytes `07 09 07 06`, no special handling).
2. Build the full entry: flag + inline + END_TEXT + extras + END_EXTRA_TEXT.
3. Apply descending complement if needed.
4. Hard-truncate at **510 bytes**.
5. Replace bytes [508..509] with a **2-byte suffix** (algorithm unknown).

```
encode_long_row_v2010(val, ascending, codes, ext_codes):
  chars = val[0 .. min(255, len(val))]   # no trim
  entry = [FLAG]
  emit_inline(chars, codes, ext_codes, entry, state)
  finish_entry(entry, state, ascending, max_length=510)
  # TODO: entry[508..509] = compute_suffix(???)
  return entry
```

### Leaf-to-row mapping

Ascending leaf entries are sorted by key value, NOT by row order:

| Data row | Leaf index | Special chars (positions 0–254) |
|---------:|-----------:|:--------------------------------|
| row[2]   | leaf[2]    | `-` (Unprintable) @0, `Á` (International) @12 |
| row[3]   | leaf[4]    | `-` (Unprintable) @3, `í` (International) @25 |
| row[4]   | leaf[3]    | `-` (Unprintable) @86, `-` (Unprintable) @102 |

An earlier analysis used `ascKeys[rowIdx]` directly (row[3]→leaf[3]),
which showed `firstDiff=50`. This was a mapping error, not an encoding
error. With the correct mapping, all three rows match to byte 508.

### Character count is fixed at 255

All three rows encode exactly 255 source characters. Unprintable chars
produce 0 inline bytes, so inline byte count varies, but the character
count is constant. The encoder does NOT use a byte budget.

### Full (untruncated) entry structure

| Row | Full length | Truncated bytes | Tail contents |
|----:|------------:|----------------:|:--------------|
| 2   | 530 | 20 | END_TEXT + 11×extras(02) + extras(0E) + 01 01 + 80 07 06 82 + 00 |
| 3   | 543 | 33 | END_TEXT + 24×extras(02) + extras(0E) + 01 01 + 80 13 06 82 + 00 |
| 4   | 520 | 10 | END_TEXT + 01 01 01 + 81 5F 06 82 + 81 9B 06 82 + 00 |

### The 2-byte suffix

Observed values (ascending mapping corrected; descending need re-verification):

| Row | Ascending | Descending |
|----:|:---------:|:----------:|
| 2   | `43 EC` | `37 DD` |
| 3   | `A2 2D` | `9A 4E` (verify) |
| 4   | `1D AC` | `C1 A1` (verify) |

Properties:
- **Not complements**: `~43EC = BC13 ≠ 37DD`. The suffix is computed
  independently per direction.
- **Varies per row**: encodes something about the truncated extras/unprintable
  section.
- **Not a function of omitted byte count** (20, 33, 10 — no relationship).
- **Not the next character's inline bytes** (char[255]='  ' → `07 02` ≠ `43 EC`).

### Exhaustive suffix testing — no match found

~3.4 million algorithm × input combinations tested per row. Zero matches.

**Algorithms**: CRC-16 (CCITT, CCITT-FALSE, XMODEM, ARC, MODBUS), all 262144
brute-force CRC polynomials (65536 × 4 modes), Fletcher-16, FNV-1a-16, DJB2-16,
XOR-fold, sum-mod-65536, Pearson-16, Adler-16, MurmurHash3 (seeds 0–255).

**Inputs**: truncated tail `[508..]`, `[509..]`, `[510..]`, `[508..^1]`,
full untruncated entry, entry minus flag, retained portion `[..508]`, inline
only `[1..508]`, remaining source text (UTF-16LE and ASCII), full source text.

**Other hypotheses disproven**: Windows NLS sort key (completely different
format), pure inline continuation (same bytes regardless of char count beyond
255), "CRC-16 of truncated data" (AI-generated claim, empirically false).

### Remaining candidates

1. Proprietary hash with a non-standard polynomial or mixing function.
2. Function of raw on-disk compressed text bytes (LZSS).
3. Internal ACE state (page/row numbers) mixed into the computation.
4. Recoverable only via disassembly of `acecore.dll`.

---

## Branches without fixtures

- **Long input with no CR/LF (V2000–V2007)**: falls back to 127-char
  single-chunk (matching Jackcess). No fixture exercises this path.
- **V1997 "General 97"**: different state machine, not investigated.
- **V2010 binary long keys**: may share the 2-byte suffix mechanism.

---

## Reproduction recipe

### V2000 / V2003 / V2007

1. Open a `testIndexCodesV20{00,03,07}` fixture.
2. Walk `Table11.DataIndex` ascending leaf B-tree.
3. The `08 07 08 04` sequence in long entries is CR+LF encoded normally.
4. Re-encode `val[0..splitAt)` + CR+LF + `val[resumeAt..255)` through
   the per-codepoint state machine with continuous char offsets.
5. Confirm byte-exact match.

### V2010

1. Open `testIndexCodesV2010.accdb`.
2. Walk `Table11.DataIndex` ascending (firstDp=112) and descending
   (firstDp=119).
3. Long-row entries are exactly 510 bytes.
4. Encode 255 chars continuously, build full entry, truncate to 510.
5. Bytes [0..507] match. Bytes [508..509] are the unknown suffix.
6. Ascending vs descending suffixes are NOT complements.

---

## Status

| Scope | State |
|-------|-------|
| General Legacy (V2000/V2003/V2007) Table11/Table11_desc | ✅ byte-exact, `GeneralLegacyEncoderFixtureTests.cs` |
| General (V2010) Table11/Table11_desc | 🟡 508/510 bytes, suffix unknown, `GeneralEncoderFixtureTests.cs` |
| V2010 binary long keys | ⬜ skipped |
| V1997 "General 97" long rows | ⬜ not investigated |

### Diagnostic probe

`LongRowSourceProbe.DumpV2010SuffixAnalysis` in
`JetDatabaseWriter.Tests/Internal/LongRowSourceProbe.cs` is a skipped
`[Fact]` that dumps char-by-char inline analysis around the 508-byte
truncation point and exercises candidate suffix algorithms. Un-skip it
locally to regenerate diagnostics when investigating the 2-byte suffix.

---

## Appendix A: fixture row details

The three long rows in V2010 `Table11` are 901, 901, 903 chars. All contain
`\r\n` at positions 179–180. Code table mappings: `-` (U+002D) = Unprintable
(0 inline, unprint `82`), `Á` (U+00C1) = International (inline `0E02`,
extra `0E`), `í` (U+00ED) = International (inline `0E32`, extra `0E`).

`í` / `i` share inline bytes `0E32`; `Á` / `A` share `0E02`. International
chars are distinguished from Simple only by the extras stream.

Ascending leaf page: 112 (13 entries total). Descending: 119.

## Appendix B: superseded documents

- **`long-row-bisect.md`** — Early chunk-boundary bisection output.
  V2000–V2007 data is accurate; V2010 interpretation (treating
  `07 09 07 06` as a separator) is incorrect. Historical only.

- **`long-row-probe-dump.md`** — Full hex leaf entries for all fixture
  rows. Active reference for debugging.
