# Native calculated condition and Boolean controls

Microsoft Access Runtime 16.0.20527.20112 / DAO.DBEngine.120 version 16.0
authored these source databases independently through x64 PowerShell 7.6.9 on
Windows 11 ARM64. DAO CompactDatabase produced each separate compacted control.
The locale is LANGID 0x0409, CP1252; no library writes produced these fixtures.

Regenerate into an empty directory using
`scripts/new-native-calculated-logical-fixtures.ps1` with a DAO-capable host.
The two input JSON files record the exact expressions, native result types and
table order. Some diagnostic functions are deliberately refused, leaving only
their source Id column; tests select the admitted controls explicitly.

The fixtures distinguish IIf condition truth, eager Choose arguments, bitwise
string conversion, numeric Boolean-result coercion and native cached statuses.
Native Boolean results round to signed Integer before testing zero, while IIf
conditions retain nonzero floating values and non-null strings, including empty
strings. Native Decimal Boolean overflow is status13; Long/Double overflow6.
The independent raw envelopes establish each error status, separately from DAO's
COM-null presentation. Ordinary Null is preserved distinctly.

| File | SHA-256 |
| --- | --- |
| logical.accdb | 3F5A27ACDC8EEA1F1766F11572A066A172FA01A7686BC1230776EFE36629A25B |
| logical-compacted.accdb | CF43C55F1D4BBBEE85EF6512F11EB3592DFCEF6D09BBF7F5B8523BEBB6BB757A |
| boolean.accdb | A923D3D8DFE8A36A5C25156C9A554246736AF96499821DDA43C2C7B13AA656AE |
| boolean-compacted.accdb | F4FF0782D94CC8A2055EA1E1F50DB88B787B3520627F518D7C1C642123939D3A |
