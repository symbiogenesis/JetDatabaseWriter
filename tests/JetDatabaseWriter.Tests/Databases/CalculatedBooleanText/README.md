# Native calculated Boolean text conversion

Microsoft DAO.DBEngine.120 16.0, Access Runtime 16.0.20527.20112, authored these
source controls independently under an en-US Windows user profile. The second
specimen is its native CompactDatabase result. No library output approved them.

Reproduce with scripts/new-native-calculated-boolean-text.ps1.

Intrinsic Boolean values have function-specific conversions. LIKE, Left/Right,
Len, InStr and UCase see their native Boolean words. Asc instead sees -1 or 0,
so Asc(IsNull(Null)) is 45 and Asc(IsEmpty(1)) is 48. Str and Str$ preserve the
intrinsic words True/False exactly, without a leading blank.

Literal True/False and ordinary or calculated Yes/No field references project
signed Integer values first: Str(True) is -1 and Str(False) is a blank followed
by 0. IIf controls retain the distinction between selected intrinsic Boolean
and selected literal Integer branches. All cached values survive native compact.

| File | SHA-256 |
| --- | --- |
| native-text.accdb | B20FFA010EA32E20C43F9DE278C52E9EF19590551212E84F7095FFFED7DD3077 |
| native-text-compacted.accdb | E32C16F4DC78D432C6708B72EB22AD024A7FA55CA874BF35EE464EDBDDFAF8C0 |
