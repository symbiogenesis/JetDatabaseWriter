# Native calculated Boolean comparison

Microsoft DAO.DBEngine.120 16.0, Access Runtime 16.0.20527.20112, authored the
source independently under an en-US user profile. The second specimen is the
native CompactDatabase result. No library output approved these fixtures.

Reproduce with scripts/new-native-calculated-boolean-comparison.ps1.

Relational comparisons project intrinsic IsNull, IsEmpty and Like Boolean
results to the signed Integer text -1/0. Thus IsNull(Null)="True" is false,
IsNull(Null)="-1" is true, and the reciprocal operand orders agree. IIf and
Choose retain the selected intrinsic Boolean; ordinary and calculated Yes/No
references retain their own signed Integer projection. Relational order uses
the database collation, including its punctuation ordering.

This comparison conversion is separate from concatenation, LIKE, Len and
string slicing, which retain the native intrinsic Boolean word conversion.

| File | SHA-256 |
| --- | --- |
| native-comparison.accdb | 7311AD9297128B442881B4B557318BF488BAD130F9A98288688C6470FCDA0001 |
| native-comparison-compacted.accdb | 7B0CDF39814A5DFC2CEB8608FE56CDA7FCC1958866B8392360E796DA8880B58C |
