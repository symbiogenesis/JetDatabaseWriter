# Native Choose index controls

These files were independently authored on 2026-10-30 with x64 Microsoft
DAO.DBEngine.120, engine version 16.0, ACEDAO.DLL 16.0.20527.20112 and Access
Runtime 16.0.20527.20112. The producer is
[`scripts/new-native-calculated-choose-fixtures.ps1`](../../../../scripts/new-native-calculated-choose-fixtures.ps1).
ADO supplies the ordinary Decimal field declaration; DAO creates the calculated
fields and inserts the Double, Single and nonfinite-expression controls.
The database uses General Legacy, LCID 1033 and CP1252 on an en-US Windows user
profile. No library output approves these fixtures.

`T` covers Double and numeric-text indices; `SingleIndices` and `DecimalIndices`
cover stored source subtypes. `NonfiniteIndices` obtains valid cached negative
infinity from PV and NaN from PMT, then applies Choose both directly and through
the cached field. The compacted file is the result of native CompactDatabase.
`observations.json` retains the native field values/types and original file
hashes. Its original Windows CRLF bytes are preserved on checkout.

Choose converts its index to Single before truncating toward zero. Thus
0.99999999 selects the first branch and 1.99999999 selects the second, while
1.9999999 selects the first. Finite out-of-range indices, including 2147483648,
1E30 and 3.40282355E38, return ordinary Null. Conversion overflow, including
3.4028236E38, 1E100, infinity and NaN, caches status 6. A Null index caches
status 13. Independent inspection of the original and compacted 39-byte scalar
error envelopes establishes these status numbers. Chosen integer literals retain
their four-byte Long representation in Binary results. Fresh native query
controls independently confirm Single conversion and large finite Null results.

| File | SHA-256 |
| --- | --- |
| choose.accdb | E3CBD8DFE40A31D9D5D2D2DE2545369EBAA13352F322AED456BC86ECF572BF52 |
| choose-compacted.accdb | 492D36470C2B9EE135AF5D52124C4D0A8BBFDC52F8D4F81AC65E13A3775ECDC8 |
| observations.json | C85B27B5CD800AC634A7092706CB0553323AF4230B055FDB9DAEEAF202C1D78F |
