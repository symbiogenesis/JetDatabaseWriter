# Native calculated field additions

Microsoft DAO.DBEngine.120 16.0 authored these controls on 2026-10-30 using
ACEDAO.DLL 16.0.20527.20112 in x64 PowerShell under an en-US Windows user profile. The native engine creates the
schemas and rows, then appends calculated fields to populated tables. Independent
CompactDatabase results retain the observed values and errors. No library output
was used to approve a native value, and no database-supplied code was executed.

Reproduce with `scripts/new-native-calculated-add-fixtures.ps1` on a DAO120 host.
The generator refuses an existing output directory. New files have fresh native
identities and timestamps; their hashes are not expected to match this capture.

The base controls contain Id/N values (1,2), (2,0), and (3,Null). Appending
`[N]*2` immediately populates 4, 0 and ordinary Null; `1/[N]` populates 0.5,
status 11 and Null. Appending Required calculated fields does not reject existing
Null or error values. The extended controls add a calculated dependency,
Text/Memo error results, and a rejecting field validation rule: existing rows
receive their native caches without applying field Required or validation rules.
Subsequent ordinary insert/update behavior retains its separate constraint pass.

`native-add-rule.accdb` has existing rows that fail the table rule `[N]>10`.
Native field append refuses with 3302 and leaves the field collection unchanged.
Matching valid and invalid table-rule probes establish that table rules remain
active during calculated field addition. The library refuses before mutation;
its focused regression repairs the source values and verifies caller rollback.

The stale controls start from the pinned `CalculatedStale/native-before.accdb`
source and append an ordinary field, constant calculated 42, or calculated
`[V]+1`. Every native addition zeroes existing stale bracketed caches, even the
ordinary-field control. Existing valid calculated caches remain unchanged, and
the new dependent caches status -5617. The library preserves the source's
surviving caches instead of reproducing that native destructive DDL behavior;
it matches the new field's native value/error binding. This is separate from
ordinary update preservation and does not infer a native repair capability.

| Specimen | SHA-256 |
| --- | --- |
| native-add.accdb | 60E0C0781C8B3309EE7449F860547C95AD9A71F13B34706DAFCEA8B810C5C48C |
| native-add-compacted.accdb | 6F7EDFD5EC052D9B79BD33E191F676E10593F8674E98A298F475267CF5DE9438 |
| native-add-extended.accdb | F235D6FA08494C91B1CC54DEAF4B22E215CE2D439EF397547EB4D8D48E00E892 |
| native-add-extended-compacted.accdb | 404EAAB9B16D295F706AAFCF6FD15E9F5552024FC1CDD8F29442ADAA31C68268 |
| native-add-rule.accdb | EF49A9B43AEE953291CB43C73A68BD772F67CC049A2C8C9AE62126384D7C62A4 |
| native-stale-dependent.accdb | 78A3973E94792FFF53B4D0B7D82B7950E93DCB192EFE83432E2FF431EE4EE3D4 |
| native-stale-independent.accdb | DE3EC2DE49D1D45BB50F8ED4213F831113267AA124D1D00213A4569BB06414A7 |
| native-stale-plain.accdb | 84E8692EA06BC78967E6BA208FE6FDBD64B529530270A71C3056203FC74B98AF |
