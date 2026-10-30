# Native calculated LIKE controls

These databases retain independently authored Microsoft ACE16.0.20527.20112 calculated results. Each `LikeCases` row carries the source, pattern, Double and Boolean LIKE caches, and three error-masking controls. `cases.json` records the native COM observations; an ordinary `DBNull` and a COM calculation-error null are recorded separately.

`native-like-legacy.accdb` was created through `DAO.DBEngine.120`. `native-like-general.accdb` starts from the repository's native [Jackcess V2010 seed](../Jackcess/V2010/testV2010.accdb), whose [fixture notices](../Jackcess/THIRD-PARTY-NOTICES.txt) apply. Newly created source columns have sort-order1033/version0 and1033/version1, respectively. Both variants produce the same expected manifest. The `-compacted` specimens come from the same engine's `CompactDatabase`.

The controls distinguish native character ranges from linguistic index comparisons, pin digraph expansion and UTF-16 wildcard boundaries, retain ANSI-89 and fullwidth wildcard operators, and distinguish a lazily reached bracket error from validation after the first reached star. Invalid patterns cache signed status93 in an otherwise zero39-byte envelope: `5D000000` followed by35zero bytes. Text-result conversion of that error refuses insertion with3802. The masking columns retain42,False,andTrue.

Reproduce with the x64 PowerShell host capable of activating the installed engine:

```powershell
pwsh -NoProfile -File scripts/create-calculated-like-fixtures.ps1 -OutputDirectory artifacts/like-legacy
pwsh -NoProfile -File scripts/create-calculated-like-fixtures.ps1 -OutputDirectory artifacts/like-general -Seed tests/JetDatabaseWriter.Tests/Databases/Jackcess/V2010/testV2010.accdb
```

The generator reads only Source and Pattern from the manifest and obtains new expectations from native calculated fields. It refuses to overwrite an existing output. The tests pin specimen SHA256 hashes and compare both native caches and new writer output with the recorded expectations.