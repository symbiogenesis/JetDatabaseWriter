# Native calculated English profile

These specimens were authored independently with `DAO.DBEngine.120` version
16.0, Access Runtime `16.0.20527.20112`, on an en-US Windows user profile.
The capture script is `scripts/probe-calculated-native-profile.ps1`.
No JetDatabaseWriter output was used to approve these files.

The 22 calculated fields cover date and numeric text, numeric string coercion,
English month and weekday names, system weekday defaults, mixed-case Unicode,
Windows-1252 Asc and String, and the native two-digit year window. In particular,
the native caches contain `Day("31/12/2020") = 31`, `CDbl("1,5") = 15`, and
`Asc("İ") = 73`.

The compacted specimen is the result of native `DBEngine.CompactDatabase` on
the original database. Tests compare all cached values after a library insert
and after recalculating the existing native row.

| File | SHA-256 |
| --- | --- |
| `native-profile.accdb` | `791DD25255861189A738FB3995D4FD779A1056768E6017228AFFEA38A3260980` |
| `native-profile-compacted.accdb` | `0B080A8E5085B3C2C3A4925E99B5F8E3A3E59DC845F6374968BB71D4E2961FF2` |
