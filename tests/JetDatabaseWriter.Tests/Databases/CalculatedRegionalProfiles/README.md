# Native calculated regional profiles

These controls were authored independently by Microsoft DAO.DBEngine.120 16.0,
Access Runtime 16.0.20527.20112, on six separately configured Windows user
regional profiles. The database LCID remains 1033; each fresh PowerShell process
verified its requested user culture before loading DAO. No library output was
used to approve the controls.

Capture: scripts/probe-calculated-native-profile.ps1. Hosted source revision:
cba67bddd552087e8770c6022d9abb869790f7e0. Run:
https://github.com/symbiogenesis/JetDatabaseWriter/actions/runs/41162301592.

Each original database and its native CompactDatabase result retain calculated
date and number coercions, all month/day names, system week starts, all ANSI
String byte values, Asc mappings, and Unicode case controls. observations.json
records native expressions, results, engine and operating-system profile metadata.

The expanded capture adds native date grammar, numeric logical conversion,
Boolean result and function/concatenation distinctions, and literal/runtime IN
controls. Source and compacted files retain all twelve independently authored
tables. Native cached results remain the oracle when managed evaluation differs.

| Profile | Source SHA-256 | Compacted SHA-256 |
| --- | --- | --- |
| ar-SA | 666C899DDE44C50C829FF107A1B63F8B3A00B9EA27411FEDA0391BF10D37E010 | 93A977EECF7D6BB9FBE38B9FCA5CB00D05F6B571B5EE1E357F415A34E328D7A9 |
| de-DE | 60C7252A2560BE7959CCFC3771FF86FC4486081AE7E2B37BDF212E26EF4FF8E5 | AE7509CD3FD1F5C9BB5776CABBB3AB6F793DA1D366A1E3D897AD898F57EEA241 |
| en-US | 712F8B3AC150DDBAEE88924AB7CC66488EB3650429681835A9E82AB0425FB9FB | F5AE2EAA52F64B58DB1F9DAEA222B59E361B7A5413A23E14B1F96568C19A0A9C |
| fr-FR | 326EC3B48CEB8155098623F150F6267A381FCFAF2451E5517EA698B47A20D38F | 8505EFEE088AE8D992734473FE8F4701B7A3D5085DA11F9F87C280333A11A062 |
| ja-JP | F67121821452078F12B89075980ABFBBC9CD1072D904119D336D7289DBCAF0B0 | D474C5055B62BDE3BBD9B8E2272F850E46BD1E140CC704A14BD3AEE376B4AC5F |
| tr-TR | 0D5493479901BF5071B37F23DF9714F2793406980B1414DE88E9F148A7643993 | C6BB2C88E6E1749D44A8DF400710088B684E842B620265340F1F370F55A7D451 |
