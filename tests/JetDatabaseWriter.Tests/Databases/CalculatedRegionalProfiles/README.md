# Native calculated regional profiles

These controls were authored independently by Microsoft DAO.DBEngine.120 16.0,
Access Runtime 16.0.20527.20112, on six separately configured Windows user
regional profiles. The database LCID remains 1033; each fresh PowerShell process
verified its requested user culture before loading DAO. No library output was
used to approve the controls.

Capture: scripts/probe-calculated-native-profile.ps1. Hosted source revision:
fc93571fb130723920f29bd3477a2a01ef23fea8. Run:
https://github.com/symbiogenesis/JetDatabaseWriter/actions/runs/41118356825.

Each original database and its native CompactDatabase result retain calculated
date and number coercions, all month/day names, system week starts, all ANSI
String byte values, Asc mappings, and Unicode case controls. observations.json
records native expressions, results, engine and operating-system profile metadata.

| Profile | Source SHA-256 | Compacted SHA-256 |
| --- | --- | --- |
| ar-SA | B3DAD0B3668643166B549669FAD9066ED23FA7505BBBDF2997B2D28216EC1C5C | EED3873932112E045356AEF4440D969FD2AC6C6302C211528ADD9419DD6DF5CE |
| de-DE | AF23266A165B391D3A70542C12C3A8906E47AB00750AABED1EF8A2B8806133A6 | 7E3FD5F6AA8084381BC6B2F416F1B454CE36C7E82660E379E417DC17EA1DC453 |
| en-US | E8D9E175E2A7F3F08B53D1563C884E52197999AEF33836E2FAB720DED99A2C6E | 1ECBB8774E257EE4A517BDD7AF8E220EC20B369902330EC6BD856E0B364F9152 |
| fr-FR | 924EABF3A9DACCA4A9ACE10B604E8516293695C09219C02172F84B037C4E2E13 | EA15C0FCA4A642C5B69E18C4BD8799CB0093416715121490082DE4BA790D7085 |
| ja-JP | AB4BEE5C066125D3F02E91B33E1F0EE42D6F80D9F5A441A5E5437CF61984380F | 59DA396196F7BE222D9290065EAD5A7DC96155DE3368894F972BCCE1F435C14E |
| tr-TR | 6A8F01B89C800AF875FDB876672D955DD14FC5A4617D06B1A37FE31B3E27CAE0 | 1F3D8433F7EE0F9B428A67F67E7DB2D93AAF5764904389EC365783C92E8D9DA6 |
