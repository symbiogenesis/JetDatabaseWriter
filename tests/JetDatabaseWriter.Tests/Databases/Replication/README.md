# Inert native Jet4 replication controls

These databases were authored on 2026-10-22 by an isolated x86 `DAO.DBEngine.36`
process on Windows 10.0.26300.0 ARM64. The engine reports version 3.6;
`dao360.dll` reports 10.0.26100.5074, and `MSJET40.DLL` and `msjtes40.dll`
report 10.0.26100.9549. The database format reports 4.0. Creation used
`CreateDatabase` with version 64 and `;LANGID=0x0409;CP=1252;COUNTRY=0`.

The producer created one ordinary `Items` table with `Id` (Long, primary key),
`Label` (Text, 80) and `Notes` (Memo). It inserted only the following literal
content: Id 1, Label `first`, Notes `inert short memo`; Id 2, Label `second`,
Notes `memo-` followed by 5,000 `x` characters. The resulting external Memo
storage is part of the frozen evidence.

The producer converted this database to a Design Master by appending
`Database.CreateProperty("Replicable", 10, "T")` to its Properties collection.
It called `Database.MakeReplica` with a new local absolute filename,
description `Controlled inert full replica` and options 0, creating a full,
read/write replica. No synchronization, network transport, replica partner,
saved query, expression, macro or VBA operation was invoked. Native-generated
host/path values in replication tables remain unchanged opaque evidence.

| File | Bytes | SHA-256 |
|---|---:|---|
| Jet4-DesignMaster.mdb | 385,024 | `AB3390B4DA7C87C65BCFC5376AF2A3E989676E9B4FC1D51C86E05D993CC0AA0E` |
| Jet4-FullReplica.mdb | 389,120 | `A4C286EDABCD06FD1E2BDFB2815244A918CF020D96D4C0923EBCF6760C9D2F95` |

The Design Master's native `ReplicaID` and `DesignMasterID` are both
`7B8B2136-2680-4FCF-AB0F-1940CE0FB38E`. The full replica's `ReplicaID` is
`BD3E443F-0B2C-4028-98AE-3955DAEA2BF6`, and its `DesignMasterID` remains
`7B8B2136-2680-4FCF-AB0F-1940CE0FB38E`.

Both specimens contain 18 local tables and 22 catalog identities. Native
replication added `Gen_Notes`, `s_ColLineage`, `s_Generation`, `s_GUID` and
`s_Lineage` to `Items`, plus the `s_Generation` and unique `s_GUID` indexes.
The added columns retain native attributes 8194. System evidence includes
`MSysRepInfo`, `MSysReplicas`, `MSysRepLock`, `MSysTombstone`, `MSysGenHistory`,
`MSysOthersHistory`, `MSysTableGuids`, `MSysSideTables`, `MSysSchChange`,
`MSysConflicts`, `MSysExchangeLog`, `MSysSchedule` and `MSysTranspAddress`.
The default native principal refuses definition inspection of `MSysACEs`,
`MSysObjects`, `MSysQueries` and `MSysRelationships` with DAO 3110; no permissions
were changed to bypass that restriction.

Read-only native reopen verified database identities, the replicated column/index
metadata and every controlled value without changing either specimen's hash.
`CompactPinnedReplicationTests` pins those hashes, compares every original and
relocated catalog/system-table value and the independently validated raw DATA,
LVAL, index and allocation evidence, and requires complete original-prefix
comparison. The original 94/95 pages remain pinned; only reconstructed known TDEF
references and validated global-map bytes may differ in that prefix. Guarded
DAO3.6 tests compare the same identities, metadata and literal values before and
after private assembly and separate native compact of both the original control
and assembled subject. Tests never synchronize replicas or execute stored code.

The same producer separately created an ordinary format-3.0 Jet3 database using
version 32. Appending the `Replicable` property refused with DAO error 3703:
"Operation not supported on replicable databases that have not been converted to
the current version." The refusal occurred before `MakeReplica`; no Jet3 replica
is claimed. An engine/corpus capable of authoring genuine replicated Jet3 files
remains an external requirement. Both DAO3.5 ProgIDs are unregistered in the
32-bit and 64-bit registry views on this host; activation fails with
`0x80040154` and no `dao350.dll` is present in the standard DAO/Windows
locations. No components were installed or registered.

This evidence covers these two inert, unsynchronized Jet4 specimens. It does not
establish general replication editing, conflict resolution, synchronization,
native replication recovery, arbitrary historical replica sets, complete incoming
ownership, source reclamation, a reclaiming managed compact result or publication
authorization. The assembly retains every original page and can grow the file.

Official API contracts: [Database.MakeReplica](https://learn.microsoft.com/en-us/office/client-developer/access/desktop-database-reference/database-makereplica-method-dao),
[Database.ReplicaID](https://learn.microsoft.com/en-us/office/client-developer/access/desktop-database-reference/database-replicaid-property-dao)
and [Database.DesignMasterID](https://learn.microsoft.com/en-us/office/client-developer/access/desktop-database-reference/database-designmasterid-property-dao).
