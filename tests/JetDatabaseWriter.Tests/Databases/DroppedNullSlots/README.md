# Native dropped slots beside scalar Nulls

These specimens were authored on 2026-10-30 through x86 `DAO.DBEngine.36`
version 3.6, `dao360.dll` 10.0.26100.5074. `CreateDatabase` used locale
`;LANGID=0x0409;CP=1252;COUNTRY=0` and options 32 (Jet3) or 64 (Jet4).
Both schema and rows were authored by Microsoft DAO. The compacted specimens
were made directly from the native source before any library mutation.

| Specimen | Bytes | SHA-256 |
|---|---:|---|
| jet3-source.mdb | 49152 | B9CB0072A843BC54B5DEF082E595A9E9C00A812A0D3DA4C4CDEDB7C4C77C6944 |
| jet3-compacted.mdb | 49152 | D6D7AFE586AFB97B679F2CA6A98D6D25BB324B9883051BF56CF2290A48B9A31A |
| jet4-source.mdb | 81920 | ED7541E050837F6247D7D8CD11E6BFA9DD6FE7432268BF746ED83AC92F4A6ACB |
| jet4-compacted.mdb | 81920 | D753BD46353F0797F330BB512D429A155BFCCD8F9341AC6C8D55256C862507E7 |

The original Items columns were Id LONG, RemovedHead LONG, Value DOUBLE,
RemovedMiddle LONG, TailValue GUID, RemovedTail DOUBLE, Note TEXT(40) and
RemovedVariable TEXT(40). Native row 1 assigned controlled values to every
fixed field, including 16909060, 84281096, 123.5 and GUID
`00112233-4455-6677-8899-aabbccddeeff`, and Note `before`. Value and TailValue
were then set Null. DAO dropped all four Removed fields and inserted row 2 with
Note `after`, again setting the surviving Double/GUID fields Null after their
initial assignment. The resulting visible rows are `(1, Null, Null, 'before')`
and `(2, Null, Null, 'after')`.

Source Value/TailValue physical column numbers are 2/4 with fixed offsets 8/20.
Native compact renumbers them to 1/2 with offsets 4/12. Excluding the column-count
prefix (one byte Jet3, two Jet4), row 1 retains 44 fixed bytes and the native
post-drop row uses 36; native compact reduces both to 28. The old row retains
controlled removed-field bytes. Native Null slots and newly allocated gaps can
contain opaque allocator bytes; they must not be inferred to be zero or treated
as assigned values. Complete hashes retain the exact observed bytes.

`NativeDroppedNullSlotTests` compares frozen hashes, raw retained sentinels and
text offsets, typed/string/POCO reads and writer insert/update on private copies.
Fresh DAO3.6 controls separately author equivalent native databases, compact an
untouched baseline, then accept writer insert/update and Null reassignment
through native read, update and compact. Scanned-row and Note-field counts guard
against empty geometry comparisons. Fresh DAO120 Jet4/ACE controls remain guarded and skipped on this host;
other numeric/date and dropped-slot variants remain F8/E2 requirements.

ASCII and UTF-16 hygiene checks found no producing user/machine/profile/worktree
path, URL, AutoExec or VBAProject markers. No embedded content or external data
source was executed. These fixtures are separate from approved golden bytes.
