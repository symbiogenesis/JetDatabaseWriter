# Native Jet3 descriptor fixture

`NativeDescriptorMetadata.mdb` was produced on 2026-10-15 by the x86 Microsoft `DAO.DBEngine.36` engine (reported version `3.6`), without JetDatabaseWriter. It is an unencrypted Access 97 / Jet3 file with no password, code page 1252, and General 97 sorting.

SHA-256: `5D5396738DFCB7EC5BA12F0186F5691C0E845370C9C4BDB7EECBFED5EB9A265C` (53,248 bytes).

The producing script used `CreateDatabase(path, ';LANGID=0x0409;CP=1252;COUNTRY=0', 32)` followed by these statements with `dbFailOnError` (`128`):

```sql
CREATE TABLE [Native] (
  [Id] LONG, [Flag] BIT, [Tiny] BYTE, [Small] SHORT,
  [Amount] CURRENCY, [Ratio] SINGLE, [Weight] DOUBLE,
  [Occurred] DATETIME, [Label] TEXT(40), [Notes] MEMO,
  [Payload] OLEOBJECT, [Identity] GUID
);
CREATE UNIQUE INDEX [PK_Id] ON [Native] ([Id]) WITH PRIMARY;
CREATE INDEX [IX_Amount] ON [Native] ([Amount]);
CREATE INDEX [IX_Label] ON [Native] ([Label] DESC);
INSERT INTO [Native]
  ([Id],[Flag],[Tiny],[Small],[Amount],[Ratio],[Weight],[Occurred],[Label],[Notes])
VALUES
  (1,True,7,-12,12.3456,1.25,-3.5,#2001-02-03 04:05:06#,'native text','native memo');
INSERT INTO [Native] ([Id],[Amount],[Label],[Notes])
VALUES (2,12.3456,'native text','second memo');
INSERT INTO [Native] ([Id],[Amount],[Label],[Notes])
VALUES (3,-99.99,'other text','third memo');
```

The producing engine closed the source, compacted it to the frozen file with the same locale and format `32` options, reopened it read-only and verified the Currency and Text values and all three indexes. The file contains no library-authored schema or synthetic byte edits. `Jet3DescriptorPreservationTests` pins its hash, the twelve native type codes, descriptor bytes 7–12 (`01 00 09 04 E4 04`) and the high byte of each page-zero two-byte slot at `0x600`–`0x7FF` (`01`). The low slot bytes remain uninterpreted and must survive writer edits unchanged.

Ordinary regressions check new writer descriptors, native descriptor preservation through Add/Drop/Rename in every write mode, surviving values, temporary-table cleanup and the complete original page-zero tail after schema and row mutations. Guarded DAO3.6 checks independently regenerate the descriptor declarations, compare writer-created descriptors, and verify each original-native-table rewrite through native read, insert, update, delete, CompactDatabase and compacted readback.
