# Native Jet4 bootstrap and catalog security

Fresh Jet4 databases include MSysObjects, MSysACEs, MSysQueries and
MSysRelationships, with the MSysDb, Tables, Databases and Relationships
catalog objects. Core table slots are reserved before their rows and indexes
are allocated. Variable Binary descriptors retain the native SID and query
Order storage flags.

Page zero contains 256 two-byte user slots at 0xE00 through 0xFFF. Native
fixtures mark each odd byte as available with 0x01. Initializing those bytes
is necessary: DAO rejects an otherwise readable generated file whose slots
are all zero with "Too many active users." A retained experiment changed
only these 256 availability bytes; DAO then opened the file, inserted a row
and compacted it. The even counter bytes and masked header region were left
unchanged. The regression compares the availability flags with
`NativeJet4Schema.mdb`.

Owners and security identities are derived from the file's own header and
MSysDb metadata, rather than copied from another database. Inherited owner
placeholders are resolved to the database owner. When the container also has
an explicit inheritable owner entry, DAO uses that entry's permission mask;
it does not combine the masks. Native identities and oracle details are in
[native-encryption-evidence.md](native-encryption-evidence.md).

Before catalog or relationship mutation, required security objects must have
unique names and IDs, the expected types and nonempty owners. Malformed
inheritance and duplicate inherited identities are refused before allocation
or page changes. These checks preserve existing metadata rather than repairing
incomplete output from earlier library builds.

`Jet4NativeBootstrapTests.FreshJet4_DaoOpensWritesAndCompacts` exercises a new
file with a primary key and foreign key. DAO reads library-written rows,
inserts valid rows, rejects an unmatched parent with error 3201, and compacts
the file. Both DAO and the library reopen the compacted output and verify the
relationship and exact rows. The validation matrix records the tested revision
and host; it does not establish every workgroup or security variant. Jet3
bootstrap and Access97 native validation remain separate requirements.
