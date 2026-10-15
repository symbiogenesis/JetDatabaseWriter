# Format facts and type metadata

`JetFormat` is the immutable profile of an opened or newly created database.
Its layouts own page, TDEF, column-descriptor, row-trailer and index offsets.
The profile also owns header initialization and masking dimensions, text/name
encoding and declared text widths, compressed-Unicode codecs, and row null-bit
operations. `HeaderLayoutOf` exposes the header facts before an encrypted
header can construct a complete profile; it does not initialize text codecs.

`ColumnDescriptorLayout` supplies the native identity, sort order, sort version,
code page and extended-flag offsets as well as the ordinary column fields.
An offset of -1 means the format has no such field. Parsing and authoring use
the same layout. Raw descriptors continue to preserve bytes that these fields
do not model.

`JetTypeInfo` has one immutable `JetTypeMetadata` entry for each known column
code. That table owns intrinsic fixed widths, scalar payload widths in variable
slots, CLR and display mappings, size projection, and physical storage flags.
Text, Binary and BIGBINARY fixed slots use the descriptor width; Boolean uses
the null bit as its value; Memo and OLE use long-value storage. Complex's
four-byte physical reference remains distinct from its logical long-value size
projection. Date/Time Extended's public declared size remains distinct from its
42-byte scalar codec. Calculated result types and envelopes retain their own
validated storage rules.

An undefined discriminator has no table entry. Hexadecimal names, diagnostic
string metadata and retained descriptor bytes do not supply a value codec or
permission to write a row. Native semantics must be established from
Microsoft-authored or independently native-accepted specimens before adding a
new discriminator, decoder or raw value write-back path. Synthetic unknown-code
tests prove conservative refusal and preservation only.

`JetTypeInfoMetadataTests` covers every byte-valued discriminator and the
physical classifications. `JetFormatTests` pins layout/text/header facts and
reads profiles from the existing Microsoft fixtures. Bootstrap and all-type
TDEF golden tests retain their approved bytes and hashes. Native schema,
fixed-field, GUID and encryption regressions supply their separate engine
checks; this organization adds no new format or unknown-type support.
