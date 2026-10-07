# Native encryption evidence

The following small fixtures were created by Microsoft's `DAO.DBEngine.120` on
2026-10-06, using the x64 COM server `ACEDAO.DLL` version 16.0.20430.20092.
They contain only synthetic test data: table `T`, `Id = 7`, and
`Label = "Native encrypted row"`. Their test password is `Native123`.
The files are under `JetDatabaseWriter.Tests/Databases/Encrypted`.

| File | Bytes | SHA-256 | Creation options |
|---|---:|---|---|
| NativeJet4Password.mdb | 90112 | BE37E9AB6D80A1A1C2F796F1DD0039DF3516A50342938ECC431F0EE51D815CB8 | dbVersion40 (64), password, no dbEncrypt |
| NativeJet4Rc4.mdb | 90112 | B164F5B845BA9CA342AB85EE50B89831FA612D9E22C2D3DDB968CE08097207BF | dbVersion40 (64) plus dbEncrypt (2), password |
| NativeAceAgile.accdb | 184320 | 2CB842ECF476D360233303FE7148C7964DC47D076A799D1F120587CD7CE47FCA | dbVersion120 (128) plus dbEncrypt (2), password |

The creation locale was `;LANGID=0x0409;CP=1252;COUNTRY=0;pwd=Native123`.
This records the installed producer, not a claim that this engine represents
all historical Microsoft providers.

## JET4 schema oracle

`NativeJet4Schema.mdb` is the JET4 RC4 fixture above with an additional empty
`Added` table (`Id LONG`, primary key), created by the same DAO engine.
SHA-256: `F6C3B19F5C73C4D0E8E99A598FE8100489E6A517A7EB682EDAE9AE28F71EA4E7`.
DAO reopened both original `T` and new `Added` successfully. The fixture
pins native catalog ownership and ACL rows; library-generated schema
compatibility remains subject to the separate DAO regression.

The native schema oracle stores `MSysDb.Owner = FA7B`. Its Tables container
has an inheritable owner-placeholder SID `FB7E` with mask `0xF00FE`, and an
inheritable Users SID `FB7B` with mask `0xFFEFF`. DAO gives a new table owner
`FA7B` and those inherited permissions, substituting the real owner for the
placeholder and clearing `FInheritable` on the new rows. The system container's
own owner is different and must not be copied as the table owner.

These bytes depend on the file. The library derives the masked `7015` owner
placeholder from the header's creation-date/password region, takes the actual
owner from `MSysDb`, and preserves inherited principal bytes and permission
masks. It does not replace them with fixed identities from a bootstrap file.
The regression pins the fixture's placeholder and checks that deriving it does
not modify page zero. Workgroup variants still require separate native evidence.

## JET4

The password-only file has a zero unmasked encoding key; encrypted pages use
an independent four-byte encoding key. The per-page RC4 key is that encoding
key XOR the little-endian page number. The UTF-16 header password is masked by
the creation-date value. Raw byte 0x62 was 233 in both native files and is not
a standalone password/encryption flag.

Preserving the original key and password permits page updates. Changing the
header password alone was insufficient: native `NewPassword` also changed
security metadata on pages 17 and 18 in the characterization database.
Encryption, decryption and password maintenance therefore require separate
native metadata work; refusal before mutation is safer than emitting a file
DAO cannot authenticate.

## ACE Agile

The native file is a flat `Standard ACE DB` database, not an Office compound
package. Page 0 contains a 1055-byte version 4.4 descriptor with flags 0x40,
AES-256-CBC, SHA-512 and spin count 100000. The descriptor has no
`dataIntegrity` element. These are observations of this fixture, not universal
requirements for all valid ACE files.

The page codec unlocks the provider key once, leaves page 0 unchanged, and
uses the native encoding key and salt when deriving each data page's IV.
Reader and writer use normal page I/O and ownership. Regression tests cover
native fixture reading, original-key mutation, wrong passwords and refusal
of a spin budget below 100000 before data pages are read. DAO validation of
library-mutated files must be recorded separately from library readback.

## Evidence limits

The DAO fixtures do not establish native JET3 producer interoperability, RC4 CryptoAPI or Standard ACE
providers, every Agile algorithm combination, workgroup security, native
password maintenance, encrypted creation, crash recovery or complete hostile
input resistance. Required work remains in `docs/todo.md` (E1, E2, F3, F4, F6,
S1-S2 and I4). The older library-generated encryption files are not native
interoperability oracles.

## Upstream JET3 oracle

`UpstreamJet3Rc4.mdb` is an unmodified 90112-byte fixture pinned to
jackcessencrypt commit `77a1d54db8fc71606f391d94adacae7e34b108ca`.
SHA-256: `8939D5F541A69B5A195AAB512D65C88A4A2CA1FBA43D19C78CD3E2F673503AF9`.
The adjacent attribution records its source URL and Apache 2.0 license.
Upstream classifies it as Access 97 and verifies Table1 rows
`(1, hello, 0)` and `(2, world, 42)` without a password.
The original producing Microsoft host is undocumented; it is an independent
format oracle, not a locally DAO-authored fixture.

Its unmasked encoding key is `A7E0C0FE`; its password region is empty.
The native per-page RC4 rule decodes it. Raw header byte 0x62 is 0x34,
and its unmasked value is zero; it is not an encryption flag. Library
read/write regressions use this fixture, while a native Access 97 mutation
and compact check still requires a compatible engine.
