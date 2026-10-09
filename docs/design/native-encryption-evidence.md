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

These bytes depend on the file. The library derives the masked `0204` owner
placeholder from the header's creation-date/password region, takes the actual
owner from `MSysDb`, and preserves inherited principal bytes and permission
masks. The fixture unmasks Admin to `0301`, Users to `0201`, and the system
owner to `0203`; fixed stored identities from another header are not plaintext
principals.

An independent DAO creation in the Access-authored `testV2003.mdb` fixture
confirms that an explicit inheritable owner SID takes precedence over the owner
placeholder. Its Tables container holds placeholder E4A6 with mask 0xF00FE and
owner E5A3 with mask 0xFFEFF; DAO emits one owner entry with 0xFFEFF. The masks
are not combined. Distinct stored identities may therefore legitimately
converge when the placeholder is resolved.

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
The native maintenance implementation remasks `MSysObjects.Owner` and
`MSysACEs.SID` with the old and new header-derived RC4 keys. The decrypted data
pages match DAO `NewPassword` output byte for byte. Physical index descriptors
are validated before remasking; indexed security identity columns are refused.
The ordinary fixtures do not establish every custom workgroup configuration.

`NativeJet4PasswordChanged.mdb` was produced on 2026-10-08 by DAO120
`NewPassword("Native123", "Changed123")` from `NativeJet4Rc4.mdb`.
SHA-256: `7954A11E0827B410BB7036A9DDB60B5538FEC842A789C0809F47985BCC2E2066`.
Focused DAO tests authenticate, write and compact encrypted, decrypted and
rekeyed output on both target frameworks.

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
of a spin budget below 100000 before data pages are read. Native fixture
maintenance tests also require free-page scrubbing, actual tail shrink and
unchanged page 0. On both target frameworks, DAO120 reads, writes and compacts
the native JET4 RC4 and ACE Agile fixtures after this maintenance. Separate
write, torn-write and flush fault tests restore the original Agile ciphertext
and reuse the writer successfully; these are library rollback checks, not
native crash-recovery evidence. Separate library subprocess tests now verify exact ciphertext restoration after process termination through the persistent sidecar protocol; this does not establish Access-native or power-loss recovery. Other algorithms and physical shrink faults
still need their own coverage.

## File replacement guarantees

The internal replacement helper writes and flushes a unique adjacent temporary
file before renaming it over the destination. Pre-canceled operations stop
before creating the temporary file. A refused rename cleans up only that
operation's file; unrelated temporary files remain untouched. Windows handles
without delete sharing refuse replacement; Unix readers may retain the old
inode while new opens see the replacement. The containing directory is not
flushed, so this does not establish persistence of the new name after power
loss. Native maintenance uses bounded page conversion into the final target
format and retains a delete-shared source handle through replacement. A pending
rollback journal is refused before staging; callers recover it through the writer.
New encrypted database creation transforms the bounded bootstrap in memory
before its first destination write. A JET password-only source stays password-only
after a password change; ACE password changes use fresh AES-256-CBC/SHA-512.

## Evidence limits

The fixtures do not establish the native ACE 50,000-iteration Standard variant,
every Agile algorithm combination, custom workgroup security, directory-entry
power-loss durability or complete hostile-input resistance. Required work remains
in `docs/todo.md` (E1, E2, F3, F4, F6, S1-S2 and I15). Library-generated encryption
round trips alone are not native interoperability oracles.

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
read/write regressions use this fixture. A compatible x86 `DAO.DBEngine.36` host
now verifies password changes, decryption, rekeying, library inserts, native
updates and compact/reopen. The producing `DAO360.dll` reports version
10.0.26100.5074. DAO120 continues to refuse Access 97 files; DAO36 is required.

| DAO36 fixture | Creation | SHA-256 |
|---|---|---|
| NativeJet3Password.mdb | Upstream fixture followed by `NewPassword("", "Native123")` | 66B7F57A8480F66548E7AFB71B5081E41B9A523E65FF9BE7FFB7CD90DACD9F04 |
| NativeJet3Rc4.mdb | DAO36 `CreateDatabase`, options 34, password Native123, then `NewPassword` to CP1252 password Pássword | EFEE0E45A1438E68FCF1BAA18859375DB0E9CCE6ADE79585CAD328276092982E |

The first oracle changes only the password region and a page-zero open counter.
JET3 passwords are encoded using the native database code page; embedded NUL,
unrepresentable text and more than 20 encoded bytes are refused when writing.

## Additional ACE providers

These unchanged files are pinned to the same Jackcess Encrypt revision above.
The original producing host is undocumented. DAO120 independently reads, writes
and compacts each after library page mutation on both target frameworks.

| Fixture | Password | Provider | SHA-256 |
|---|---|---|---|
| Upstream-db2007-oldenc.accdb | Test123 | RC4 CryptoAPI | 174BA7FF6349D4F929ADF555F5035C0722923776662F30A523F2933827D9558E |
| Upstream-db2007-enc.accdb | Test123 | Agile AES-128-CBC/SHA-1 | FA6CD4ED7639DEE40AD288FF6C530356EA050BDE36DBDE5E4328FEE6AA07F306 |
| Upstream-db-nonstandard.accdb | password | Compatibility AES-256/SHA-1, zero iterations | 5E93BAD848E0262EA8952C14DB897D63F8588CA22870043AB7CF954D865B4B39 |
| Upstream-db2013-enc.accdb | 1234 | Agile AES-256-CBC/SHA-512 | 3E9EE219A7CA9BF7DEE261F6455F298887603D41C82EA0C5BC06D8831F09637A |

Native Standard uses AES ECB with a page-specific derived key; its page addressing
differs from Office package encryption. The pinned
`Upstream-OfficeStandard.docx` and its published plaintext validate AES Standard
primitives independently, but an Office package is not a native ACE producer
fixture. Attribution, source revision and hashes are in `THIRD-PARTY-NOTICES.txt`.
The implementation follows [MS-OFFCRYPTO](https://learn.microsoft.com/en-us/openspecs/office_file_formats/ms-offcrypto/).

Agile additionally accepts AES-192, CFB8, SHA-256 and SHA-384, including distinct
password and page algorithms. Independent specification vectors cover those
combinations; they are not advertised as Microsoft-produced fixture evidence.
Password hashing checks cancellation within the loop and consumes an aggregate
reader/linked-source iteration budget before deriving a key. Descriptor tests
refuse duplicate roles, incorrect namespaces and inconsistent algorithm sizes.
