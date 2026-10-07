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

The fixtures do not establish JET3 encryption, RC4 CryptoAPI or Standard ACE
providers, every Agile algorithm combination, workgroup security, native
password maintenance, encrypted creation, crash recovery or complete hostile
input resistance. Required work remains in `docs/todo.md` (E1, E2, F3-F6,
S1-S2 and I4). The older library-generated encryption files are not native
interoperability oracles.
