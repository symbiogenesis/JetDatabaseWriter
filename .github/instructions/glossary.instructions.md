---
description: "Glossary of acronyms and abbreviations used throughout this codebase. Reference when encountering unfamiliar terms in source, docs, or comments."
applyTo: "**"
---

# Glossary

## Database Engine & File Format

| Acronym | Meaning |
|---------|---------|
| **JET** | Joint Engine Technology — Microsoft's database engine for Access |
| **Jet3** | JET version 3 — Access 97 format (2048-byte pages, ANSI encoding) |
| **Jet4** | JET version 4 — Access 2000–2003 format (4096-byte pages, UCS-2) |
| **ACE** | Access Connectivity Engine — successor to JET used by Access 2007+ |
| **MDB** | Microsoft Database — file extension for Jet3/Jet4 databases |
| **ACCDB** | Access Database — file extension for ACE-format databases (2007+) |
| **DAO** | Data Access Objects — Microsoft COM-based API for Access |
| **OLE** | Object Linking and Embedding |
| **OLE DB** | OLE Database — COM-based data-access API |

## Page Types & Structures

| Acronym | Meaning |
|---------|---------|
| **TDEF** | Table Definition — page type storing column metadata, index defs, and row counts |
| **LVAL** | Long Value — pages storing MEMO/OLE data that exceeds inline limits |
| **EOD** | End of Data — marker in the variable-length column trailer |
| **PK** | Primary Key |
| **FK** | Foreign Key |
| **DDL** | Data Definition Language |
| **DML** | Data Manipulation Language |

## Compound File Binary (MS-CFB)

| Acronym | Meaning |
|---------|---------|
| **CFB** | Compound File Binary — OLE2 container format used by Office Crypto wrappers and test fixtures; Access-native Agile `.accdb` files use a flat page layout instead |
| **FAT** | File Allocation Table — sector chain mapping in a CFB file |
| **DIFAT** | Double-Indirect FAT — extension array when >109 FAT sectors overflow the header |
| **Mini-FAT** | Mini File Allocation Table — allocation table for streams < 4096 bytes |
| **MSAT** | Master Sector Allocation Table — alternate name for DIFAT |
| **SAT** | Sector Allocation Table — alternate name for FAT |

## Encryption & Cryptography

| Acronym | Meaning |
|---------|---------|
| **RC4** | Rivest Cipher 4 — stream cipher used by Jet4 per-page encryption |
| **AES** | Advanced Encryption Standard — AES-128-ECB (legacy ACCDB) and AES-256-CBC (Agile) |
| **ECB** | Electronic Codebook — AES block mode for legacy encryption |
| **CBC** | Cipher Block Chaining — AES block mode for Agile encryption |
| **XOR** | Exclusive OR — Jet3 page obfuscation (128-byte cyclical mask) |
| **HMAC** | Hash-based Message Authentication Code — integrity check in Agile encryption |
| **SHA** | Secure Hash Algorithm — SHA-256 (legacy AES key), SHA-512 (Agile) |
| **MD5** | Message Digest 5 — Jet4 RC4 per-page key derivation |
| **PBKDF** | Password-Based Key Derivation Function — Agile uses SHA-512 spin loop |
| **IV** | Initialization Vector — per-segment AES-CBC IV |

## Text Encoding

| Acronym | Meaning |
|---------|---------|
| **ANSI** | Jet3 text encoding (code-page–dependent) |
| **UCS-2** | Universal Character Set (2-byte) — Jet4/ACE text encoding |
| **UTF-16** | Unicode Transformation Format (16-bit) — password and column-name storage |

## Data Types & Column Constants

| Acronym | Meaning |
|---------|---------|
| **GUID** | Globally Unique Identifier — column type 0x0F |
| **BLOB** | Binary Large Object — large binary data stored via LVAL |
| **MEMO** | Memo field — column type 0x0C; long text |
| **BCD** | Binary-Coded Decimal — format for `T_NUMERIC` (0x10) columns |
| **T_BOOL** | Type Boolean (0x01) |
| **T_BINARY** | Type Binary (0x09) |
| **T_TEXT** | Type Text (0x0A) |
| **T_NUMERIC** | Type Numeric/BCD (0x10) |
| **T_COMPLEX** | Type Complex (0x12) — multi-value/attachment |
| **T_DATETIMEEXT** | Type DateTime Extended (0x14) — Access 2019+ high-precision |

## Internal Structures

| Acronym | Meaning |
|---------|---------|
| **LRU** | Least Recently Used — 256-page eviction cache in `AccessReader` |
| **B-tree** | Balanced tree — index page structure (leaf 0x04, intermediate 0x03) |

## System Tables (MSys*)

| Acronym | Meaning |
|---------|---------|
| **MSysObjects** | Catalog table listing all database objects |
| **MSysACEs** | Access Control Entries (security) |
| **MSysRelationships** | Foreign key relationship definitions |
| **MSysComplexColumns** | Links complex columns to their template tables |
| **MSysIndexes** | Index definitions |

## Standards & Specifications

| Acronym | Meaning |
|---------|---------|
| **ECMA-376** | Office Open XML standard — defines Agile encryption |
| **MS-CFB** | Microsoft Compound File Binary Format specification |
| **MS-OFFCRYPTO** | Microsoft Office Document Cryptography Structure specification |
| **CVE** | Common Vulnerabilities and Exposures |
| **OOB** | Out of Bounds — memory access vulnerability class |
