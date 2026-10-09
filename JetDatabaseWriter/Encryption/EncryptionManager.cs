namespace JetDatabaseWriter.Encryption;

using System;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.CompoundFile;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Infrastructure;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Centralizes all JET / ACE / ACCDB encryption logic — header detection,
/// password verification, key derivation, and per-page decryption.
/// </summary>
internal static class EncryptionManager
{
    /// <summary>Where a reader's password comes from, as a missing-password message names it.</summary>
    internal const string ReaderPasswordOption = nameof(AccessReaderOptions) + "." + nameof(AccessReaderOptions.Password);

    /// <summary>Where a writer's password comes from, as a missing-password message names it.</summary>
    internal const string WriterPasswordOption = nameof(AccessWriterOptions) + "." + nameof(AccessWriterOptions.Password);

    /// <summary>Where <c>ChangePasswordAsync</c> and <c>DecryptAsync</c> take the current password from, as a missing-password message names it.</summary>
    internal const string OldPasswordArgument = "the oldPassword argument";

    /// <summary>Names the retained replacement staging file in a failed commit exception.</summary>
    internal const string ReplacementFileDataKey = "JetDatabaseWriter.ReplacementFile";

    /// <summary>Returns true when the file begins with the OLE2 Compound File Binary magic bytes.</summary>
    /// <param name="header">The header.</param>
    public static bool IsCompoundFileEncrypted(byte[] header) =>
        header is not null && CompoundFileReader.HasCompoundFileMagic(header);

    private const int HeaderPasswordLength = 40;

    private const int HeaderPasswordLengthPrefixLength = 4;

    private const int HeaderPasswordCharSize = 2;

    private const int HeaderPasswordNormalizedLength = HeaderPasswordLengthPrefixLength + HeaderPasswordLength;

    /// <summary>
    /// Inspects the database header for Jet3 / Jet4 / ACCDB page-encryption or
    /// password flags, verifies the supplied password where required, and returns
    /// the owned page-decryption keys for the database header and password context.
    /// </summary>
    /// <param name="header">The database header bytes.</param>
    /// <param name="format">The format.</param>
    /// <param name="password">The password.</param>
    /// <param name="passwordOptionName">Where the caller supplies the password, named by a missing-password message (<see cref="ReaderPasswordOption"/>, <see cref="WriterPasswordOption"/>).</param>
    /// <exception cref="UnauthorizedAccessException">Thrown when the database requires a password and the supplied password is missing or incorrect.</exception>
    internal static IPageCodec OpenPageCodec(
        byte[] header,
        DatabaseFormat format,
        ReadOnlyMemory<char> password,
        string passwordOptionName)
    {
        uint? rc4DbKey = null;

        if (format is DatabaseFormat.Jet3Mdb or DatabaseFormat.Jet4Mdb)
        {
            if (HasHeaderPassword(header, format))
            {
                if (password.IsEmpty || !(format == DatabaseFormat.Jet3Mdb ? NativeJet3PasswordMatches(header, password.Span) : NativeJet4PasswordMatches(header, password.Span)))
                {
                    throw new UnauthorizedAccessException(
                        $"This database requires its password via {passwordOptionName}; the supplied password is missing or incorrect.");
                }
            }

            byte[] unmasked = (byte[])header.Clone();
            TransformHeaderMask(unmasked, format);
            uint encodingKey = Ru32(unmasked, Constants.DatabaseHeader.EncodingKey);
            CryptographicOperations.ZeroMemory(unmasked);
            if (encodingKey != 0)
            {
                rc4DbKey = encodingKey;
            }
        }

        if (rc4DbKey.HasValue)
        {
            return new Jet4Rc4PageCodec(rc4DbKey.Value);
        }

        return new NoPageCodec();
    }

    /// <summary>
    /// Constant RC4 key Microsoft Access applies to header bytes [0x18 .. 0x18+126]
    /// (Jet3) or [0x18 .. 0x18+128] (Jet4/ACE) at file write time. The same key
    /// unscrambles the bytes again at read time. mdbtools applies it
    /// unconditionally in mdb_handle_from_stream (src/libmdb/file.c).
    /// </summary>
    private static readonly byte[] HeaderRc4Key = [0xC7, 0xDA, 0x39, 0x6B];

    /// <summary>
    /// Reads the database codepage from a raw, freshly-loaded page-0 header.
    /// The codepage word at offset 0x3C is scrambled by the constant-key RC4
    /// stream Microsoft Access applies to the header, so this helper
    /// descrambles a local copy of the relevant byte range before returning the
    /// codepage value.
    /// Returns 0 when the header does not carry a recognizable codepage, in
    /// which case callers should fall back to a sensible default (1252).
    /// </summary>
    /// <param name="hdr">The database header bytes.</param>
    /// <param name="format">The format.</param>
    public static int DecodeHeaderCodePage(byte[] hdr, DatabaseFormat format)
    {
        if (hdr is null || hdr.Length < 0x3E)
        {
            return 0;
        }

        int rc4Length = format == DatabaseFormat.Jet3Mdb ? 126 : 128;
        if (hdr.Length < 0x18 + rc4Length)
        {
            rc4Length = hdr.Length - 0x18;
        }

        byte[] copy = new byte[rc4Length];
        Buffer.BlockCopy(hdr, 0x18, copy, 0, rc4Length);
        Rc4Transform(copy, 0, rc4Length, HeaderRc4Key);

        // Codepage lives at hdr[0x3C..0x3D]; in the descrambled copy that is at
        // offset 0x3C - 0x18 = 0x24.
        const int codePageOffsetInCopy = 0x3C - 0x18;
        if (copy.Length < codePageOffsetInCopy + 2)
        {
            return 0;
        }

        return Ru16(copy, codePageOffsetInCopy);
    }

    /// <summary>
    /// Applies or removes the fixed RC4 mask Access uses for Jet4 / ACE
    /// page-0 header bytes <c>0x18..0x97</c>. The transform is symmetric, and
    /// on bytes <c>0x18..0x95</c> it matches the 126-byte Jet3 mask, so an
    /// unmask-edit-mask round trip of those bytes is right on Jet3 too.
    /// </summary>
    /// <param name="headerPage">The header page.</param>
    internal static void TransformHeaderMask(byte[] headerPage) => TransformHeaderMask(headerPage, DatabaseFormat.AceAccdb);

    /// <summary>
    /// Applies or removes the fixed RC4 mask Access uses for the page-0
    /// header of <paramref name="format"/>: bytes <c>0x18..0x95</c> on Jet3,
    /// <c>0x18..0x97</c> on Jet4 / ACE. The transform is symmetric.
    /// </summary>
    /// <param name="headerPage">The header page.</param>
    /// <param name="format">The database format.</param>
    internal static void TransformHeaderMask(byte[] headerPage, DatabaseFormat format)
    {
        Guard.NotNull(headerPage, nameof(headerPage));
        int maskLength = format == DatabaseFormat.Jet3Mdb ? Constants.DatabaseHeader.Jet3MaskLength : Constants.DatabaseHeader.MaskLength;
        int length = Math.Min(maskLength, headerPage.Length - Constants.DatabaseHeader.MaskStart);
        if (length > 0)
        {
            Rc4Transform(headerPage, Constants.DatabaseHeader.MaskStart, length, HeaderRc4Key);
        }
    }

    /// <summary>
    /// <para>
    /// Returns <see langword="true"/> when the page-0 header password area
    /// holds a password. The area lies in the masked header region. On a
    /// database without a password Access fills it with zeros (Jet3) or with
    /// the whole days of the creation date at <c>0x72</c>, as a little-endian
    /// <see cref="int"/>, repeated (Jet4 / ACE); anything else is a password.
    /// </para>
    /// <para>
    /// Raw header byte <c>0x62</c> is byte 32 of the Jet4 / ACE
    /// area. On a file without a password it is a creation-date byte, so its
    /// raw value says nothing unless this returns <see langword="true"/>.
    /// </para>
    /// </summary>
    /// <param name="header">Raw page-0 bytes: at least the password area and, on Jet4 / ACE, the creation date (0x7A bytes).</param>
    /// <param name="format">The database format.</param>
    /// <returns><see langword="true"/> when the password area holds a password; <see langword="false"/> when it holds Access's empty-password pattern or the header is too short to hold the area.</returns>
    internal static bool HasHeaderPassword(byte[] header, DatabaseFormat format)
    {
        bool jet3 = format == DatabaseFormat.Jet3Mdb;
        int passwordLength = jet3 ? Constants.DatabaseHeader.Jet3PasswordLength : Constants.DatabaseHeader.PasswordLength;
        int length = jet3
            ? Constants.DatabaseHeader.Password + passwordLength
            : Constants.DatabaseHeader.CreationDate + sizeof(double);
        if (header is null || header.Length < length)
        {
            return false;
        }

        byte[] unmasked = new byte[length];
        Buffer.BlockCopy(header, 0, unmasked, 0, length);
        Rc4Transform(unmasked, Constants.DatabaseHeader.MaskStart, length - Constants.DatabaseHeader.MaskStart, HeaderRc4Key);

        ReadOnlySpan<byte> area = unmasked.AsSpan(Constants.DatabaseHeader.Password, passwordLength);
        bool allZero = true;
        foreach (byte b in area)
        {
            allZero &= b == 0;
        }

        if (allZero)
        {
            return false;
        }

        Span<byte> pattern = stackalloc byte[4];
        if (jet3 || !TryGetEmptyPasswordPattern(unmasked, pattern))
        {
            return true;
        }

        for (int i = 0; i < area.Length; i++)
        {
            if (area[i] != pattern[i % pattern.Length])
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Writes the password area of a database without a password into an
    /// unmasked page-0 header, as Access writes it: zeros on Jet3, and on
    /// Jet4 / ACE the creation date's whole days repeated. A creation date
    /// that is not a finite day number leaves zeros, which
    /// <see cref="HasHeaderPassword"/> also reads as no password.
    /// </summary>
    /// <param name="unmaskedHeader">Page-0 bytes with the header mask removed: at least the password area and, on Jet4 / ACE, the creation date.</param>
    /// <param name="format">The database format.</param>
    internal static void WriteEmptyHeaderPassword(Span<byte> unmaskedHeader, DatabaseFormat format)
    {
        bool jet3 = format == DatabaseFormat.Jet3Mdb;
        Span<byte> area = unmaskedHeader.Slice(
            Constants.DatabaseHeader.Password,
            jet3 ? Constants.DatabaseHeader.Jet3PasswordLength : Constants.DatabaseHeader.PasswordLength);

        Span<byte> pattern = stackalloc byte[4];
        if (jet3 || !TryGetEmptyPasswordPattern(unmaskedHeader, pattern))
        {
            area.Clear();
            return;
        }

        for (int i = 0; i < area.Length; i++)
        {
            area[i] = pattern[i % pattern.Length];
        }
    }

    /// <summary>
    /// Builds the 4-byte Jet4 / ACE empty-password pattern: the whole days of
    /// the creation date at <c>0x72</c> (truncated toward zero, as Access
    /// does) as a little-endian <see cref="int"/>.
    /// </summary>
    /// <param name="unmaskedHeader">Unmasked page-0 bytes holding the creation date.</param>
    /// <param name="pattern">Receives the 4-byte pattern.</param>
    /// <returns><see langword="false"/> when the creation date is not a finite number in <see cref="int"/> range.</returns>
    private static bool TryGetEmptyPasswordPattern(ReadOnlySpan<byte> unmaskedHeader, Span<byte> pattern)
    {
        double creationDate = ReadDoubleLittleEndian(unmaskedHeader.Slice(Constants.DatabaseHeader.CreationDate, sizeof(double)));
        if (!double.IsFinite(creationDate) || creationDate < int.MinValue || creationDate > int.MaxValue)
        {
            return false;
        }

        Wi32(pattern, 0, (int)creationDate);
        return true;
    }

    /// <summary>
    /// Detects the on-disk encryption format of the database at
    /// <paramref name="path"/>. Returns <see cref="AccessEncryptionFormat.None"/>
    /// when the file is unencrypted. The file is read but not modified.
    /// </summary>
    /// <param name="path">Path to the .mdb or .accdb file.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A <see cref="ValueTask{TResult}"/> yielding the detected format.</returns>
    public static async ValueTask<AccessEncryptionFormat> DetectEncryptionFormatAsync(
        string path,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        Guard.RequireExistingDatabaseFile(path, nameof(path));

        await using FileStream fs = FileStreamFactory.Open(
            path,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read,
            FileOptions.Asynchronous);
        return await DetectEncryptionFormatAsync(fs, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Detects the on-disk encryption format of the database in <paramref name="stream"/>
    /// without modifying it. The stream must be seekable.
    /// </summary>
    /// <param name="stream">A readable, seekable stream containing the database bytes.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A <see cref="ValueTask{TResult}"/> yielding the detected format.</returns>
    public static async ValueTask<AccessEncryptionFormat> DetectEncryptionFormatAsync(
        Stream stream,
        CancellationToken cancellationToken = default)
    {
        Guard.RequireReadableSeekableStream(stream, nameof(stream));
        cancellationToken.ThrowIfCancellationRequested();

        long origin = stream.Position;
        try
        {
            byte[] sniff = await ReadHeaderPageAsync(stream, cancellationToken).ConfigureAwait(false);

            return EncryptionConverter.Detect(sniff);
        }
        finally
        {
            _ = stream.Seek(origin, SeekOrigin.Begin);
        }
    }

    /// <summary>
    /// Reads page 0 for an open: the 4096-byte Jet4 / ACE header page from
    /// the start of <paramref name="stream"/>, zero-filled past the end of a
    /// shorter file. One read serves both the <see cref="Constants.DatabaseHeader.Length"/>-byte
    /// header and the flat Agile probe, whose descriptor lives in page 0.
    /// </summary>
    /// <param name="stream">A readable, seekable stream containing the database bytes.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A <see cref="ValueTask{TResult}"/> yielding the header page.</returns>
    /// <exception cref="EndOfStreamException">Thrown when the stream holds fewer than <see cref="Constants.DatabaseHeader.Length"/> bytes.</exception>
    internal static async ValueTask<byte[]> ReadOpenHeaderPageAsync(Stream stream, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _ = stream.Seek(0, SeekOrigin.Begin);
        byte[] headerPage = new byte[Constants.PageSizes.Jet4];
        int read = await stream.ReadAtLeastAsync(
            headerPage.AsMemory(),
            headerPage.Length,
            throwOnEndOfStream: false,
            cancellationToken: cancellationToken).ConfigureAwait(false);
        if (read < Constants.DatabaseHeader.Length)
        {
            throw new EndOfStreamException(
                $"The database is {read} bytes long, shorter than the {Constants.DatabaseHeader.Length}-byte JET header.");
        }

        return headerPage;
    }

    /// <summary>
    /// Reads the 4096-byte Jet4 / ACE header page (page 0) from the start of
    /// <paramref name="stream"/>. A shorter file leaves the tail zero-filled.
    /// Jet3 pages are 2048 bytes, so for Jet3 this also covers page 1; no
    /// caller looks past the Jet3 header.
    /// </summary>
    /// <param name="stream">A readable, seekable stream containing the database bytes.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private static async ValueTask<byte[]> ReadHeaderPageAsync(Stream stream, CancellationToken cancellationToken)
    {
        _ = stream.Seek(0, SeekOrigin.Begin);
        byte[] headerPage = new byte[Constants.PageSizes.Jet4];
        _ = await stream.ReadAtLeastAsync(
            headerPage.AsMemory(),
            headerPage.Length,
            throwOnEndOfStream: false,
            cancellationToken: cancellationToken).ConfigureAwait(false);
        return headerPage;
    }

    /// <summary>Flushes staging contents, then commits an atomic replacement with a recoverable original.</summary>
    /// <param name="path">The destination file.</param>
    /// <param name="contents">The replacement contents.</param>
    /// <param name="cancellationToken">Cancellation is honored until the namespace commit begins.</param>
    /// <returns>The asynchronous replacement operation.</returns>
    /// <exception cref="IOException">The commit failed; retained original and staged paths are supplied in the exception data.</exception>
    internal static ValueTask ReplaceFileAtomicAsync(string path, ReadOnlyMemory<byte> contents, CancellationToken cancellationToken)
        => EncryptionFileReplacement.ReplaceAsync(
            path,
            (stream, token) => stream.WriteAsync(contents, token),
            cancellationToken);

    /// <summary>Commits an adjacent flushed staging file, retaining recoverable copies on failure.</summary>
    /// <param name="tempPath">The flushed staging file.</param>
    /// <param name="path">The destination file.</param>
    /// <exception cref="IOException">The commit failed; retained paths are supplied in the exception data.</exception>
    internal static void ReplaceFileWithTemp(string tempPath, string path)
        => EncryptionReplacementCommit.Commit(tempPath, path);

    // ── Crypto primitives ────────────────────────────────────────────

    /// <summary>Combines the native Jet encoding key with the little-endian page number using XOR.</summary>
    /// <param name="dbKey">The unmasked database encoding key.</param>
    /// <param name="pageNumber">The page number.</param>
    /// <param name="destination">The four-byte RC4 key destination.</param>
    internal static void DeriveRc4PageKey(uint dbKey, uint pageNumber, Span<byte> destination)
        => Wu32(destination, 0, dbKey ^ pageNumber);

    /// <summary>In-place RC4 transform (encrypt and decrypt are the same operation).</summary>
    /// <param name="data">The data bytes or values.</param>
    /// <param name="offset">The offset.</param>
    /// <param name="length">The length.</param>
    /// <param name="key">The key bytes or index key.</param>
    internal static void Rc4Transform(byte[] data, int offset, int length, ReadOnlySpan<byte> key)
    {
        Span<byte> s = stackalloc byte[256];
        try
        {
            for (int i = 0; i < 256; i++)
            {
                s[i] = (byte)i;
            }

            int j = 0;
            for (int i = 0; i < 256; i++)
            {
                j = (j + s[i] + key[i % key.Length]) & 0xFF;
                (s[i], s[j]) = (s[j], s[i]);
            }

            int x = 0, y = 0;
            for (int k = 0; k < length; k++)
            {
                x = (x + 1) & 0xFF;
                y = (y + s[x]) & 0xFF;
                (s[x], s[y]) = (s[y], s[x]);
                data[offset + k] ^= s[(s[x] + s[y]) & 0xFF];
            }
        }
        finally
        {
            CryptographicOperations.ZeroMemory(s);
        }
    }

    /// <summary>Writes a native Jet4 encoding key and date-masked UTF-16 password into the raw header.</summary>
    /// <param name="header">The raw page-zero header.</param>
    /// <param name="encodingKey">The native database encoding key.</param>
    /// <param name="password">The database password.</param>
    /// <exception cref="JetLimitationException">The password exceeds the native twenty-character field.</exception>
    /// <exception cref="ArgumentException">The password contains a null character.</exception>
    internal static void WriteNativeJet4EncryptionHeader(byte[] header, uint encodingKey, ReadOnlySpan<char> password)
    {
        if (password.Length > HeaderPasswordLength / sizeof(char))
        {
            throw new JetLimitationException("Jet4 passwords cannot exceed twenty UTF-16 characters.");
        }

        if (password.IndexOf('\0') >= 0)
        {
            throw new ArgumentException("Native database passwords cannot contain null characters.", nameof(password));
        }

        TransformHeaderMask(header);
        try
        {
            Wu32(header, Constants.DatabaseHeader.EncodingKey, encodingKey);
            WriteEmptyHeaderPassword(header, DatabaseFormat.Jet4Mdb);
            Span<byte> bytes = stackalloc byte[HeaderPasswordLength];
            bytes.Clear();
            _ = Encoding.Unicode.GetBytes(password, bytes);
            for (int i = 0; i < bytes.Length; i++)
            {
                header[Constants.DatabaseHeader.Password + i] ^= bytes[i];
            }

            CryptographicOperations.ZeroMemory(bytes);
        }
        finally
        {
            TransformHeaderMask(header);
        }
    }

    /// <summary>Writes the native Jet3 encoding key and code-page password field.</summary>
    /// <param name="header">The raw header.</param>
    /// <param name="encodingKey">The page encoding key.</param>
    /// <param name="password">The database password.</param>
    /// <exception cref="ArgumentException">The password contains a null character or cannot be encoded in the database code page.</exception>
    /// <exception cref="JetLimitationException">The encoded password exceeds twenty bytes.</exception>
    internal static void WriteNativeJet3EncryptionHeader(byte[] header, uint encodingKey, ReadOnlySpan<char> password)
    {
        if (password.IndexOf('\0') >= 0)
        {
            throw new ArgumentException("Native database passwords cannot contain null characters.", nameof(password));
        }

        if (password.Length > Constants.DatabaseHeader.Jet3PasswordLength)
        {
            throw new JetLimitationException("Jet3 passwords cannot exceed twenty bytes in the database code page.");
        }

        var encoding = (Encoding)JetFormat.FromHeader(header).AnsiEncoding.Clone();
        encoding.EncoderFallback = EncoderFallback.ExceptionFallback;
        byte[] bytes = new byte[encoding.GetByteCount(password)];
        _ = encoding.GetBytes(password, bytes);
        try
        {
            if (bytes.Length > Constants.DatabaseHeader.Jet3PasswordLength)
            {
                throw new JetLimitationException("Jet3 passwords cannot exceed twenty bytes in the database code page.");
            }

            TransformHeaderMask(header, DatabaseFormat.Jet3Mdb);
            try
            {
                Wu32(header, Constants.DatabaseHeader.EncodingKey, encodingKey);
                header.AsSpan(Constants.DatabaseHeader.Password, Constants.DatabaseHeader.Jet3PasswordLength).Clear();
                bytes.CopyTo(header, Constants.DatabaseHeader.Password);
            }
            finally
            {
                TransformHeaderMask(header, DatabaseFormat.Jet3Mdb);
            }
        }
        finally
        {
            CryptographicOperations.ZeroMemory(bytes);
        }
    }

    /// <summary>Verifies the native Jet3 single-byte password field independently of page encryption.</summary>
    /// <param name="header">The raw header.</param>
    /// <param name="password">The supplied password.</param>
    private static bool NativeJet3PasswordMatches(byte[] header, ReadOnlySpan<char> password)
    {
        if (password.Length > Constants.DatabaseHeader.Jet3PasswordLength || password.IndexOf('\0') >= 0)
        {
            return false;
        }

        byte[] unmasked = (byte[])header.Clone();
        byte[]? supplied = null;
        char[] passwordCharacters = password.ToArray();
        try
        {
            TransformHeaderMask(unmasked, DatabaseFormat.Jet3Mdb);
            var encoding = (Encoding)JetFormat.FromHeader(header).AnsiEncoding.Clone();
            encoding.EncoderFallback = EncoderFallback.ExceptionFallback;
            supplied = encoding.GetBytes(passwordCharacters);
            if (supplied.Length > Constants.DatabaseHeader.Jet3PasswordLength)
            {
                return false;
            }

            Span<byte> padded = stackalloc byte[Constants.DatabaseHeader.Jet3PasswordLength];
            padded.Clear();
            supplied.CopyTo(padded);
            bool matches = CryptographicOperations.FixedTimeEquals(unmasked.AsSpan(Constants.DatabaseHeader.Password, padded.Length), padded);
            CryptographicOperations.ZeroMemory(padded);
            return matches;
        }
        catch (EncoderFallbackException)
        {
            return false;
        }
        finally
        {
            CryptographicOperations.ZeroMemory(unmasked);
            OfficeCryptoPrimitives.ZeroIfNotNull(supplied);
            Array.Clear(passwordCharacters, 0, passwordCharacters.Length);
        }
    }

    private static bool NativeJet4PasswordMatches(byte[] header, ReadOnlySpan<char> password)
    {
        byte[] unmasked = (byte[])header.Clone();
        Span<byte> stored = stackalloc byte[HeaderPasswordNormalizedLength];
        Span<byte> supplied = stackalloc byte[HeaderPasswordNormalizedLength];
        Span<byte> pattern = stackalloc byte[4];
        try
        {
            TransformHeaderMask(unmasked);
            _ = TryGetEmptyPasswordPattern(unmasked, pattern);
            stored.Clear();
            Span<byte> bytes = stored.Slice(HeaderPasswordLengthPrefixLength, HeaderPasswordLength);
            for (int i = 0; i < bytes.Length; i++)
            {
                bytes[i] = (byte)(unmasked[Constants.DatabaseHeader.Password + i] ^ pattern[i % pattern.Length]);
            }

            int length = bytes.Length;
            for (int i = 0; i < bytes.Length; i += sizeof(char))
            {
                if (bytes[i] == 0 && bytes[i + 1] == 0)
                {
                    length = i;
                    bytes[i..].Clear();
                    break;
                }
            }

            Wu32(stored, 0, (uint)length);
            NormalizeSuppliedHeaderPassword(password, supplied);
            return CryptographicOperations.FixedTimeEquals(stored, supplied);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(unmasked);
            CryptographicOperations.ZeroMemory(stored);
            CryptographicOperations.ZeroMemory(supplied);
        }
    }

    private static void NormalizeSuppliedHeaderPassword(ReadOnlySpan<char> password, Span<byte> destination)
    {
        destination.Clear();
        Span<byte> passwordBytes = destination.Slice(HeaderPasswordLengthPrefixLength, HeaderPasswordLength);
        const int maxPasswordChars = HeaderPasswordLength / HeaderPasswordCharSize;
        int charsToEncode = Math.Min(password.Length, maxPasswordChars);

        if (charsToEncode > 0)
        {
            _ = Encoding.Unicode.GetBytes(password[..charsToEncode], passwordBytes);
        }

        uint passwordByteLength = password.Length <= maxPasswordChars
            ? (uint)(password.Length * HeaderPasswordCharSize)
            : uint.MaxValue;
        Wu32(destination, 0, passwordByteLength);
    }
}
