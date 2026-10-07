namespace JetDatabaseWriter.Encryption;

using System;
using System.IO;
#if !NET6_0_OR_GREATER
using System.Runtime.InteropServices;
#endif
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.CompoundFile;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Transactions;
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

    /// <summary>
    /// The <see cref="Exception.Data"/> key under which an
    /// <see cref="IOException"/> from <see cref="ReplaceFileWithTemp"/> (and so
    /// from <see cref="ReplaceFileAtomicAsync"/>) carries the temp file's path
    /// when the original file was removed and the temp file, which then holds
    /// the only copy of the new contents, could not be renamed to its name.
    /// Every other failure leaves the original unchanged and has no entry.
    /// </summary>
    internal const string ReplacementFileDataKey = "JetDatabaseWriter.ReplacementFile";

    /// <summary>Returns true when the file begins with the OLE2 Compound File Binary magic bytes.</summary>
    /// <param name="header">The header.</param>
    public static bool IsCompoundFileEncrypted(byte[] header) =>
        header is not null && CompoundFileReader.HasCompoundFileMagic(header);

    private const int HeaderPasswordLength = 40;

    private const int HeaderPasswordLengthPrefixLength = 4;

    private const int HeaderPasswordCharSize = 2;

    private const int HeaderPasswordNormalizedLength = HeaderPasswordLengthPrefixLength + HeaderPasswordLength;

    /// <summary>The <see cref="Exception.HResult"/> of Windows' <c>ERROR_SHARING_VIOLATION</c>, which <see cref="File.Replace(string, string, string?, bool)"/> reports while a handle holds the file without <see cref="FileShare.Delete"/>.</summary>
    private const int SharingViolationHResult = unchecked((int)0x80070020);

    /// <summary>The <see cref="Exception.HResult"/> of Windows' <c>ERROR_UNABLE_TO_REMOVE_REPLACED</c>: the file to be replaced could not be removed, and both files keep their names.</summary>
    private const int UnableToRemoveReplacedHResult = unchecked((int)0x80070497);

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

    /// <summary>
    /// Changes the password of an already-encrypted JET / ACE database,
    /// preserving the existing on-disk encryption format. The re-encrypted
    /// file replaces the original through <see cref="ReplaceFileAtomicAsync"/>,
    /// a temp file renamed over it, never an overwrite in place.
    /// </summary>
    /// <param name="path">Path to the file.</param>
    /// <param name="oldPassword">The old password.</param>
    /// <param name="newPassword">The new password.</param>
    /// <param name="options">The options.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="IOException">Thrown when the file cannot be replaced, for example while an <see cref="AccessReader"/> holds it open; see <see cref="ReplaceFileAtomicAsync"/>.</exception>
    public static ValueTask ChangePasswordAsync(
        string path,
        ReadOnlyMemory<char> oldPassword,
        ReadOnlyMemory<char> newPassword,
        AccessWriterOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        Guard.NotNullOrEmpty(path, nameof(path));
        Guard.NotEmpty(newPassword, nameof(newPassword));
        return ReencryptFileAsync(
            path,
            oldPassword,
            newPassword,
            targetFormat: null,
            requireSourceEncrypted: true,
            options,
            cancellationToken);
    }

    /// <summary>
    /// Encrypts a currently-unencrypted JET / ACE database, applying the
    /// requested <paramref name="targetFormat"/>. The encrypted file replaces
    /// the original through <see cref="ReplaceFileAtomicAsync"/>, a temp file
    /// renamed over it, never an overwrite in place.
    /// </summary>
    /// <param name="path">Path to the file.</param>
    /// <param name="newPassword">The new password.</param>
    /// <param name="targetFormat">The target format.</param>
    /// <param name="options">The options.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="ArgumentException">Thrown when <paramref name="targetFormat"/> is <see cref="AccessEncryptionFormat.None"/>.</exception>
    /// <exception cref="IOException">Thrown when the file cannot be replaced, for example while an <see cref="AccessReader"/> holds it open; see <see cref="ReplaceFileAtomicAsync"/>.</exception>
    public static ValueTask EncryptAsync(
        string path,
        ReadOnlyMemory<char> newPassword,
        AccessEncryptionFormat? targetFormat = null,
        AccessWriterOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        Guard.NotNullOrEmpty(path, nameof(path));
        Guard.NotEmpty(newPassword, nameof(newPassword));
        if (targetFormat == AccessEncryptionFormat.None)
        {
            throw new ArgumentException(
                "Target format must not be None. Use DecryptAsync to remove encryption.",
                nameof(targetFormat));
        }

        return ReencryptFileAsync(
            path,
            oldPassword: null,
            newPassword,
            targetFormat,
            requireSourceEncrypted: false,
            options,
            cancellationToken);
    }

    /// <summary>
    /// Removes encryption from a JET / ACE database. The decrypted file
    /// replaces the original through <see cref="ReplaceFileAtomicAsync"/>, a
    /// temp file renamed over it, never an overwrite in place.
    /// </summary>
    /// <param name="path">Path to the file.</param>
    /// <param name="oldPassword">The old password.</param>
    /// <param name="options">The options.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="IOException">Thrown when the file cannot be replaced, for example while an <see cref="AccessReader"/> holds it open; see <see cref="ReplaceFileAtomicAsync"/>.</exception>
    public static ValueTask DecryptAsync(
        string path,
        ReadOnlyMemory<char> oldPassword,
        AccessWriterOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        Guard.NotNullOrEmpty(path, nameof(path));
        return ReencryptFileAsync(
            path,
            oldPassword,
            newPassword: null,
            targetFormat: AccessEncryptionFormat.None,
            requireSourceEncrypted: true,
            options,
            cancellationToken);
    }

    /// <summary>
    /// Stream-based equivalent of <see cref="ChangePasswordAsync(string,ReadOnlyMemory{char},ReadOnlyMemory{char},AccessWriterOptions?,CancellationToken)"/>.
    /// </summary>
    /// <param name="stream">The stream.</param>
    /// <param name="oldPassword">The old password.</param>
    /// <param name="newPassword">The new password.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    public static ValueTask ChangePasswordAsync(
        Stream stream,
        ReadOnlyMemory<char> oldPassword,
        ReadOnlyMemory<char> newPassword,
        CancellationToken cancellationToken = default)
    {
        Guard.NotNull(stream, nameof(stream));
        Guard.NotEmpty(newPassword, nameof(newPassword));
        return ReencryptStreamAsync(
            stream,
            oldPassword,
            newPassword,
            targetFormat: null,
            requireSourceEncrypted: true,
            cancellationToken);
    }

    /// <summary>
    /// Stream-based equivalent of <see cref="EncryptAsync(string,ReadOnlyMemory{char},AccessEncryptionFormat?,AccessWriterOptions?,CancellationToken)"/>.
    /// </summary>
    /// <param name="stream">The stream.</param>
    /// <param name="newPassword">The new password.</param>
    /// <param name="targetFormat">The target format.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="ArgumentException">Thrown when <paramref name="targetFormat"/> is <see cref="AccessEncryptionFormat.None"/>.</exception>
    public static ValueTask EncryptAsync(
        Stream stream,
        ReadOnlyMemory<char> newPassword,
        AccessEncryptionFormat? targetFormat = null,
        CancellationToken cancellationToken = default)
    {
        Guard.NotNull(stream, nameof(stream));
        Guard.NotEmpty(newPassword, nameof(newPassword));
        if (targetFormat == AccessEncryptionFormat.None)
        {
            throw new ArgumentException(
                "Target format must not be None. Use DecryptAsync to remove encryption.",
                nameof(targetFormat));
        }

        return ReencryptStreamAsync(
            stream,
            oldPassword: null,
            newPassword,
            targetFormat,
            requireSourceEncrypted: false,
            cancellationToken);
    }

    /// <summary>
    /// Stream-based equivalent of <see cref="DecryptAsync(string,ReadOnlyMemory{char},AccessWriterOptions?,CancellationToken)"/>.
    /// </summary>
    /// <param name="stream">The stream.</param>
    /// <param name="oldPassword">The old password.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    public static ValueTask DecryptAsync(
        Stream stream,
        ReadOnlyMemory<char> oldPassword,
        CancellationToken cancellationToken = default)
    {
        Guard.NotNull(stream, nameof(stream));
        return ReencryptStreamAsync(
            stream,
            oldPassword,
            newPassword: null,
            targetFormat: AccessEncryptionFormat.None,
            requireSourceEncrypted: true,
            cancellationToken);
    }

    private static async ValueTask ReencryptFileAsync(
        string path,
        ReadOnlyMemory<char>? oldPassword,
        ReadOnlyMemory<char>? newPassword,
        AccessEncryptionFormat? targetFormat,
        bool requireSourceEncrypted,
        AccessWriterOptions? options,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        Guard.RequireExistingDatabaseFile(path, nameof(path));

        using var lockFile = LockFileCoordinator.ForReencrypt(path, options);
        lockFile.Acquire();

        byte[] sourceBytes = await File.ReadAllBytesAsync(path, cancellationToken).ConfigureAwait(false);
        await using var sourceStream = new MemoryStream(sourceBytes, writable: false);

        byte[] result = await ReencryptCoreAsync(
            sourceStream,
            oldPassword,
            newPassword,
            targetFormat,
            requireSourceEncrypted,
            cancellationToken).ConfigureAwait(false);

        await ReplaceFileAtomicAsync(path, result, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// <para>
    /// Replaces the file at <paramref name="path"/> with
    /// <paramref name="contents"/> atomically: the contents go to a temp file
    /// beside it (<c>&lt;path&gt;.reenc-&lt;guid&gt;.tmp</c>), which is flushed to
    /// disk and then renamed over the original by <see cref="ReplaceFileWithTemp"/>.
    /// The rename does not expose a partially written file. The containing
    /// directory is not flushed, so persistence of the new name after a power
    /// loss depends on the host filesystem.
    /// </para>
    /// <para>
    /// When the replace is refused, as it is on Windows while any handle
    /// holds the file without <see cref="FileShare.Delete"/> (an open
    /// <see cref="AccessReader"/> does by default) or while the file is
    /// read-only, this deletes the temp file, throws, and leaves the original
    /// untouched. It never overwrites the original in place. The one failure
    /// that does not leave the original is described under
    /// <see cref="ReplaceFileWithTemp"/>: its exception carries the temp
    /// file's path under <see cref="ReplacementFileDataKey"/>.
    /// </para>
    /// </summary>
    /// <param name="path">The file to replace.</param>
    /// <param name="contents">Its new contents.</param>
    /// <param name="cancellationToken">A token used to cancel the operation; it is honoured until the temp file is written.</param>
    /// <returns>A <see cref="ValueTask"/> that completes once the file is replaced.</returns>
    /// <exception cref="OperationCanceledException">Thrown when <paramref name="cancellationToken"/> is cancelled while the temp file is written; the temp file is deleted and the original is unchanged.</exception>
    /// <exception cref="IOException">
    /// Thrown when the temp file cannot be written or the replace is refused.
    /// The temp file is deleted and the original is unchanged, unless the
    /// exception's <see cref="Exception.Data"/> holds
    /// <see cref="ReplacementFileDataKey"/>: then the original was removed and
    /// the new contents are only in the temp file that entry names.
    /// </exception>
    internal static async ValueTask ReplaceFileAtomicAsync(string path, ReadOnlyMemory<byte> contents, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        string tempPath = path + ".reenc-" + Guid.NewGuid().ToString("N") + ".tmp";
        try
        {
            await using FileStream fs = FileStreamFactory.Open(
                tempPath,
                FileMode.CreateNew,
                FileAccess.Write,
                FileShare.None,
                FileOptions.Asynchronous,
                preallocationSize: contents.Length);
            await fs.WriteAsync(contents, cancellationToken).ConfigureAwait(false);
            await fs.FlushAsync(cancellationToken).ConfigureAwait(false);

            // Without this, a power loss after the rename can leave the new
            // name pointing at data that never reached the disk.
#pragma warning disable CA1849 // FileStream has no asynchronous Flush(flushToDisk: true).
            fs.Flush(flushToDisk: true);
#pragma warning restore CA1849
        }
        catch
        {
            TryDeleteFile(tempPath);
            throw;
        }

        ReplaceFileWithTemp(tempPath, path);
    }

    /// <summary>
    /// <para>
    /// Renames <paramref name="tempPath"/> over <paramref name="path"/> in
    /// one step: <see cref="File.Replace(string, string, string?, bool)"/>,
    /// which keeps the original's attributes and ACL, and if that is refused,
    /// on .NET 6 and later (the net10.0 build), <c>File.Move</c> with
    /// overwrite. The caller has flushed the temp file to disk; it must be in
    /// the same directory.
    /// </para>
    /// <para>
    /// It never deletes or overwrites <paramref name="path"/> first: that
    /// would leave no good copy if it failed partway or the process died. When
    /// both renames are refused it deletes the temp file and throws an
    /// <see cref="IOException"/> that says what refused it (a handle without
    /// <see cref="FileShare.Delete"/>, a read-only file, denied access, or a
    /// platform with no rename-over), and the original is unchanged.
    /// netstandard2.1 has no overwriting move, so there the refused
    /// <see cref="File.Replace(string, string, string?, bool)"/> is final.
    /// </para>
    /// <para>
    /// The one exception is a replace that removed the original but could
    /// not rename the temp file (Windows'
    /// <c>ERROR_UNABLE_TO_MOVE_REPLACEMENT</c>): the temp file is then moved
    /// to <paramref name="path"/>, or, if that fails too, kept, since it
    /// holds the only copy. The exception then names it and carries its path
    /// in <see cref="Exception.Data"/> under <see cref="ReplacementFileDataKey"/>,
    /// so a caller can tell this case from a refusal that left the original.
    /// </para>
    /// </summary>
    /// <param name="tempPath">The flushed temp file holding the new contents.</param>
    /// <param name="path">The file to replace.</param>
    /// <exception cref="IOException">Thrown when the file cannot be replaced; the original is unchanged unless the exception carries <see cref="ReplacementFileDataKey"/>.</exception>
    internal static void ReplaceFileWithTemp(string tempPath, string path)
    {
        Exception replaceError;
        try
        {
            File.Replace(tempPath, path, destinationBackupFileName: null, ignoreMetadataErrors: true);
            return;
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or PlatformNotSupportedException)
        {
            replaceError = ex;
        }

        Exception cause = replaceError;
        try
        {
#if NET6_0_OR_GREATER
            File.Move(tempPath, path, overwrite: true);
            return;
#else
            if (!File.Exists(path))
            {
                File.Move(tempPath, path);
                return;
            }
#endif
        }
        catch (Exception moveError) when (moveError is IOException or UnauthorizedAccessException)
        {
            // The replace's error is the first and usually the same refusal;
            // only on a platform without File.Replace does the move's say more.
            if (replaceError is PlatformNotSupportedException)
            {
                cause = moveError;
            }
        }

        if (!File.Exists(path))
        {
            var stranded = new IOException(
                $"Could not replace '{path}': {cause.Message} The original file was removed, and its new contents are in '{tempPath}'. " +
                $"Move that file to '{path}' to finish the replace.",
                cause);
            stranded.Data[ReplacementFileDataKey] = tempPath;
            throw stranded;
        }

        TryDeleteFile(tempPath);
        string message = $"Could not replace '{path}': {cause.Message} The file was left unchanged.";
        string? refusal = DescribeReplaceRefusal(cause, path);
        throw new IOException(refusal is null ? message : message + " " + refusal, cause);
    }

    /// <summary>
    /// Says what refused <see cref="ReplaceFileWithTemp"/>, as far as the
    /// error shows it, for the end of its exception's message.
    /// </summary>
    /// <param name="error">The error the refused replace (or move) threw.</param>
    /// <param name="path">The file that was to be replaced.</param>
    /// <returns>The sentences to append, or <see langword="null"/> when the error's own message is all there is to say.</returns>
    private static string? DescribeReplaceRefusal(Exception error, string path)
    {
        if (error is IOException && error.HResult is SharingViolationHResult or UnableToRemoveReplacedHResult)
        {
            return "It is replaced by renaming a temp file over it, which Windows refuses while any process holds the file open " +
                "without FileShare.Delete; an open AccessReader holds it so by default. Close every handle on the file and try again.";
        }

        if (error is UnauthorizedAccessException)
        {
            return IsReadOnlyOnWindows(path)
                ? "The file is read-only, and Windows does not rename a file over a read-only one. Clear its read-only attribute and try again."
                : "Access to the file or its directory was denied: replacing the file needs permission to delete it and to write to its directory.";
        }

        // Only the netstandard2.1 build gets here with this: the net10.0
        // build reports the overwriting move's error instead.
        return error is PlatformNotSupportedException
            ? "This platform does not support File.Replace, and this build of the library has no other way to rename a file over another."
            : null;
    }

    /// <summary>
    /// Whether <paramref name="path"/> has the read-only attribute on Windows,
    /// where that alone refuses a rename over it. On other systems a file's
    /// own permissions do not block a rename over it, so this is always false
    /// there.
    /// </summary>
    /// <param name="path">The file to check.</param>
    /// <returns><see langword="true"/> for a read-only file on Windows.</returns>
    private static bool IsReadOnlyOnWindows(string path)
    {
#if NET6_0_OR_GREATER
        bool isWindows = OperatingSystem.IsWindows();
#else
        bool isWindows = RuntimeInformation.IsOSPlatform(OSPlatform.Windows);
#endif
        if (!isWindows)
        {
            return false;
        }

        try
        {
            return (File.GetAttributes(path) & FileAttributes.ReadOnly) != 0;
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            return false;
        }
    }

    private static void TryDeleteFile(string path)
    {
        try
        {
            File.Delete(path);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            // Best effort: the temp file holds nothing the caller still needs.
        }
    }

    private static async ValueTask ReencryptStreamAsync(
        Stream stream,
        ReadOnlyMemory<char>? oldPassword,
        ReadOnlyMemory<char>? newPassword,
        AccessEncryptionFormat? targetFormat,
        bool requireSourceEncrypted,
        CancellationToken cancellationToken)
    {
        Guard.RequireReadWriteSeekableStream(stream, nameof(stream));

        byte[] result = await ReencryptCoreAsync(
            stream,
            oldPassword,
            newPassword,
            targetFormat,
            requireSourceEncrypted,
            cancellationToken).ConfigureAwait(false);

        _ = stream.Seek(0, SeekOrigin.Begin);
        await stream.WriteAsync(result.AsMemory(), cancellationToken).ConfigureAwait(false);
        stream.SetLength(result.Length);
        await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    private static async ValueTask<byte[]> ReencryptCoreAsync(
        Stream source,
        ReadOnlyMemory<char>? oldPassword,
        ReadOnlyMemory<char>? newPassword,
        AccessEncryptionFormat? targetFormat,
        bool requireSourceEncrypted,
        CancellationToken cancellationToken)
    {
        ReadOnlyMemory<char> oldPwd = oldPassword.GetValueOrDefault();
        ReadOnlyMemory<char> newPwd = newPassword.GetValueOrDefault();

        long origPos = source.Position;
        AccessEncryptionFormat detectedFormat = await DetectEncryptionFormatAsync(source, cancellationToken).ConfigureAwait(false);
        _ = source.Seek(origPos, SeekOrigin.Begin);

        if (requireSourceEncrypted && detectedFormat == AccessEncryptionFormat.None)
        {
            throw new InvalidOperationException(
                "The source database is not encrypted. Use EncryptAsync to add a password.");
        }

        if (!requireSourceEncrypted && detectedFormat != AccessEncryptionFormat.None)
        {
            throw new InvalidOperationException(
                $"The source database is already encrypted ({detectedFormat}). Use ChangePasswordAsync or DecryptAsync.");
        }

        if (detectedFormat is AccessEncryptionFormat.Jet3Rc4 or AccessEncryptionFormat.Jet4Rc4 or AccessEncryptionFormat.AccdbAgile)
        {
            throw new NotSupportedException("Changing or removing native database encryption requires native system security metadata that is not supported. The database remains unchanged.");
        }

        (byte[] plaintext, AccessEncryptionFormat sourceFormat) = await EncryptionConverter
            .ReadDecryptedAsync(source, oldPwd, cancellationToken)
            .ConfigureAwait(false);

        if (sourceFormat == AccessEncryptionFormat.Jet4Rc4)
        {
            throw new NotSupportedException("Changing or removing native Jet4 encryption requires native system security metadata that is not supported. The database remains unchanged.");
        }

        AccessEncryptionFormat effectiveTarget = targetFormat
            ?? (requireSourceEncrypted
                ? sourceFormat
                : EncryptionConverter.ResolveBestTargetFormat(plaintext));
        return EncryptionConverter.ApplyEncryption(plaintext, effectiveTarget, newPwd);
    }

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
    internal static void WriteNativeJet4EncryptionHeader(byte[] header, uint encodingKey, ReadOnlySpan<char> password)
    {
        if (password.Length > HeaderPasswordLength / sizeof(char))
        {
            throw new JetLimitationException("Jet4 passwords cannot exceed twenty UTF-16 characters.");
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
