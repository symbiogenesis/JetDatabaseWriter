namespace JetDatabaseWriter.Encryption;

using System;
using System.IO;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Infrastructure;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// <para>
/// Implements the read-decrypt-rewrite pipeline used by
/// <see cref="AccessWriter.ChangePasswordAsync(string, ReadOnlyMemory{char}, ReadOnlyMemory{char}, AccessWriterOptions?, CancellationToken)"/>,
/// <see cref="AccessWriter.EncryptAsync(string, ReadOnlyMemory{char}, AccessEncryptionFormat?, AccessWriterOptions?, CancellationToken)"/>,
/// and <see cref="AccessWriter.DecryptAsync(string, ReadOnlyMemory{char}, AccessWriterOptions?, CancellationToken)"/>.
/// </para>
/// <para>
/// All public entry points are pure byte-array transforms — they never touch
/// the filesystem directly, so the caller can decide whether to seek-and-rewrite
/// an existing stream or write to a temp file and rename atomically.
/// </para>
/// </summary>
internal static class EncryptionConverter
{
    private const int HeaderLength = 0x80;

    /// <summary>
    /// Resolves the strongest writer-supported password encryption format for
    /// a clean database image based on its JET / ACE format.
    /// </summary>
    /// <param name="plaintext">The plaintext database bytes.</param>
    /// <returns>The default target encryption format.</returns>
    /// <exception cref="InvalidDataException">Thrown when <paramref name="plaintext"/> is shorter than a JET header.</exception>
    /// <exception cref="NotSupportedException">Thrown when the database format has no password encryption target.</exception>
    public static AccessEncryptionFormat ResolveBestTargetFormat(byte[] plaintext)
    {
        Guard.NotNull(plaintext, nameof(plaintext));
        if (plaintext.Length < HeaderLength)
        {
            throw new InvalidDataException("Plaintext database is shorter than the JET header.");
        }

        return JetFormat.DetectFormat(plaintext) switch
        {
            DatabaseFormat.Jet4Mdb => AccessEncryptionFormat.Jet4Rc4,
            DatabaseFormat.AceAccdb => AccessEncryptionFormat.AccdbAgile,
            DatabaseFormat.Jet3Mdb => AccessEncryptionFormat.Jet3Rc4,
            _ => throw new InvalidDataException("The database format is unknown."),
        };
    }

    /// <summary>
    /// Reads <paramref name="source"/>, applies any active decryption, and
    /// returns a fully-plaintext copy of the database (encoding key 0, the
    /// empty-password pattern Access writes in the password area, header
    /// magic restored). The returned byte
    /// array has the same length as the native Jet/ACE database.
    /// </summary>
    /// <param name="source">The source.</param>
    /// <param name="password">The password.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="UnauthorizedAccessException">Thrown when an encrypted flat Agile database is detected and no password was supplied.</exception>
    /// <exception cref="InvalidDataException">The compound file does not contain a supported encrypted package.</exception>
    public static async ValueTask<(byte[] Plaintext, AccessEncryptionFormat SourceFormat)> ReadDecryptedAsync(
        Stream source,
        ReadOnlyMemory<char> password,
        CancellationToken cancellationToken)
    {
        Guard.NotNull(source, nameof(source));

        _ = source.Seek(0, SeekOrigin.Begin);
        byte[] header = new byte[HeaderLength];
        await source.ReadExactlyAsync(header.AsMemory(), cancellationToken).ConfigureAwait(false);

        _ = source.Seek(0, SeekOrigin.Begin);
        byte[] rawFile = new byte[source.Length];
        await source.ReadExactlyAsync(rawFile.AsMemory(), cancellationToken).ConfigureAwait(false);

        if (OfficeCryptoAgile.IsFlatAgileEncrypted(rawFile))
        {
            if (password.IsEmpty)
            {
                throw new UnauthorizedAccessException(
                    "This .accdb file is encrypted with Access Agile encryption. " +
                    $"Provide the database password via {EncryptionManager.OldPasswordArgument} to open it.");
            }

            return (OfficeCryptoAgile.DecryptFlatDatabase(rawFile, password.Span), AccessEncryptionFormat.AccdbAgile);
        }

        DatabaseFormat fmt = JetFormat.DetectFormat(header);
        AccessEncryptionFormat src = DetectFlatFormat(rawFile, fmt);
        await using var rawStream = new MemoryStream(rawFile, writable: false);
        byte[] plaintext = await ReadFlatDecryptedAsync(rawStream, header, password, cancellationToken)
            .ConfigureAwait(false);

        return (plaintext, src);
    }

    /// <summary>
    /// Encodes <paramref name="plaintext"/> in the requested target encryption
    /// format and returns the resulting on-disk bytes. <paramref name="plaintext"/>
    /// must already be a clean (no-encryption) Jet3/Jet4/ACE database.
    /// </summary>
    /// <param name="plaintext">The plaintext.</param>
    /// <param name="targetFormat">The target format.</param>
    /// <param name="targetPassword">The target password.</param>
    /// <exception cref="InvalidDataException">Thrown when <paramref name="plaintext"/> is shorter than a JET header.</exception>
    /// <exception cref="NotSupportedException">Thrown when <paramref name="targetFormat"/> is incompatible with the database format or is unrecognized.</exception>
    /// <exception cref="ArgumentException">Thrown when <paramref name="targetPassword"/> is empty for an encrypted target format.</exception>
    public static byte[] ApplyEncryption(
        byte[] plaintext,
        AccessEncryptionFormat targetFormat,
        ReadOnlyMemory<char> targetPassword)
    {
        Guard.NotNull(plaintext, nameof(plaintext));
        if (plaintext.Length < HeaderLength)
        {
            throw new InvalidDataException("Plaintext database is shorter than the JET header.");
        }

        DatabaseFormat fmt = JetFormat.DetectFormat(plaintext);
        return targetFormat switch
        {
            AccessEncryptionFormat.None => (byte[])plaintext.Clone(),
            AccessEncryptionFormat.Jet3Rc4 when fmt != DatabaseFormat.Jet3Mdb
                => throw new NotSupportedException($"Target format {targetFormat} is only valid for Jet3 (.mdb) databases."),
            AccessEncryptionFormat.Jet4Rc4 when fmt != DatabaseFormat.Jet4Mdb
                => throw new NotSupportedException($"Target format {targetFormat} is only valid for Jet4 (.mdb) databases."),
            AccessEncryptionFormat.AccdbAgile when fmt != DatabaseFormat.AceAccdb
                => throw new NotSupportedException($"Target format {targetFormat} is only valid for ACE (.accdb) databases."),
            AccessEncryptionFormat.Jet3Rc4 or
            AccessEncryptionFormat.Jet4Rc4 or
            AccessEncryptionFormat.AccdbAgile when targetPassword.IsEmpty
                => throw new ArgumentException("A non-empty password is required to apply encryption.", nameof(targetPassword)),
            AccessEncryptionFormat.Jet3Rc4 => throw new NotSupportedException("Creating or changing a Jet3 database password requires native security metadata that is not supported. Existing native encrypted databases can be read and updated with their original password."),
            AccessEncryptionFormat.Jet4Rc4 => throw new NotSupportedException("Creating or changing a Jet4 database password requires native system security metadata that is not supported. Existing native encrypted databases can be read and updated with their original password."),
            AccessEncryptionFormat.AccdbAgile => throw new NotSupportedException("Creating native ACE encryption requires native system security metadata that is not supported. Existing native encrypted databases can be read and updated with their original password."),
            _ => throw new NotSupportedException($"Unhandled target encryption format: {targetFormat}."),
        };
    }

    /// <summary>Detects the on-disk encryption format of <paramref name="rawFile"/> without modifying it.</summary>
    /// <param name="rawFile">The raw file.</param>
    /// <exception cref="NotSupportedException">Office compound packages are not native database inputs.</exception>
    public static AccessEncryptionFormat Detect(byte[] rawFile)
    {
        if (rawFile == null || rawFile.Length < HeaderLength)
        {
            return AccessEncryptionFormat.None;
        }

        if (EncryptionManager.IsCompoundFileEncrypted(rawFile))
        {
            throw new NotSupportedException("Office compound packages are not native Microsoft Access databases.");
        }

        DatabaseFormat fmt = JetFormat.DetectFormat(rawFile);
        return DetectFlatFormat(rawFile, fmt);
    }

    /// <summary>
    /// Reads pages 0..N-1 from a flat (non-CFB) Jet/ACE source, decrypts pages
    /// 1+ using the password-derived keys, and returns a fully plaintext copy
    /// (clean header, no encryption flags, no password residue).
    /// </summary>
    /// <param name="source">The source.</param>
    /// <param name="header">The header.</param>
    /// <param name="password">The password.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <exception cref="InvalidDataException">Thrown when the source database is shorter than one whole JET page.</exception>
    private static async ValueTask<byte[]> ReadFlatDecryptedAsync(
        Stream source,
        byte[] header,
        ReadOnlyMemory<char> password,
        CancellationToken cancellationToken)
    {
        DatabaseFormat fmt = JetFormat.DetectFormat(header);
        int pageSize = fmt == DatabaseFormat.Jet3Mdb ? Constants.PageSizes.Jet3 : Constants.PageSizes.Jet4;

        using IPageCodec pageKeys = PageCodecFactory.Open(header, fmt, password, EncryptionManager.OldPasswordArgument);

        long length = source.Length;
        if (length % pageSize != 0)
        {
            // Some Access tools leave a trailing partial page; truncate to the
            // last whole page so we don't try to decrypt a short tail.
            length -= length % pageSize;
        }

        if (length < pageSize)
        {
            throw new InvalidDataException("Source database is shorter than a single JET page.");
        }

        byte[] result = new byte[length];

        // Page 0: copy the header verbatim, then sanitise it.
        _ = source.Seek(0, SeekOrigin.Begin);
        await source.ReadExactlyAsync(result.AsMemory(0, pageSize), cancellationToken).ConfigureAwait(false);
        StripEncryptionFromHeader(result, fmt);

        bool hasPageEncryption = pageKeys.HasEncryption;

        // Pages 1+: read raw, decrypt in place.
        for (long page = 1, offset = pageSize; offset < length; page++, offset += pageSize)
        {
            cancellationToken.ThrowIfCancellationRequested();
            _ = source.Seek(offset, SeekOrigin.Begin);
            await source.ReadExactlyAsync(result.AsMemory((int)offset, pageSize), cancellationToken).ConfigureAwait(false);

            if (hasPageEncryption)
            {
                pageKeys.Decode(result, checked((int)offset), page, pageSize);
            }
        }

        return result;
    }

    /// <summary>
    /// Removes any encryption residue from a freshly-read header so the page
    /// becomes the header Access writes for an unencrypted database. In
    /// the masked header region, sets the
    /// encoding key to 0 and writes the empty-password pattern over the
    /// password area, which also covers the Jet4 / ACE flag byte at
    /// <c>0x62</c>; on Jet3, whose password area ends at <c>0x55</c>, the
    /// flag byte is cleared on its own.
    /// </summary>
    /// <param name="db">The database input.</param>
    /// <param name="fmt">The database format.</param>
    private static void StripEncryptionFromHeader(byte[] db, DatabaseFormat fmt)
    {
        // The encoding key, the password area and the flag byte all lie in
        // the masked header region, so edit them unmasked. Writing raw zeros
        // instead would leave an unmasked key of 0x4EBC8AFB, which Jackcess and
        // mdbtools read as an encrypted file.
        EncryptionManager.TransformHeaderMask(db);
        Array.Clear(db, Constants.DatabaseHeader.EncodingKey, 4);
        EncryptionManager.WriteEmptyHeaderPassword(db, fmt);
        if (fmt == DatabaseFormat.Jet3Mdb)
        {
            db[0x62] = 0;
        }

        EncryptionManager.TransformHeaderMask(db);
    }

    private static AccessEncryptionFormat DetectFlatFormat(byte[] header, DatabaseFormat fmt)
    {
        if (fmt == DatabaseFormat.AceAccdb && OfficeCryptoAgile.IsFlatAgileEncrypted(header))
        {
            return AccessEncryptionFormat.AccdbAgile;
        }

        if (header.Length <= 0x62)
        {
            return AccessEncryptionFormat.None;
        }

        if (fmt is DatabaseFormat.Jet3Mdb or DatabaseFormat.Jet4Mdb)
        {
            byte[] unmasked = (byte[])header.Clone();
            EncryptionManager.TransformHeaderMask(unmasked, fmt);
            uint encodingKey = Ru32(unmasked, Constants.DatabaseHeader.EncodingKey);
            CryptographicOperations.ZeroMemory(unmasked);
            if (encodingKey == 0)
            {
                return AccessEncryptionFormat.None;
            }

            return fmt == DatabaseFormat.Jet3Mdb ? AccessEncryptionFormat.Jet3Rc4 : AccessEncryptionFormat.Jet4Rc4;
        }

        return AccessEncryptionFormat.None;
    }
}
