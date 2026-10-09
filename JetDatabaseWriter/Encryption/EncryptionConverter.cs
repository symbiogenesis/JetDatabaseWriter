namespace JetDatabaseWriter.Encryption;

using System;
using System.IO;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Infrastructure;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>Transforms native flat databases page by page and detects their providers.</summary>
internal static class EncryptionConverter
{
    private const int HeaderLength = 0x80;

    /// <summary>Copies a native flat database through one reusable page buffer.</summary>
    /// <param name="source">The readable seekable source, retained by the caller.</param>
    /// <param name="destination">The writable destination, retained by the caller.</param>
    /// <param name="targetHeader">The prepared output page zero.</param>
    /// <param name="sourceCodec">The source page codec, retained by the caller.</param>
    /// <param name="targetCodec">The target page codec, retained by the caller.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The asynchronous copy operation.</returns>
    /// <exception cref="InvalidDataException">The source does not contain whole native pages.</exception>
    internal static async ValueTask TransformPagesAsync(
        Stream source,
        Stream destination,
        byte[] targetHeader,
        IPageCodec sourceCodec,
        IPageCodec targetCodec,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        int pageSize = targetHeader.Length;
        long length = source.Length;
        if (pageSize is not (Constants.PageSizes.Jet3 or Constants.PageSizes.Jet4)
            || length < pageSize || length % pageSize != 0)
        {
            throw new InvalidDataException("The source database must contain complete native database pages.");
        }

        _ = source.Seek(pageSize, SeekOrigin.Begin);
        await destination.WriteAsync(targetHeader.AsMemory(), cancellationToken).ConfigureAwait(false);
        byte[] buffer = new byte[pageSize];
        try
        {
            for (long page = 1; page < length / pageSize; page++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await source.ReadExactlyAsync(buffer.AsMemory(), cancellationToken).ConfigureAwait(false);
                sourceCodec.Decode(buffer, 0, page, pageSize);
                targetCodec.Encode(buffer, 0, page, pageSize);
                await destination.WriteAsync(buffer.AsMemory(), cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            CryptographicOperations.ZeroMemory(buffer);
        }
    }

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

    private static AccessEncryptionFormat DetectFlatFormat(byte[] header, DatabaseFormat fmt)
    {
        if (fmt == DatabaseFormat.AceAccdb && OfficeCryptoAgile.IsFlatAgileEncrypted(header))
        {
            return AccessEncryptionFormat.AccdbAgile;
        }

        if (fmt == DatabaseFormat.AceAccdb)
        {
            byte[] unmasked = (byte[])header.Clone();
            try
            {
                EncryptionManager.TransformHeaderMask(unmasked, fmt);
                if (Ru32(unmasked, Constants.DatabaseHeader.EncodingKey) == 0)
                {
                    return AccessEncryptionFormat.None;
                }

                const int infoOffset = Constants.AgileEncryption.FlatEncryptionInfoOffset;
                const int algorithmOffset = infoOffset + 20;
                if (header.Length < algorithmOffset + sizeof(uint))
                {
                    throw new InvalidDataException("The native ACE encryption descriptor is truncated.");
                }

                uint algorithm = Ru32(header, algorithmOffset);
                uint flags = Ru32(header, infoOffset + 4);
                if (algorithm == 0)
                {
                    algorithm = (flags & 0x20) == 0 ? 0x6801u : 0x660Eu;
                }

                return algorithm switch
                {
                    0x6801 => AccessEncryptionFormat.AccdbRc4CryptoApi,
                    0x660E or 0x660F or 0x6610 => AccessEncryptionFormat.AccdbStandard,
                    _ => throw new NotSupportedException($"The native ACE encryption algorithm 0x{algorithm:X} is unsupported."),
                };
            }
            finally
            {
                CryptographicOperations.ZeroMemory(unmasked);
            }
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
