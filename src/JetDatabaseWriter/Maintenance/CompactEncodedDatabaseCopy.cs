namespace JetDatabaseWriter.Maintenance;

using System;
using System.IO;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Infrastructure;

/// <summary>Encodes closed private decrypted staging with its exact original native header and compares reopened pages.</summary>
/// <remarks>
/// The immutable original header, password memory and decrypted staging must remain unchanged throughout the call.
/// Staging is a private decrypted image retaining its original encrypted header; its structural validation remains
/// separate. No header, provider parameter, password or security identity is regenerated. One page is transformed
/// at a time. All transferred handles close before independent comparison opens; no whole-file snapshot is retained.
/// This private codec boundary never publishes output, replaces the original or authorizes reclamation.
/// </remarks>
internal static class CompactEncodedDatabaseCopy
{
    /// <summary>Encodes, closes, reopens with the original password and compares every decrypted staging page.</summary>
    /// <param name="originalHeader">The immutable complete original raw native page zero.</param>
    /// <param name="password">The immutable original password memory, retained by the caller.</param>
    /// <param name="workspace">Distinct exclusively owned private stores with transferred handle ownership.</param>
    /// <param name="budget">Cumulative encoding and independent comparison admission.</param>
    /// <param name="cancellationToken">Cancels factories, password derivation, page I/O and comparison.</param>
    /// <returns>Private complete codec comparison evidence without database acceptance.</returns>
    /// <exception cref="ArgumentException">Private storage capabilities, independent reopening or distinctness are invalid.</exception>
    /// <exception cref="JetCorruptDataException">Header binding, page geometry or reopened decrypted bytes disagree.</exception>
    /// <exception cref="JetLimitationException">A cumulative resource or password hashing budget is exhausted.</exception>
    /// <exception cref="JetAccessDeniedException">The original password is missing or incorrect.</exception>
    /// <exception cref="JetNotSupportedException">The original native encryption provider is unsupported.</exception>
    internal static async ValueTask<CompactEncodedDatabaseResult> EncodeAndValidateAsync(
        ReadOnlyMemory<byte> originalHeader,
        ReadOnlyMemory<char> password,
        CompactEncodedDatabaseWorkspace workspace,
        CompactEncodedDatabaseBudget budget,
        CancellationToken cancellationToken)
    {
        ValidateArguments(originalHeader, workspace, budget);
        cancellationToken.ThrowIfCancellationRequested();
        long working = 262144L + (128L * originalHeader.Length) + (4L * password.Length);
        if (working > budget.MaxWorkingBytes)
        {
            throw Limit("Private codec buffers and native codec admission exceed MaxWorkingBytes.");
        }

        byte[] header = originalHeader.ToArray();
        byte[] buffer = new byte[header.Length];
        byte[] comparison = new byte[header.Length];
        try
        {
            JetFormat.ValidateNativeSignature(header);
            DatabaseFormat format = JetFormat.DetectFormat(header);
            if (JetFormat.PageSizeOf(format) != header.Length)
            {
                throw Corrupt("The original codec header is not exactly one matching native page.");
            }

            AccessEncryptionFormat provider = EncryptionConverter.Detect(header);
            var admission = new Admission(budget, cancellationToken);
            var hashing = new EncryptionWorkBudget(budget.MaxTotalEncryptionSpinCount);
            Stream input = await workspace.OpenDecrypted(cancellationToken).ConfigureAwait(false);
            Guard.NotNull(input, nameof(workspace));
            Stream? writer = null;
            long length;
            try
            {
                admission.Check();
                RequireInput(input);
                length = input.Length;
                AdmitLength(length, header.Length, budget);
                await admission.ReadAsync(input, buffer, cancellationToken).ConfigureAwait(false);
                RequireHeader(header, buffer);
                using IPageCodec codec = OpenCodec(header, format, password, budget, hashing, cancellationToken);
                admission.Check();
                writer = await workspace.CreateEncoded(cancellationToken).ConfigureAwait(false);
                Guard.NotNull(writer, nameof(workspace));
                try
                {
                    RequireDistinct(input, writer);
                    admission.Check();
                    if (!writer.CanRead || !writer.CanWrite || !writer.CanSeek || writer.Length != 0)
                    {
                        throw new ArgumentException("Encoded output must be empty readable writable seekable private storage.", nameof(workspace));
                    }

                    writer.Position = 0;
                    await admission.WriteAsync(writer, header, cancellationToken).ConfigureAwait(false);
                    for (long page = 1; page < length / header.Length; page++)
                    {
                        RequireLength(input, length);
                        await admission.ReadAsync(input, buffer, cancellationToken).ConfigureAwait(false);
                        codec.Encode(buffer, 0, page, buffer.Length);
                        await admission.WriteAsync(writer, buffer, cancellationToken).ConfigureAwait(false);
                    }

                    RequireLength(input, length);
                    RequireLength(writer, length);
                    admission.Check();
                    await writer.FlushAsync(cancellationToken).ConfigureAwait(false);
                }
                finally
                {
                    if (!ReferenceEquals(input, writer))
                    {
                        await writer.DisposeAsync().ConfigureAwait(false);
                    }
                }
            }
            finally
            {
                await input.DisposeAsync().ConfigureAwait(false);
            }

            admission.Check();
            Stream reopened = await workspace.OpenDecrypted(cancellationToken).ConfigureAwait(false);
            Guard.NotNull(reopened, nameof(workspace));
            try
            {
                RejectEarlier(reopened, input, writer);
                RequireInput(reopened);
                RequireLength(reopened, length);
                admission.Check();
                Stream encoded = await workspace.OpenEncoded(cancellationToken).ConfigureAwait(false);
                Guard.NotNull(encoded, nameof(workspace));
                try
                {
                    RejectEarlier(encoded, input, writer);
                    RequireDistinct(reopened, encoded);
                    RequireInput(encoded);
                    RequireLength(encoded, length);
                    await admission.ReadAsync(encoded, comparison, cancellationToken).ConfigureAwait(false);
                    RequireHeader(header, comparison);
                    using IPageCodec codec = OpenCodec(comparison, format, password, budget, hashing, cancellationToken);
                    for (long page = 0; page < length / header.Length; page++)
                    {
                        RequireLength(reopened, length);
                        RequireLength(encoded, length);
                        await admission.ReadAsync(reopened, buffer, cancellationToken).ConfigureAwait(false);
                        if (page > 0)
                        {
                            await admission.ReadAsync(encoded, comparison, cancellationToken).ConfigureAwait(false);
                            codec.Decode(comparison, 0, page, comparison.Length);
                        }

                        admission.Check();
                        if (!buffer.AsSpan().SequenceEqual(comparison))
                        {
                            throw Corrupt($"Reopened native codec output differs from immutable decrypted staging at page {page}.");
                        }
                    }

                    RequireLength(reopened, length);
                    RequireLength(encoded, length);
                    admission.Check();
                    return new(provider, length / header.Length, admission.ReadBytes, admission.WrittenBytes);
                }
                finally
                {
                    if (!ReferenceEquals(encoded, reopened))
                    {
                        await encoded.DisposeAsync().ConfigureAwait(false);
                    }
                }
            }
            finally
            {
                await reopened.DisposeAsync().ConfigureAwait(false);
            }
        }
        finally
        {
            CryptographicOperations.ZeroMemory(header);
            CryptographicOperations.ZeroMemory(buffer);
            CryptographicOperations.ZeroMemory(comparison);
        }
    }

    private static void ValidateArguments(ReadOnlyMemory<byte> header, CompactEncodedDatabaseWorkspace workspace, CompactEncodedDatabaseBudget budget)
    {
        Guard.NotNull(workspace, nameof(workspace));
        Guard.NotNull(workspace.OpenDecrypted, nameof(workspace));
        Guard.NotNull(workspace.CreateEncoded, nameof(workspace));
        Guard.NotNull(workspace.OpenEncoded, nameof(workspace));
        Guard.NotNull(budget, nameof(budget));
        Guard.InRange(budget.MaxPages, 0L, 0x1000000L, nameof(budget));
        Guard.InRange(budget.MaxOutputBytes, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxReadBytes, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxWriteBytes, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxWorkingBytes, 1L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxEncryptionSpinCount, 0, int.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxTotalEncryptionSpinCount, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxEncryptionInfoBytes, 0, int.MaxValue, nameof(budget));
        if (header.Length is not (Constants.PageSizes.Jet3 or Constants.PageSizes.Jet4))
        {
            throw Corrupt("Private codec encoding requires one complete original native header page.");
        }
    }

    private static IPageCodec OpenCodec(byte[] header, DatabaseFormat format, ReadOnlyMemory<char> password, CompactEncodedDatabaseBudget budget, EncryptionWorkBudget hashing, CancellationToken token)
        => PageCodecFactory.Open(header, format, password, "password", budget.MaxEncryptionSpinCount, budget.MaxEncryptionInfoBytes, hashing, token);

    private static void AdmitLength(long length, int pageSize, CompactEncodedDatabaseBudget budget)
    {
        if (length < pageSize || length % pageSize != 0)
        {
            throw Corrupt("Private decrypted staging must contain whole native pages and a complete header.");
        }

        if (length / pageSize > budget.MaxPages || length > budget.MaxOutputBytes)
        {
            throw Limit("Private native codec output exceeds page or output-byte admission.");
        }
    }

    private static void RequireInput(Stream input)
    {
        if (!input.CanRead || !input.CanSeek || input.CanWrite)
        {
            throw new ArgumentException("Private comparison inputs must be immutable readable seekable streams.", nameof(input));
        }

        input.Position = 0;
    }

    private static void RequireHeader(byte[] expected, byte[] actual)
    {
        if (!expected.AsSpan().SequenceEqual(actual))
        {
            throw Corrupt("Private staging or encoded page zero differs from the exact immutable original native header.");
        }
    }

    private static void RequireLength(Stream stream, long expected)
    {
        if (stream.Length != expected)
        {
            throw Corrupt("Private codec input or output length changed during construction or comparison.");
        }
    }

    private static void RequireDistinct(Stream first, Stream second)
    {
        if (ReferenceEquals(first, second) || (first is FileStream left && second is FileStream right && string.Equals(left.Name, right.Name, StringComparison.OrdinalIgnoreCase)))
        {
            throw new ArgumentException("Decrypted and encoded stores must have distinct backing storage, including through wrappers.");
        }
    }

    private static void RejectEarlier(Stream stream, Stream input, Stream? writer)
    {
        if (ReferenceEquals(stream, input) || ReferenceEquals(stream, writer))
        {
            throw new ArgumentException("Private codec comparison must independently reopen closed construction handles.", nameof(stream));
        }
    }

    private static JetCorruptDataException Corrupt(string reason) => new(JetErrorCode.CorruptPage, reason);

    private static JetLimitationException Limit(string reason) => new(JetErrorCode.ValueTooLarge, reason);

    private sealed class Admission(CompactEncodedDatabaseBudget budget, CancellationToken token)
    {
        internal long ReadBytes { get; private set; }

        internal long WrittenBytes { get; private set; }

        internal void Check() => token.ThrowIfCancellationRequested();

        internal async ValueTask ReadAsync(Stream stream, byte[] buffer, CancellationToken cancellationToken)
        {
            int offset = 0;
            while (offset < buffer.Length)
            {
                this.Check();
                int requested = buffer.Length - offset;
                if (requested > budget.MaxReadBytes - this.ReadBytes)
                {
                    throw Limit("Private codec cumulative requested reads exceed MaxReadBytes.");
                }

                this.ReadBytes += requested;
                int read = await stream.ReadAsync(buffer.AsMemory(offset, requested), cancellationToken).ConfigureAwait(false);
                if (read == 0)
                {
                    throw Corrupt("Private codec input has a truncated native page.");
                }

                offset += read;
            }
        }

        internal async ValueTask WriteAsync(Stream stream, byte[] buffer, CancellationToken cancellationToken)
        {
            this.Check();
            if (buffer.Length > budget.MaxWriteBytes - this.WrittenBytes)
            {
                throw Limit("Private codec cumulative writes exceed MaxWriteBytes.");
            }

            this.WrittenBytes += buffer.Length;
            await stream.WriteAsync(buffer.AsMemory(), cancellationToken).ConfigureAwait(false);
        }
    }
}
