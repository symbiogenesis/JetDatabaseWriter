namespace JetDatabaseWriter.Encryption;

using System;
using System.Buffers.Binary;
using System.Threading;
using JetDatabaseWriter.Enums;

/// <summary>Selects the page codec after validating the header and password.</summary>
internal static class PageCodecFactory
{
    /// <summary>Opens the codec for a database header.</summary>
    /// <param name="header">The raw header.</param>
    /// <param name="format">The database format.</param>
    /// <param name="password">The database password.</param>
    /// <param name="passwordOptionName">The password option named in errors.</param>
    /// <param name="maxEncryptionSpinCount">The password hashing budget.</param>
    /// <param name="maxEncryptionInfoBytes">The descriptor byte budget.</param>
    /// <param name="workBudget">The aggregate password hashing budget.</param>
    /// <param name="cancellationToken">Cancellation for password hashing.</param>
    /// <exception cref="UnauthorizedAccessException">The native encrypted database requires a password.</exception>
    /// <exception cref="System.IO.InvalidDataException">The native descriptor is missing or malformed.</exception>
    /// <exception cref="NotSupportedException">The native provider is unsupported.</exception>
    /// <returns>The owned codec.</returns>
    internal static IPageCodec Open(byte[] header, DatabaseFormat format, ReadOnlyMemory<char> password, string passwordOptionName, int maxEncryptionSpinCount = 1_000_000, int maxEncryptionInfoBytes = 1024 * 1024, EncryptionWorkBudget? workBudget = null, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        byte[] unmasked = (byte[])header.Clone();
        EncryptionManager.TransformHeaderMask(unmasked);
        bool nativeAceEncrypted = format == DatabaseFormat.AceAccdb
            && BinaryPrimitives.ReadUInt32LittleEndian(unmasked.AsSpan(Constants.DatabaseHeader.EncodingKey, sizeof(uint))) != 0;

        if (nativeAceEncrypted)
        {
            if (password.IsEmpty)
            {
                throw new UnauthorizedAccessException($"The native encrypted database requires a password via {passwordOptionName}.");
            }

            return OfficeCryptoAgile.IsFlatAgileEncrypted(header)
                ? OfficeCryptoAgile.CreateFlatPageCodec(header, password.Span, maxEncryptionSpinCount, maxEncryptionInfoBytes, workBudget, cancellationToken)
                : NativeStandardPageCodec.Open(header, password.Span, maxEncryptionSpinCount, maxEncryptionInfoBytes, workBudget, cancellationToken);
        }

        return EncryptionManager.OpenPageCodec(header, format, password, passwordOptionName);
    }
}
