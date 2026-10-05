namespace JetDatabaseWriter.Encryption;

using System;
using JetDatabaseWriter.Enums;

/// <summary>Selects the page codec after validating the header and password.</summary>
internal static class PageCodecFactory
{
    /// <summary>Opens the codec for a database header.</summary>
    /// <param name="header">The raw header.</param>
    /// <param name="format">The database format.</param>
    /// <param name="isLegacyAesCfb">Whether the header has CFB magic.</param>
    /// <param name="password">The database password.</param>
    /// <param name="passwordOptionName">The password option named in errors.</param>
    /// <returns>The owned codec.</returns>
    internal static IPageCodec Open(byte[] header, DatabaseFormat format, bool isLegacyAesCfb, ReadOnlyMemory<char> password, string passwordOptionName)
        => EncryptionManager.OpenPageCodec(header, format, isLegacyAesCfb, password, passwordOptionName);
}