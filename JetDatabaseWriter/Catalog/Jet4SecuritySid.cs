namespace JetDatabaseWriter.Catalog;

using System;
using System.Buffers.Binary;
using System.Security.Cryptography;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Exceptions;

/// <summary>Encodes Jet4 security identities using the database header's SID key.</summary>
internal static class Jet4SecuritySid
{
    /// <summary>Returns the masked inherited-owner placeholder for a raw Jet4 page zero.</summary>
    /// <param name="format">The database format profile.</param>
    /// <param name="rawHeader">The raw database header.</param>
    /// <returns>The stored two-byte owner placeholder.</returns>
    /// <exception cref="JetCorruptDataException">The creation date cannot supply a valid SID key.</exception>
    internal static byte[] GetOwnerPlaceholder(JetFormat format, byte[] rawHeader)
        => Encode(format, rawHeader, [0x02, 0x04]);

    /// <summary>Encodes a security identity for the specified native Jet4 header.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="rawHeader">The raw header.</param>
    /// <param name="plainIdentity">The unmasked identity.</param>
    /// <returns>The masked identity.</returns>
    /// <exception cref="JetCorruptDataException">The header or creation date cannot supply a SID key.</exception>
    internal static byte[] Encode(JetFormat format, byte[] rawHeader, ReadOnlySpan<byte> plainIdentity)
    {
        if (rawHeader.Length < 0x92)
        {
            throw new JetCorruptDataException("The Jet4 header is too short to derive its security identity key.");
        }

        byte[] header = (byte[])rawHeader.Clone();
        Span<byte> dateMask = stackalloc byte[4];
        Span<byte> keyBytes = stackalloc byte[4];
        try
        {
            format.TransformHeaderMask(header);
            double creationDate = BitConverter.Int64BitsToDouble(BinaryPrimitives.ReadInt64LittleEndian(header.AsSpan(0x72, 8)));
            if (double.IsNaN(creationDate) || double.IsInfinity(creationDate) || creationDate < int.MinValue || creationDate > int.MaxValue)
            {
                throw new JetCorruptDataException("The Jet4 creation date cannot derive its security identity key.");
            }

            BinaryPrimitives.WriteInt32LittleEndian(dateMask, (int)creationDate);
            uint key = BinaryPrimitives.ReadUInt32LittleEndian(header.AsSpan(0x72, 4));
            for (int index = 0; index < 40; index++)
            {
                int byteIndex = index * 2;
                byte value = header[0x42 + byteIndex];
                if (byteIndex < 40)
                {
                    value ^= dateMask[byteIndex % 4];
                }

                key ^= (uint)value << (index % 24);
            }

            BinaryPrimitives.WriteUInt32LittleEndian(keyBytes, key);
            byte[] identity = plainIdentity.ToArray();
            EncryptionManager.Rc4Transform(identity, 0, identity.Length, keyBytes);
            return identity;
        }
        finally
        {
            CryptographicOperations.ZeroMemory(header);
            CryptographicOperations.ZeroMemory(dateMask);
            CryptographicOperations.ZeroMemory(keyBytes);
        }
    }
}
