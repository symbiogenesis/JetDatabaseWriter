namespace JetDatabaseWriter.Catalog;

using System;
using System.Buffers.Binary;
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
    {
        byte[] header = (byte[])rawHeader.Clone();
        format.TransformHeaderMask(header);
        double creationDate = BitConverter.Int64BitsToDouble(BinaryPrimitives.ReadInt64LittleEndian(header.AsSpan(0x72, 8)));
        if (double.IsNaN(creationDate) || double.IsInfinity(creationDate) || creationDate < int.MinValue || creationDate > int.MaxValue)
        {
            throw new JetCorruptDataException("The Jet4 creation date cannot derive its security identity key.");
        }

        byte[] dateMask = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(dateMask, (int)creationDate);
        uint key = BinaryPrimitives.ReadUInt32LittleEndian(header.AsSpan(0x72, 4));
        for (int index = 0; index < 40; index++)
        {
            byte value = header[0x42 + (index * 2)];
            if (index * 2 < 40)
            {
                value ^= dateMask[(index * 2) % 4];
            }

            key ^= (uint)value << (index % 24);
        }

        byte[] keyBytes = new byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(keyBytes, key);
        byte[] identity = [0x70, 0x15];
        EncryptionManager.Rc4Transform(identity, 0, identity.Length, keyBytes);
        return identity;
    }
}
