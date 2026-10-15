namespace JetDatabaseWriter;

using System;
using JetDatabaseWriter.Enums;
using static JetDatabaseWriter.Schema.JetTypeInfo;

internal sealed partial class JetFormat
{
    /// <summary>Gets the page-zero layout, also available before a masked header can create a profile.</summary>
    internal DatabaseHeaderLayout Header => HeaderLayoutOf(this.Kind);

    /// <summary>Returns header facts without constructing codecs or reading masked fields.</summary>
    /// <param name="format">The classified database format.</param>
    /// <returns>The native header layout.</returns>
    internal static DatabaseHeaderLayout HeaderLayoutOf(DatabaseFormat format)
        => format == DatabaseFormat.Jet3Mdb ? DatabaseHeaderLayout.Jet3 : DatabaseHeaderLayout.Jet4;

    /// <summary>Initializes and masks the page-zero bytes of a newly allocated bootstrap image.</summary>
    /// <param name="database">The zero-initialized bootstrap image.</param>
    internal void InitializeNewHeader(byte[] database)
    {
        database[1] = 0x01;
        this.HeaderSignature.CopyTo(database.AsSpan(4));
        database[0x14] = this.NewDatabaseVersion;
        if (this.IsJet3)
        {
            WriteJet3HeaderDefaults(database);
        }
        else
        {
            WriteJet4AceHeaderDefaults(database);

            // Each two-byte user slot ends with 1 when available.
            for (int offset = 0xE01; offset < this.PageSize; offset += 2)
            {
                database[offset] = 1;
            }
        }

        this.TransformHeaderMask(database);
    }

    /// <summary>
    /// Writes the unmasked page-0 fields Access 97 writes in a new database,
    /// as found in the Access 97 fixtures: code page 1252 and sort order 0x0409.
    /// The encoding key and the 20-byte password area stay zero, so the file
    /// is unencrypted and has no password. The 2 bytes at 0x56, which vary from
    /// file to file and whose meaning is unknown, stay zero.
    /// </summary>
    /// <param name="db">The database bytes whose header is initialized.</param>
    private static void WriteJet3HeaderDefaults(byte[] db)
    {
        db[0x19] = 0x01;
        db[0x1C] = 0x01;
        db[0x1D] = 0x01;
        Wi32(db, 0x20, 2);
        Wi32(db, 0x24, 3);
        Wi32(db, 0x28, 4);
        Wi32(db, 0x2C, 5);
        db[0x38] = 0x01;
        Wu16(db, Constants.DatabaseHeader.Jet3SortOrder, 0x0409);
        Wu16(db, Constants.DatabaseHeader.CodePage, 1252);
    }

    private static void WriteJet4AceHeaderDefaults(byte[] db)
    {
        db[0x19] = 0x01;
        db[0x1C] = 0x01;
        db[0x1D] = 0x01;
        Wi32(db, 0x20, 2);
        Wi32(db, 0x24, 3);
        Wi32(db, 0x28, 4);
        Wi32(db, 0x2C, 5);
        Wu16(db, 0x3C, 1252);

        for (int offset = 0x42; offset < 0x6A; offset += 4)
        {
            db[offset] = 0x53;
            db[offset + 1] = 0xB4;
        }

        Wu16(db, 0x6A, 0x11A6);
        Wu16(db, 0x6E, 0x0409);

        ReadOnlySpan<byte> creationDate = [0x08, 0x6E, 0x41, 0x1D, 0x7C, 0x8A, 0xE6, 0x40];
        creationDate.CopyTo(db.AsSpan(0x72));

        Wi32(db, 0x98, 0x0654);
        db[0x9C] = (byte)'4';
        db[0x9D] = (byte)'.';
        db[0x9E] = (byte)'0';
    }
}

/// <summary>Page-zero fields whose widths or locations vary between supported formats.</summary>
/// <param name="MaskLength">The fixed-key RC4 mask length.</param>
/// <param name="PasswordLength">The password area length.</param>
/// <param name="SortOrderOffset">The default sort-order word offset.</param>
/// <param name="SortVersionOffset">The default sort-version byte offset, or -1 when absent.</param>
/// <param name="UsesCreationDatePassword">Whether an empty password repeats creation-date days.</param>
internal readonly record struct DatabaseHeaderLayout(
    int MaskLength,
    int PasswordLength,
    int SortOrderOffset,
    int SortVersionOffset,
    bool UsesCreationDatePassword)
{
    /// <summary>Gets the Access97 header layout.</summary>
    internal static DatabaseHeaderLayout Jet3 => new(Constants.DatabaseHeader.Jet3MaskLength, Constants.DatabaseHeader.Jet3PasswordLength, Constants.DatabaseHeader.Jet3SortOrder, -1, false);

    /// <summary>Gets the Jet4/ACE header layout.</summary>
    internal static DatabaseHeaderLayout Jet4 => new(Constants.DatabaseHeader.MaskLength, Constants.DatabaseHeader.PasswordLength, 0x6E, 0x71, true);
}
