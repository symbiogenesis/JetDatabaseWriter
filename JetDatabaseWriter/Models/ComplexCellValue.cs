namespace JetDatabaseWriter.Models;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Interfaces;

/// <summary>
/// <para>
/// Decodes the value that the row-read APIs return for an Access 2007+ complex
/// column (Attachment, Multi-value, or Version-history). Complex columns report
/// <see cref="ColumnMetadata.ClrType"/> <c>byte[]</c>, so
/// <see cref="IAccessReader.Rows(string, IProgress{long}?, System.Threading.CancellationToken)"/>,
/// <see cref="IAccessReader.ReadTableAsync(string?, uint?, IProgress{long}?, System.Threading.CancellationToken)"/>
/// and the mapped <c>Rows&lt;T&gt;</c> / <c>ReadTableAsync&lt;T&gt;</c> reads put
/// every item of the parent row into one <c>byte[]</c> cell. Use
/// <see cref="ReadAttachments(byte[])"/> or <see cref="ReadMultiValueItems(byte[])"/>
/// to turn that cell back into the same records that
/// <see cref="IAccessReader.GetAttachmentsAsync(string, string, System.Threading.CancellationToken)"/>
/// and <see cref="IAccessReader.GetMultiValueItemsAsync(string, string, System.Threading.CancellationToken)"/>
/// return for the parent row. A parent row with no items reads as <see cref="DBNull"/>.
/// <see cref="IAccessReader.RowsAsStrings(string, IProgress{long}?, System.Threading.CancellationToken)"/>
/// and <see cref="IAccessReader.ReadTableAsStringsAsync(string, uint?, IProgress{long}?, System.Threading.CancellationToken)"/>
/// return the same bytes as a <c>data:application/octet-stream;base64,</c> URI, or an
/// empty string for a row with no items.
/// </para>
/// <para>
/// Cell layout (little-endian; strings are length-prefixed UTF-8 as written by
/// <see cref="BinaryWriter.Write(string)"/>):
/// <c>"JCX"</c>, one kind byte (<c>'A'</c> attachments, <c>'M'</c> multi-value or
/// version-history values), the <c>Int32</c> per-row complex reference, the
/// <c>Int32</c> item count, then each item. An attachment item is FileName,
/// FileType, a presence byte plus FileURL, a presence byte plus FileTimeStamp
/// (<see cref="DateTime.ToBinary"/>), and the <c>Int32</c> length and bytes of
/// the decoded (unwrapped and decompressed) file data. A multi-value item is a
/// type tag followed by the value: 0 null, 1 Boolean, 2 Byte, 3 Int16, 4 Int32,
/// 5 Int64, 6 Single, 7 Double, 8 Decimal, 9 Guid (16 bytes), 10 DateTime
/// (<see cref="DateTime.ToBinary"/>), 11 String, 12 byte[] (<c>Int32</c> length
/// and bytes). Values of any other type are stored as their invariant-culture string.
/// </para>
/// </summary>
public static class ComplexCellValue
{
    private const byte AttachmentKind = (byte)'A';
    private const byte MultiValueKind = (byte)'M';

    private const byte NullTag = 0;
    private const byte BooleanTag = 1;
    private const byte ByteTag = 2;
    private const byte Int16Tag = 3;
    private const byte Int32Tag = 4;
    private const byte Int64Tag = 5;
    private const byte SingleTag = 6;
    private const byte DoubleTag = 7;
    private const byte DecimalTag = 8;
    private const byte GuidTag = 9;
    private const byte DateTimeTag = 10;
    private const byte StringTag = 11;
    private const byte BytesTag = 12;

    private static readonly byte[] Magic = [(byte)'J', (byte)'C', (byte)'X'];

    /// <summary>
    /// Decodes an Attachment column cell into one <see cref="AttachmentRecord"/>
    /// per file attached to the parent row, in flat-table order.
    /// </summary>
    /// <param name="cell">The <c>byte[]</c> value read from an Attachment column.</param>
    /// <returns>The parent row's attachments, with decoded file data.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="cell"/> is <see langword="null"/>.</exception>
    /// <exception cref="FormatException"><paramref name="cell"/> is not an Attachment column cell.</exception>
    public static IReadOnlyList<AttachmentRecord> ReadAttachments(byte[] cell)
    {
        using BinaryReader reader = OpenCell(cell, AttachmentKind, out int conceptualTableId, out int count);
        try
        {
            var result = new List<AttachmentRecord>(count);
            for (int i = 0; i < count; i++)
            {
                string fileName = reader.ReadString();
                string fileType = reader.ReadString();
                string? fileUrl = reader.ReadBoolean() ? reader.ReadString() : null;
                DateTime? fileTimeStamp = reader.ReadBoolean() ? DateTime.FromBinary(reader.ReadInt64()) : null;
                byte[] fileData = ReadByteArray(reader);
                result.Add(new AttachmentRecord
                {
                    ConceptualTableId = conceptualTableId,
                    FileName = fileName,
                    FileType = fileType,
                    FileURL = fileUrl,
                    FileTimeStamp = fileTimeStamp,
                    FileData = fileData,
                });
            }

            return result;
        }
        catch (EndOfStreamException ex)
        {
            throw new FormatException("The complex column cell is truncated.", ex);
        }
    }

    /// <summary>
    /// Decodes a Multi-value (or Version-history) column cell into one
    /// <see cref="MultiValueItem"/> per value stored for the parent row, in flat-table order.
    /// </summary>
    /// <param name="cell">The <c>byte[]</c> value read from a Multi-value column.</param>
    /// <returns>The parent row's values.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="cell"/> is <see langword="null"/>.</exception>
    /// <exception cref="FormatException"><paramref name="cell"/> is not a Multi-value column cell.</exception>
    public static IReadOnlyList<MultiValueItem> ReadMultiValueItems(byte[] cell)
    {
        using BinaryReader reader = OpenCell(cell, MultiValueKind, out int conceptualTableId, out int count);
        try
        {
            var result = new List<MultiValueItem>(count);
            for (int i = 0; i < count; i++)
            {
                result.Add(new MultiValueItem
                {
                    ConceptualTableId = conceptualTableId,
                    Value = ReadValue(reader),
                });
            }

            return result;
        }
        catch (EndOfStreamException ex)
        {
            throw new FormatException("The complex column cell is truncated.", ex);
        }
    }

    /// <summary>Encodes the attachments of one parent row as a cell value.</summary>
    /// <param name="conceptualTableId">The parent row's complex reference.</param>
    /// <param name="attachments">The parent row's attachments.</param>
    internal static byte[] EncodeAttachments(int conceptualTableId, IReadOnlyList<AttachmentRecord> attachments)
    {
        using var stream = new MemoryStream();
        using (BinaryWriter writer = BeginCell(stream, AttachmentKind, conceptualTableId, attachments.Count))
        {
            foreach (AttachmentRecord attachment in attachments)
            {
                writer.Write(attachment.FileName ?? string.Empty);
                writer.Write(attachment.FileType ?? string.Empty);
                writer.Write(attachment.FileURL != null);
                if (attachment.FileURL != null)
                {
                    writer.Write(attachment.FileURL);
                }

                writer.Write(attachment.FileTimeStamp.HasValue);
                if (attachment.FileTimeStamp.HasValue)
                {
                    writer.Write(attachment.FileTimeStamp.Value.ToBinary());
                }

                byte[] data = attachment.FileData ?? [];
                writer.Write(data.Length);
                writer.Write(data);
            }
        }

        return stream.ToArray();
    }

    /// <summary>Encodes the multi-value items of one parent row as a cell value.</summary>
    /// <param name="conceptualTableId">The parent row's complex reference.</param>
    /// <param name="items">The parent row's values.</param>
    internal static byte[] EncodeMultiValueItems(int conceptualTableId, IReadOnlyList<MultiValueItem> items)
    {
        using var stream = new MemoryStream();
        using (BinaryWriter writer = BeginCell(stream, MultiValueKind, conceptualTableId, items.Count))
        {
            foreach (MultiValueItem item in items)
            {
                WriteValue(writer, item.Value);
            }
        }

        return stream.ToArray();
    }

    private static BinaryWriter BeginCell(MemoryStream stream, byte kind, int conceptualTableId, int count)
    {
        var writer = new BinaryWriter(stream, Encoding.UTF8, leaveOpen: true);
        writer.Write(Magic);
        writer.Write(kind);
        writer.Write(conceptualTableId);
        writer.Write(count);
        return writer;
    }

    private static BinaryReader OpenCell(byte[] cell, byte expectedKind, out int conceptualTableId, out int count)
    {
        Guard.NotNull(cell, nameof(cell));

        const int signatureSize = 4;
        const int headerSize = signatureSize + 8;
        if (cell.Length < headerSize
            || cell[0] != Magic[0]
            || cell[1] != Magic[1]
            || cell[2] != Magic[2])
        {
            throw new FormatException("The value is not a complex column cell.");
        }

        if (cell[3] != expectedKind)
        {
            string expected = expectedKind == AttachmentKind ? "an Attachment" : "a Multi-value";
            throw new FormatException($"The complex column cell is not {expected} column cell.");
        }

        var reader = new BinaryReader(new MemoryStream(cell, signatureSize, cell.Length - signatureSize, writable: false), Encoding.UTF8);
        conceptualTableId = reader.ReadInt32();
        count = reader.ReadInt32();
        if (count < 0)
        {
            reader.Dispose();
            throw new FormatException("The complex column cell has a negative item count.");
        }

        return reader;
    }

    private static byte[] ReadByteArray(BinaryReader reader)
    {
        int length = reader.ReadInt32();
        if (length < 0)
        {
            throw new FormatException("The complex column cell has a negative byte length.");
        }

        byte[] bytes = reader.ReadBytes(length);
        return bytes.Length == length ? bytes : throw new EndOfStreamException();
    }

    private static void WriteValue(BinaryWriter writer, object? value)
    {
        switch (value)
        {
            case null or DBNull:
                writer.Write(NullTag);
                break;
            case bool b:
                writer.Write(BooleanTag);
                writer.Write(b);
                break;
            case byte b:
                writer.Write(ByteTag);
                writer.Write(b);
                break;
            case short s:
                writer.Write(Int16Tag);
                writer.Write(s);
                break;
            case int i:
                writer.Write(Int32Tag);
                writer.Write(i);
                break;
            case long l:
                writer.Write(Int64Tag);
                writer.Write(l);
                break;
            case float f:
                writer.Write(SingleTag);
                writer.Write(f);
                break;
            case double d:
                writer.Write(DoubleTag);
                writer.Write(d);
                break;
            case decimal m:
                writer.Write(DecimalTag);
                writer.Write(m);
                break;
            case Guid g:
                writer.Write(GuidTag);
                writer.Write(g.ToByteArray());
                break;
            case DateTime dt:
                writer.Write(DateTimeTag);
                writer.Write(dt.ToBinary());
                break;
            case string s:
                writer.Write(StringTag);
                writer.Write(s);
                break;
            case byte[] bytes:
                writer.Write(BytesTag);
                writer.Write(bytes.Length);
                writer.Write(bytes);
                break;
            default:
                writer.Write(StringTag);
                writer.Write(Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty);
                break;
        }
    }

    private static object? ReadValue(BinaryReader reader)
    {
        byte tag = reader.ReadByte();
        return tag switch
        {
            NullTag => null,
            BooleanTag => reader.ReadBoolean(),
            ByteTag => reader.ReadByte(),
            Int16Tag => reader.ReadInt16(),
            Int32Tag => reader.ReadInt32(),
            Int64Tag => reader.ReadInt64(),
            SingleTag => reader.ReadSingle(),
            DoubleTag => reader.ReadDouble(),
            DecimalTag => reader.ReadDecimal(),
            GuidTag => new Guid(ReadExactly(reader, 16)),
            DateTimeTag => DateTime.FromBinary(reader.ReadInt64()),
            StringTag => reader.ReadString(),
            BytesTag => ReadByteArray(reader),
            _ => throw new FormatException($"The complex column cell has an unknown value tag {tag.ToString(CultureInfo.InvariantCulture)}."),
        };
    }

    private static byte[] ReadExactly(BinaryReader reader, int length)
    {
        byte[] bytes = reader.ReadBytes(length);
        return bytes.Length == length ? bytes : throw new EndOfStreamException();
    }
}
