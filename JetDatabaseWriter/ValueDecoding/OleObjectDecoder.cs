namespace JetDatabaseWriter.ValueDecoding;

using System;
using System.Buffers.Binary;
using System.Text;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;

/// <summary>
/// The structured OLE Object parser behind <see cref="OleObjectValue"/>, the media-type
/// detection, and the <c>data:</c> URI the string read APIs render an OLE value as.
/// Read APIs return OLE values as their stored bytes; nothing here runs on a read
/// unless the caller asks for it, except the data URI, which renders the stored bytes.
/// </summary>
/// <remarks>
/// Every length is read as an unsigned value and checked against the bytes that remain
/// before it is used, so a truncated or inconsistent value is reported as
/// <see cref="OleObjectKind.Unknown"/> instead of throwing. The layouts are in
/// <see cref="OleObjectValue"/>'s remarks; they were checked against the
/// Access-authored testOleV2007 and test2 fixtures.
/// </remarks>
internal static class OleObjectDecoder
{
    /// <summary>The media type a data URI gets when no file signature starts the bytes.</summary>
    internal const string DefaultMediaType = "application/octet-stream";

    private const ushort AccessOleSignature = 0x1C15;
    private const int AccessHeaderMinLength = 20;
    private const uint OleVersion = 0x00000501;
    private const uint LinkedFormatId = 1;
    private const uint EmbeddedFormatId = 2;
    private const ushort PackageSignature = 2;
    private const ushort PackageLinkedFile = 1;
    private const ushort PackageEmbeddedFile = 3;
    private const string PackageClassName = "Package";

    /// <summary>The ANSI code page Access writes OLE header and package strings in.</summary>
    private static readonly Encoding Ansi = CreateAnsiEncoding();

    /// <summary>
    /// Parses the stored bytes of an OLE Object value; see <see cref="OleObjectValue.Parse(byte[])"/>.
    /// </summary>
    /// <param name="stored">The stored bytes.</param>
    internal static OleObjectContent Parse(byte[] stored)
    {
        ReadOnlySpan<byte> value = stored;
        if (value.Length < 2 || BinaryPrimitives.ReadUInt16LittleEndian(value) != AccessOleSignature)
        {
            return Raw(stored, OleObjectKind.NotWrapped, displayName: null, className: null);
        }

        if (value.Length < AccessHeaderMinLength)
        {
            return Raw(stored, OleObjectKind.Unknown, displayName: null, className: null);
        }

        int headerSize = BinaryPrimitives.ReadUInt16LittleEndian(value[2..]);
        if (headerSize < AccessHeaderMinLength || headerSize > value.Length)
        {
            return Raw(stored, OleObjectKind.Unknown, displayName: null, className: null);
        }

        ReadOnlySpan<byte> header = value[..headerSize];
        string? displayName = ReadHeaderString(header, BinaryPrimitives.ReadUInt16LittleEndian(value[12..]), BinaryPrimitives.ReadUInt16LittleEndian(value[8..]));
        string? headerClassName = ReadHeaderString(header, BinaryPrimitives.ReadUInt16LittleEndian(value[14..]), BinaryPrimitives.ReadUInt16LittleEndian(value[10..]));

        // The MS-OLEDS ObjectHeader follows Access's header.
        var cursor = new Cursor(value, headerSize);
        if (!cursor.TryReadUInt32(out uint version)
            || version != OleVersion
            || !cursor.TryReadUInt32(out uint formatId)
            || !cursor.TryReadLengthPrefixedAnsi(out string className)
            || !cursor.TryReadLengthPrefixedAnsi(out string topicName)
            || !cursor.TryReadLengthPrefixedAnsi(out _))
        {
            return Raw(stored, OleObjectKind.Unknown, displayName, headerClassName);
        }

        string? objectClass = className.Length > 0 ? className : headerClassName;
        if (formatId == LinkedFormatId)
        {
            return topicName.Length == 0
                ? Raw(stored, OleObjectKind.Unknown, displayName, objectClass)
                : new OleObjectContent { Kind = OleObjectKind.LinkedFile, DisplayName = displayName, ClassName = objectClass, SourcePath = topicName };
        }

        if (formatId != EmbeddedFormatId
            || !cursor.TryReadUInt32(out uint nativeSize)
            || !cursor.TryReadBytes(nativeSize, out ReadOnlySpan<byte> native))
        {
            return Raw(stored, OleObjectKind.Unknown, displayName, objectClass);
        }

        if (!string.Equals(objectClass, PackageClassName, StringComparison.OrdinalIgnoreCase))
        {
            byte[] nativeData = native.ToArray();
            return new OleObjectContent
            {
                Kind = OleObjectKind.EmbeddedObject,
                DisplayName = displayName,
                ClassName = objectClass,
                Content = nativeData,
                MediaType = DetectMediaType(nativeData),
            };
        }

        return ParsePackage(native, displayName, objectClass) ?? Raw(stored, OleObjectKind.Unknown, displayName, objectClass);
    }

    /// <summary>
    /// Returns the media type a file signature at the first byte identifies; see
    /// <see cref="OleObjectValue.DetectMediaType(ReadOnlySpan{byte})"/>.
    /// </summary>
    /// <param name="bytes">The bytes to look at.</param>
    internal static string? DetectMediaType(ReadOnlySpan<byte> bytes)
    {
        if (bytes.StartsWith(Constants.OleMagicBytes.Jpeg))
        {
            return "image/jpeg";
        }

        if (bytes.StartsWith(Constants.OleMagicBytes.Png))
        {
            return "image/png";
        }

        if (bytes.StartsWith(Constants.OleMagicBytes.Gif))
        {
            return "image/gif";
        }

        if (bytes.StartsWith(Constants.OleMagicBytes.Bmp))
        {
            return "image/bmp";
        }

        if (bytes.StartsWith(Constants.OleMagicBytes.TiffLittleEndian) || bytes.StartsWith(Constants.OleMagicBytes.TiffBigEndian))
        {
            return "image/tiff";
        }

        if (bytes.StartsWith(Constants.OleMagicBytes.Pdf))
        {
            return "application/pdf";
        }

        if (bytes.StartsWith(Constants.OleMagicBytes.Zip))
        {
            return "application/zip";
        }

        if (bytes.StartsWith(Constants.OleMagicBytes.OleCompound))
        {
            return "application/msword";
        }

        return bytes.StartsWith(Constants.OleMagicBytes.Rtf) ? "application/rtf" : null;
    }

    /// <summary>
    /// Renders stored OLE bytes as an RFC 2397 <c>data:</c> URI: the media type from a
    /// signature at the first byte (<see cref="DetectMediaType"/>), else
    /// <see cref="DefaultMediaType"/>, and every stored byte in base64.
    /// </summary>
    /// <param name="buffer">The buffer holding the value.</param>
    /// <param name="offset">The value's offset in <paramref name="buffer"/>.</param>
    /// <param name="length">The value's length.</param>
    internal static string ToDataUri(byte[] buffer, int offset, int length)
        => "data:" + (DetectMediaType(buffer.AsSpan(offset, length)) ?? DefaultMediaType) + ";base64," + Convert.ToBase64String(buffer, offset, length);

    /// <summary>
    /// Parses an OLE Package's native data: <c>u16 2</c>, the label and the source path
    /// (ANSI, NUL-terminated), the icon index and the type (<c>u16</c> each), then for
    /// type 3 the temporary path and the file (<c>u32</c> length each), and for type 1
    /// a <c>u16</c> and the linked path. Returns <see langword="null"/> when the data does
    /// not follow that layout.
    /// </summary>
    /// <param name="native">The package's native data.</param>
    /// <param name="displayName">The display name from Access's header.</param>
    /// <param name="className">The object's class.</param>
    private static OleObjectContent? ParsePackage(ReadOnlySpan<byte> native, string? displayName, string? className)
    {
        var cursor = new Cursor(native, 0);
        if (!cursor.TryReadUInt16(out ushort signature)
            || signature != PackageSignature
            || !cursor.TryReadAnsiZ(out string label)
            || !cursor.TryReadAnsiZ(out string sourcePath)
            || !cursor.TryReadUInt16(out _)
            || !cursor.TryReadUInt16(out ushort type))
        {
            return null;
        }

        if (type == PackageEmbeddedFile)
        {
            if (!cursor.TryReadUInt32(out uint pathLength)
                || !cursor.TryReadBytes(pathLength, out _)
                || !cursor.TryReadUInt32(out uint dataLength)
                || !cursor.TryReadBytes(dataLength, out ReadOnlySpan<byte> data))
            {
                return null;
            }

            byte[] file = data.ToArray();
            return new OleObjectContent
            {
                Kind = OleObjectKind.EmbeddedFile,
                DisplayName = displayName,
                ClassName = className,
                FileName = label,
                SourcePath = sourcePath,
                Content = file,
                MediaType = DetectMediaType(file),
            };
        }

        if (type == PackageLinkedFile && cursor.TryReadUInt16(out _) && cursor.TryReadAnsiZ(out string linkPath))
        {
            return new OleObjectContent
            {
                Kind = OleObjectKind.LinkedFile,
                DisplayName = displayName,
                ClassName = className,
                FileName = label,
                SourcePath = linkPath.Length > 0 ? linkPath : sourcePath,
            };
        }

        return null;
    }

    private static OleObjectContent Raw(byte[] stored, OleObjectKind kind, string? displayName, string? className) => new()
    {
        Kind = kind,
        DisplayName = displayName,
        ClassName = className,
        Content = stored,
        MediaType = DetectMediaType(stored),
    };

    /// <summary>
    /// Reads a NUL-terminated ANSI string that Access's header places at
    /// <paramref name="offset"/>, or <see langword="null"/> when it does not fit the header.
    /// </summary>
    /// <param name="header">Access's OLE header.</param>
    /// <param name="offset">The string's offset.</param>
    /// <param name="length">The string's length, terminator included.</param>
    private static string? ReadHeaderString(ReadOnlySpan<byte> header, int offset, int length)
    {
        if (length == 0 || offset < AccessHeaderMinLength || offset > header.Length - length)
        {
            return null;
        }

        ReadOnlySpan<byte> text = header.Slice(offset, length);
        int terminator = text.IndexOf((byte)0);
        return Ansi.GetString(terminator >= 0 ? text[..terminator] : text);
    }

    private static Encoding CreateAnsiEncoding()
    {
        Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
        return Encoding.GetEncoding(1252);
    }

    /// <summary>A bounds-checked reader over a span; a failed read leaves the position where it was.</summary>
    private ref struct Cursor
    {
        private readonly ReadOnlySpan<byte> data;
        private int position;

        internal Cursor(ReadOnlySpan<byte> data, int position)
        {
            this.data = data;
            this.position = position;
        }

        private readonly int Remaining => this.data.Length - this.position;

        internal bool TryReadUInt16(out ushort value)
        {
            value = 0;
            if (this.Remaining < sizeof(ushort))
            {
                return false;
            }

            value = BinaryPrimitives.ReadUInt16LittleEndian(this.data[this.position..]);
            this.position += sizeof(ushort);
            return true;
        }

        internal bool TryReadUInt32(out uint value)
        {
            value = 0;
            if (this.Remaining < sizeof(uint))
            {
                return false;
            }

            value = BinaryPrimitives.ReadUInt32LittleEndian(this.data[this.position..]);
            this.position += sizeof(uint);
            return true;
        }

        internal bool TryReadBytes(uint length, out ReadOnlySpan<byte> bytes)
        {
            bytes = default;
            if (length > (uint)this.Remaining)
            {
                return false;
            }

            bytes = this.data.Slice(this.position, (int)length);
            this.position += (int)length;
            return true;
        }

        /// <summary>Reads an MS-OLEDS <c>LengthPrefixedAnsiString</c>: a <c>u32</c> length, terminator included, then the bytes.</summary>
        /// <param name="value">The string, without its terminator; empty for a zero length.</param>
        internal bool TryReadLengthPrefixedAnsi(out string value)
        {
            value = string.Empty;
            int start = this.position;
            if (!this.TryReadUInt32(out uint length) || !this.TryReadBytes(length, out ReadOnlySpan<byte> bytes))
            {
                this.position = start;
                return false;
            }

            int terminator = bytes.IndexOf((byte)0);
            value = Ansi.GetString(terminator >= 0 ? bytes[..terminator] : bytes);
            return true;
        }

        /// <summary>Reads a NUL-terminated ANSI string.</summary>
        /// <param name="value">The string, without its terminator.</param>
        internal bool TryReadAnsiZ(out string value)
        {
            value = string.Empty;
            int terminator = this.data[this.position..].IndexOf((byte)0);
            if (terminator < 0)
            {
                return false;
            }

            value = Ansi.GetString(this.data.Slice(this.position, terminator));
            this.position += terminator + 1;
            return true;
        }
    }
}
