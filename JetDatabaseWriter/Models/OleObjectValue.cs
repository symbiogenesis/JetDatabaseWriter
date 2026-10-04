namespace JetDatabaseWriter.Models;

using System;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.ValueDecoding;

/// <summary>
/// Unwraps OLE Object column values. Every read API returns an OLE Object value as
/// its stored bytes, as DAO, ADO and Jackcess do: an object Access inserted keeps
/// Access's OLE header, the MS-OLEDS object stream and its presentation data. This
/// class parses that structure on request, without guessing: each length is checked
/// against the bytes that are there, and a value it cannot follow is reported as
/// <see cref="Enums.OleObjectKind.Unknown"/> with the stored bytes as its content.
/// </summary>
/// <remarks>
/// The layout is Access's OLE header (<c>15 1C</c>, header size, object type, display
/// and class names), then an MS-OLEDS <c>ObjectHeader</c> (version <c>0x0501</c>, a
/// <c>FormatID</c> of 2 for an embedded and 1 for a linked object, and the class, topic
/// and item names), then for an embedded object its native data. An OLE Package (class
/// <c>Package</c>) holds a label, a source path and either an embedded file (type 3) or
/// a link (type 1) in its native data. Only the object's presentation data and Access's
/// 4-byte trailer follow the native data, and unwrapping does not need them: a value cut
/// right after the native data still unwraps, while one cut before its end is
/// <see cref="Enums.OleObjectKind.Unknown"/> (or <see cref="Enums.OleObjectKind.NotWrapped"/>
/// when cut before the <c>15 1C</c> signature).
/// </remarks>
public static class OleObjectValue
{
    /// <summary>
    /// Parses the stored bytes of an OLE Object value. Never throws for any content.
    /// </summary>
    /// <param name="stored">The value as a read API returns it.</param>
    /// <returns>What the value holds; see <see cref="OleObjectContent"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="stored"/> is <see langword="null"/>.</exception>
    public static OleObjectContent Parse(byte[] stored)
    {
        Guard.NotNull(stored, nameof(stored));
        return OleObjectDecoder.Parse(stored);
    }

    /// <summary>
    /// Returns the unwrapped content of an OLE Object value: the embedded file of an OLE
    /// Package, the native data of another embedded object, an empty array for a link,
    /// and the stored bytes for a value that is not wrapped or cannot be parsed.
    /// </summary>
    /// <param name="stored">The value as a read API returns it.</param>
    /// <returns><see cref="OleObjectContent.Content"/> of <see cref="Parse(byte[])"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="stored"/> is <see langword="null"/>.</exception>
    public static byte[] GetContent(byte[] stored) => Parse(stored).Content;

    /// <summary>
    /// Returns the media type that a file signature at the first byte of
    /// <paramref name="bytes"/> identifies: <c>image/jpeg</c>, <c>image/png</c>,
    /// <c>image/gif</c>, <c>image/bmp</c>, <c>image/tiff</c>, <c>application/pdf</c>,
    /// <c>application/zip</c> (also Office Open XML), <c>application/msword</c> (any OLE
    /// compound file) or <c>application/rtf</c>. Returns <see langword="null"/> when no
    /// known signature starts the bytes; a signature later in the bytes is not looked for.
    /// </summary>
    /// <param name="bytes">The bytes to look at.</param>
    /// <returns>The media type, or <see langword="null"/>.</returns>
    public static string? DetectMediaType(ReadOnlySpan<byte> bytes) => OleObjectDecoder.DetectMediaType(bytes);
}
