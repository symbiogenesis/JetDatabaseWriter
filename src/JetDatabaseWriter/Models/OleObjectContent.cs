namespace JetDatabaseWriter.Models;

using System.Diagnostics.CodeAnalysis;
using JetDatabaseWriter.Enums;

/// <summary>
/// The content of an OLE Object column value, as <see cref="OleObjectValue.Parse(byte[])"/>
/// unwraps it from the stored bytes. Reading the column returns the stored bytes; this
/// record describes what they hold.
/// </summary>
public sealed record OleObjectContent
{
    /// <summary>Gets what the value holds.</summary>
    public OleObjectKind Kind { get; init; }

    /// <summary>
    /// Gets the display name in Access's OLE header, such as <c>Packager Shell Object</c>
    /// or <c>Document</c>, or <see langword="null"/> when the value has none.
    /// </summary>
    public string? DisplayName { get; init; }

    /// <summary>
    /// Gets the OLE class of the object, such as <c>Package</c> or <c>Word.Document.8</c>,
    /// or <see langword="null"/> when the value has none.
    /// </summary>
    public string? ClassName { get; init; }

    /// <summary>
    /// Gets the name of the embedded or linked file of an OLE Package (its label), or
    /// <see langword="null"/> for any other value.
    /// </summary>
    public string? FileName { get; init; }

    /// <summary>
    /// Gets the path of the file an OLE Package was made from or links to, or the source
    /// of a linked OLE object, or <see langword="null"/> when the value names none.
    /// </summary>
    public string? SourcePath { get; init; }

    /// <summary>
    /// Gets the unwrapped content: the embedded file for <see cref="OleObjectKind.EmbeddedFile"/>,
    /// the native data for <see cref="OleObjectKind.EmbeddedObject"/>, an empty array for
    /// <see cref="OleObjectKind.LinkedFile"/>, and the stored bytes themselves for
    /// <see cref="OleObjectKind.NotWrapped"/> and <see cref="OleObjectKind.Unknown"/>.
    /// </summary>
    [SuppressMessage("Performance", "CA1819:Properties should not return arrays", Justification = "The unwrapped OLE content is binary data; a byte[] matches the column's CLR type.")]
    public byte[] Content { get; init; } = [];

    /// <summary>
    /// Gets the media type of <see cref="Content"/> from a file signature at its first
    /// byte (see <see cref="OleObjectValue.DetectMediaType(System.ReadOnlySpan{byte})"/>), or
    /// <see langword="null"/> when no known signature starts it.
    /// </summary>
    public string? MediaType { get; init; }
}
