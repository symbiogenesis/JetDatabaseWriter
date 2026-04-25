namespace JetDatabaseWriter;

using System;
using System.Diagnostics.CodeAnalysis;

/// <summary>
/// One attachment row decoded from the hidden flat child table of an
/// Access 2007+ Attachment column. Returned by
/// <see cref="IAccessReader.GetAttachmentsAsync(string, string, System.Threading.CancellationToken)"/>.
/// </summary>
/// <remarks>
/// The wrapper format (1 byte vs 4-byte type-flag, optional deflate compression)
/// is decoded by the reader; <see cref="FileData"/> is the raw payload bytes
/// after wrapper / deflate removal. See
/// <c>docs/design/complex-columns-format-notes.md</c> §3.
/// </remarks>
public sealed record AttachmentRecord
{
    /// <summary>
    /// Gets the per-parent-row <c>ConceptualTableID</c> joining this flat-table
    /// row back to its parent. Equal to the 4-byte value stored in the parent
    /// row's complex column slot.
    /// </summary>
    public int ConceptualTableId { get; init; }

    /// <summary>Gets the display file name from the flat table's <c>FileName</c> column.</summary>
    public string FileName { get; init; } = string.Empty;

    /// <summary>Gets the lowercase file extension from the flat table's <c>FileType</c> column.</summary>
    public string FileType { get; init; } = string.Empty;

    /// <summary>Gets the optional source URL from the flat table's <c>FileURL</c> column.</summary>
    [SuppressMessage("Design", "CA1056:URI-like properties should not be strings", Justification = "Mirrors the underlying Access flat-table column type, which is a free-text MEMO.")]
    public string? FileURL { get; init; }

    /// <summary>Gets the timestamp from the flat table's <c>FileTimeStamp</c> column.</summary>
    public DateTime? FileTimeStamp { get; init; }

    /// <summary>Gets the decoded raw file bytes (wrapper removed, deflate decompressed).</summary>
    [SuppressMessage("Performance", "CA1819:Properties should not return arrays", Justification = "Attachment payload is binary by definition; mirrors public byte[] surface elsewhere in the reader API.")]
    public byte[] FileData { get; init; } = Array.Empty<byte>();
}
