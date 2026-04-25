namespace JetDatabaseWriter;

using System;

/// <summary>
/// Payload supplied to
/// <see cref="IAccessWriter.AddAttachmentAsync(string, string, System.Collections.Generic.IReadOnlyDictionary{string, object}, AttachmentInput, System.Threading.CancellationToken)"/>
/// for one file attached to a parent row's complex (Attachment) column.
/// </summary>
/// <remarks>
/// Phase C4 of the complex-columns writer. See
/// <c>docs/design/complex-columns-format-notes.md</c> §3 for the wrapper-encoded
/// on-disk format the writer applies to <see cref="FileData"/>.
/// </remarks>
public sealed record AttachmentInput
{
    /// <summary>
    /// Initializes a new <see cref="AttachmentInput"/>.
    /// </summary>
    /// <param name="fileName">Display file name (with extension); non-empty.</param>
    /// <param name="fileData">Raw file bytes (uncompressed); non-null.</param>
    public AttachmentInput(string fileName, byte[] fileData)
    {
        if (string.IsNullOrEmpty(fileName))
        {
            throw new ArgumentException("File name is required.", nameof(fileName));
        }

        FileName = fileName;
        FileData = fileData ?? throw new ArgumentNullException(nameof(fileData));
    }

    /// <summary>Gets the display file name (e.g. <c>"invoice.pdf"</c>).</summary>
    public string FileName { get; }

    /// <summary>Gets the raw, uncompressed payload bytes.</summary>
    public byte[] FileData { get; }

    /// <summary>
    /// Gets the lowercase file extension without the leading <c>.</c>. When null,
    /// the extension is derived from <see cref="FileName"/>.
    /// </summary>
    public string? FileType { get; init; }

    /// <summary>Gets the optional source URL persisted in the flat-table <c>FileURL</c> column.</summary>
    public string? FileURL { get; init; }

    /// <summary>
    /// Gets the timestamp persisted in the flat-table <c>FileTimeStamp</c> column.
    /// Defaults to <see cref="DateTime.UtcNow"/> when null.
    /// </summary>
    public DateTime? FileTimeStamp { get; init; }
}
