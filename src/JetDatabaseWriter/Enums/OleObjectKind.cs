namespace JetDatabaseWriter.Enums;

using JetDatabaseWriter.Models;

/// <summary>
/// What an OLE Object column value holds, as <see cref="OleObjectValue.Parse(byte[])"/>
/// finds it in the value's stored bytes.
/// </summary>
public enum OleObjectKind
{
    /// <summary>
    /// The value does not start with Access's OLE header (<c>15 1C</c>), such as a file a
    /// program stored as is. <see cref="OleObjectContent.Content"/> is the stored bytes.
    /// </summary>
    NotWrapped = 0,

    /// <summary>
    /// An OLE Package that embeds a file (Package type 3), as Access stores a file
    /// inserted into an OLE Object field. <see cref="OleObjectContent.Content"/> is the
    /// file's bytes, <see cref="OleObjectContent.FileName"/> its name and
    /// <see cref="OleObjectContent.SourcePath"/> the path it was inserted from.
    /// </summary>
    EmbeddedFile = 1,

    /// <summary>
    /// A link to a file: an OLE Package of type 1, or a linked OLE object (MS-OLEDS
    /// <c>FormatID</c> 1). <see cref="OleObjectContent.Content"/> is empty and
    /// <see cref="OleObjectContent.SourcePath"/> names the linked file.
    /// </summary>
    LinkedFile = 2,

    /// <summary>
    /// An embedded OLE object other than a package, such as a Word document or an Excel
    /// sheet. <see cref="OleObjectContent.Content"/> is the object's native data
    /// (MS-OLEDS <c>NativeData</c>), without the presentation data Access stores after it.
    /// </summary>
    EmbeddedObject = 3,

    /// <summary>
    /// The value starts with Access's OLE header, but the structure behind it is
    /// truncated or inconsistent. <see cref="OleObjectContent.Content"/> is the stored bytes.
    /// </summary>
    Unknown = 4,
}
