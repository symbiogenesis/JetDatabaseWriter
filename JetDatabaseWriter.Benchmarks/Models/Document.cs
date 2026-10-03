namespace JetDatabaseWriter.Benchmarks.Models;

/// <summary>
/// DTO bound to the synthetic <c>Documents</c> table (see
/// <see cref="Infrastructure.SyntheticDatabases"/>): an int key and an Attachment
/// column, whose cell <see cref="JetDatabaseWriter.Models.ComplexCellValue.ReadAttachments(byte[])"/> decodes.
/// </summary>
internal sealed class Document
{
    public int Id { get; set; }

    public byte[]? Files { get; set; }
}
