namespace JetDatabaseWriter.Benchmarks.Models;

/// <summary>
/// DTO bound to the synthetic <c>LargeLongValues</c> table (see
/// <see cref="Infrastructure.SyntheticDatabases"/>): an int, a MEMO and an OLE
/// column whose large values span more LVAL pages than the default page cache.
/// Used by the large long-value benchmarks.
/// </summary>
public sealed class LargeLongValueRow
{
    public int Id { get; set; }

    public string? Body { get; set; }

#pragma warning disable CA1819 // Rows<T> maps an OLE column to a byte[] property; the benchmark measures that decode.
    public byte[]? Blob { get; set; }
#pragma warning restore CA1819
}
