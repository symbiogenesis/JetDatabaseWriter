namespace JetDatabaseWriter;

/// <summary>
/// One column participating in a JET index, in key order.
/// </summary>
public sealed record IndexColumnReference
{
    /// <summary>Gets the name of the column. Empty when the column number does not resolve to a known column.</summary>
    public string Name { get; init; } = string.Empty;

    /// <summary>Gets the absolute column number (<c>col_num</c>) recorded in the index col_map slot.</summary>
    public int ColumnNumber { get; init; }

    /// <summary>
    /// Gets a value indicating whether this column is sorted ascending in the index
    /// (<c>col_order</c> == <c>0x01</c>). Set to <see langword="false"/> for the rare
    /// descending case (<c>col_order</c> != <c>0x01</c>).
    /// </summary>
    public bool IsAscending { get; init; } = true;
}
