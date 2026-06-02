namespace JetDatabaseWriter.Models;

using JetDatabaseWriter.Interfaces;

/// <summary>
/// One multi-value row decoded from the hidden flat child table of an
/// Access 2007+ Multi-Value column. Returned by
/// <see cref="IAccessReader.GetMultiValueItemsAsync(string, string, System.Threading.CancellationToken)"/>.
/// </summary>
public sealed record MultiValueItem
{
    /// <summary>
    /// Gets the per-parent-row complex reference joining this flat-table
    /// row back to its parent. Equal to the 4-byte value stored in the parent
    /// row's complex column slot.
    /// </summary>
    public int ConceptualTableId { get; init; }

    /// <summary>Gets the typed value from the flat table's value column.</summary>
    public object? Value { get; init; }
}
