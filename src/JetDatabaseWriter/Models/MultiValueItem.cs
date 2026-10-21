namespace JetDatabaseWriter.Models;

using System;
using JetDatabaseWriter.Interfaces;

/// <summary>
/// One row decoded from the hidden flat child table of an Access 2007+
/// Multi-Value or Version-history column. Returned by
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

    /// <summary>
    /// Gets the typed value from the flat table's value column. For a
    /// version-history column, the text of one version of the append-only
    /// Memo column.
    /// </summary>
    public object? Value { get; init; }

    /// <summary>
    /// Gets, for a version-history column, when Access recorded this version:
    /// the flat table's <c>Modified_&lt;GUID&gt;</c> Date/Time column.
    /// <see langword="null"/> for a multi-value column.
    /// </summary>
    public DateTime? Modified { get; init; }
}
