namespace JetDatabaseWriter.Pages.Models;

using System.Collections.Generic;
using JetDatabaseWriter.LongValues.Models;

/// <summary>
/// One live row of a table decoded from the data page at <see cref="Location"/>.
/// Writer workflows that change rows they have read mutate exactly this
/// location, so a row that cannot be decoded is left alone rather than shifting
/// which location every later row is paired with.
/// </summary>
/// <param name="Location">Where the row was decoded from.</param>
/// <param name="Values">The row's values in table-column order, with <see cref="System.DBNull.Value"/> for nulls.</param>
internal readonly record struct LocatedRow(RowLocation Location, object[] Values)
{
    /// <summary>Gets original Text slots, including fixed-slot bytes under null values.</summary>
    internal IReadOnlyDictionary<int, byte[]>? TextBytes { get; init; }

    /// <summary>Gets original long-value slots, retaining descriptors and their existing payload chains.</summary>
    internal IReadOnlyDictionary<int, byte[]>? LongValues { get; init; }

    /// <summary>Selects unchanged long-value slots that this mutation did not assign.</summary>
    /// <param name="newValues">The composed replacement values.</param>
    /// <param name="assignedColumns">Columns explicitly assigned by the mutation.</param>
    internal IReadOnlyDictionary<int, byte[]>? GetPreservedLongValues(object[] newValues, IEnumerable<int>? assignedColumns = null)
    {
        Dictionary<int, byte[]>? preserved = this.PreserveSlots(this.LongValues, newValues, assignedColumns);
        if (this.LongValues is not null && preserved is not null)
        {
            foreach ((int column, byte[] bytes) in this.LongValues)
            {
                if (!preserved.ContainsKey(column) && LongValueDescriptor.TryRead(bytes, out LongValueDescriptor descriptor) && descriptor.IsExternal)
                {
                    // Mixed retained/replaced external roots need complete incoming
                    // ownership before selective reclamation can be enabled.
                    return null;
                }
            }
        }

        return preserved;
    }

    /// <summary>Selects unchanged Text slots that this mutation did not assign.</summary>
    /// <param name="newValues">The composed replacement values.</param>
    /// <param name="assignedColumns">Columns explicitly assigned by the mutation.</param>
    internal IReadOnlyDictionary<int, byte[]>? GetPreservedText(object[] newValues, IEnumerable<int>? assignedColumns = null)
        => this.PreserveSlots(this.TextBytes, newValues, assignedColumns);

    private Dictionary<int, byte[]>? PreserveSlots(IReadOnlyDictionary<int, byte[]>? slots, object[] newValues, IEnumerable<int>? assignedColumns)
    {
        if (slots is null)
        {
            return null;
        }

        HashSet<int>? assigned = assignedColumns is null ? null : [.. assignedColumns];
        var preserved = new Dictionary<int, byte[]>();
        foreach ((int column, byte[] bytes) in slots)
        {
            if (assigned?.Contains(column) != true && Equals(this.Values[column], newValues[column]))
            {
                preserved.Add(column, bytes);
            }
        }

        return preserved;
    }
}
