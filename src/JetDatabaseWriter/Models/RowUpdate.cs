namespace JetDatabaseWriter.Models;

using System.Collections.Generic;
using JetDatabaseWriter.Infrastructure;

/// <summary>
/// Pairs a <see cref="RowCriteria"/> filter with the <see cref="RowValues"/> assignments
/// to apply to rows matching that filter within a single atomic multi-row update statement.
/// </summary>
public sealed class RowUpdate
{
    /// <summary>
    /// Initializes a new instance of the <see cref="RowUpdate"/> class.
    /// </summary>
    /// <param name="criteria">The filter selecting the target row or rows.</param>
    /// <param name="updatedValues">The named column assignments to apply to matching rows.</param>
    public RowUpdate(RowCriteria criteria, RowValues updatedValues)
    {
        Guard.NotNull(criteria, nameof(criteria));
        Guard.NotNull(updatedValues, nameof(updatedValues));
        this.Criteria = criteria;
        this.UpdatedValues = updatedValues;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="RowUpdate"/> class from an existing set of column assignments.
    /// </summary>
    /// <param name="criteria">The filter selecting the target row or rows.</param>
    /// <param name="updatedValues">The named column assignments to apply to matching rows.</param>
    public RowUpdate(RowCriteria criteria, IEnumerable<KeyValuePair<string, object?>> updatedValues)
    {
        Guard.NotNull(criteria, nameof(criteria));
        Guard.NotNull(updatedValues, nameof(updatedValues));
        this.Criteria = criteria;
        this.UpdatedValues = [with(updatedValues)];
    }

    /// <summary>Gets the filter selecting the target row or rows.</summary>
    public RowCriteria Criteria { get; }

    /// <summary>Gets the named column assignments to apply to matching rows.</summary>
    public RowValues UpdatedValues { get; }
}
