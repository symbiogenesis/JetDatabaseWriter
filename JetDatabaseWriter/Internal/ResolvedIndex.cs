namespace JetDatabaseWriter.Internal;

using System.Collections.Generic;

/// <summary>
/// Resolved per-index column-number / direction / uniqueness tuple consumed
/// by the TDEF emitter and the maintenance loop.
/// </summary>
internal readonly struct ResolvedIndex
{
    public ResolvedIndex(string name, IReadOnlyList<int> columnNumbers, IReadOnlyList<bool> ascending, bool isPrimaryKey, bool isUnique)
    {
        Name = name;
        ColumnNumbers = columnNumbers;
        Ascending = ascending;
        IsPrimaryKey = isPrimaryKey;
        IsUnique = isUnique;
    }

    public string Name { get; }

    public IReadOnlyList<int> ColumnNumbers { get; }

    /// <summary>Gets the per-column sort direction (parallel to <see cref="ColumnNumbers"/>).</summary>
    public IReadOnlyList<bool> Ascending { get; }

    public bool IsPrimaryKey { get; }

    /// <summary>
    /// Gets a value indicating whether this index enforces uniqueness on its
    /// key columns. Primary keys are implicitly unique; for them this
    /// returns <see langword="true"/> regardless of the user-supplied
    /// <see cref="IndexDefinition.IsUnique"/>.
    /// </summary>
    public bool IsUnique { get; }
}
