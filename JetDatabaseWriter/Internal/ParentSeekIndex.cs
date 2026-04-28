namespace JetDatabaseWriter.Internal;

using System.Collections.Generic;

/// <summary>
/// Resolved parent-side seek index for a single relationship. The seeker
/// uses <see cref="RootPage"/> as the entry point and encodes the FK-side
/// row values using <see cref="KeyColumns"/> (one entry per relationship
/// PK column, in declaration order) plus the foreign-table column index
/// supplying each value.
/// </summary>
internal sealed record ParentSeekIndex(
    long RootPage,
    IReadOnlyList<ParentSeekKeyColumn> KeyColumns);

/// <summary>One column of a parent-seek composite key.</summary>
internal readonly record struct ParentSeekKeyColumn(
    byte ColumnType,
    bool Ascending,
    int ForeignColumnIndex);
