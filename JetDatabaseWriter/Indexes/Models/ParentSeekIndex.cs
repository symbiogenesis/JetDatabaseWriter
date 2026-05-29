namespace JetDatabaseWriter.Indexes.Models;

using System.Collections.Generic;
using JetDatabaseWriter.Enums;

/// <summary>
/// Resolved parent-side seek index for a single relationship. The seeker
/// uses <see cref="RootPage"/> as the entry point and encodes the FK-side
/// row values using <see cref="KeyColumns"/> (one entry per relationship
/// PK column, in declaration order) plus the foreign-table column index
/// supplying each value.
/// </summary>
/// <param name="RootPage">The root page.</param>
/// <param name="KeyColumns">The key columns.</param>
internal sealed record ParentSeekIndex(
    long RootPage,
    IReadOnlyList<ParentSeekKeyColumn> KeyColumns);

/// <summary>One column of a parent-seek composite key.</summary>
/// <param name="ColumnType">The column type.</param>
/// <param name="Ascending">The ascending.</param>
/// <param name="ForeignColumnIndex">The foreign column index.</param>
/// <param name="NumericScale">The numeric scale.</param>
/// <param name="LegacyNumeric">The legacy numeric.</param>
internal readonly record struct ParentSeekKeyColumn(
    ColumnType ColumnType,
    bool Ascending,
    int ForeignColumnIndex,
    byte NumericScale,
    bool LegacyNumeric);
