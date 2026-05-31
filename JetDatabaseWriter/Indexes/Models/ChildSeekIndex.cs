namespace JetDatabaseWriter.Indexes.Models;

using System.Collections.Generic;
using JetDatabaseWriter.Enums;

/// <summary>
/// Resolved child-side (FK-side) seek index for a single relationship.
/// The index cursor uses <see cref="RootPage"/> as the entry point and encodes
/// the parent's PK tuple (in relationship-PK declaration order) using
/// <see cref="KeyColumns"/>. Used by cascade-update / cascade-delete to
/// locate dependent child rows in O(log N + K) page reads instead of an
/// O(N) child-table snapshot scan.
/// </summary>
/// <param name="RootPage">The root page.</param>
/// <param name="KeyColumns">The key columns.</param>
internal sealed record ChildSeekIndex(
    long RootPage,
    IReadOnlyList<ChildSeekKeyColumn> KeyColumns);

/// <summary>One column of a child (FK-side) seek composite key.</summary>
/// <param name="ColumnType">The column type.</param>
/// <param name="Ascending">The ascending.</param>
/// <param name="NumericScale">The numeric scale.</param>
/// <param name="LegacyNumeric">The legacy numeric.</param>
internal readonly record struct ChildSeekKeyColumn(
    ColumnType ColumnType,
    bool Ascending,
    byte NumericScale,
    bool LegacyNumeric);
