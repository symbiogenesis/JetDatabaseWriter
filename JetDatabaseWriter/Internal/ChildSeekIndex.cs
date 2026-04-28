namespace JetDatabaseWriter.Internal;

using System.Collections.Generic;

/// <summary>
/// Resolved child-side (FK-side) seek index for a single relationship.
/// The seeker uses <see cref="RootPage"/> as the entry point and encodes
/// the parent's PK tuple (in relationship-PK declaration order) using
/// <see cref="KeyColumns"/>. Used by cascade-update / cascade-delete to
/// locate dependent child rows in O(log N + K) page reads instead of an
/// O(N) child-table snapshot scan.
/// </summary>
internal sealed record ChildSeekIndex(
    long RootPage,
    IReadOnlyList<ChildSeekKeyColumn> KeyColumns);

/// <summary>One column of a child (FK-side) seek composite key.</summary>
internal readonly record struct ChildSeekKeyColumn(
    byte ColumnType,
    bool Ascending);
