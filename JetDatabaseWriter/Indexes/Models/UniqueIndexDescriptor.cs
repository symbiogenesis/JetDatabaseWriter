namespace JetDatabaseWriter.Indexes.Models;

using System.Collections.Generic;

/// <summary>
/// One unique real-idx slot bundled with a best-effort logical-idx name
/// (used in error messages) and the resolved <see cref="KeyColumnInfo"/>
/// list. Produced by the writer's pre-write unique-index loader and
/// consumed by the composite-key encoder + collision detector.
/// </summary>
/// <param name="RealIdxNum">The real index number of.</param>
/// <param name="Name">The name.</param>
/// <param name="KeyColumns">The key columns.</param>
/// <param name="RootPage">The root index B-tree page.</param>
internal readonly record struct UniqueIndexDescriptor(
    int RealIdxNum,
    string Name,
    IReadOnlyList<KeyColumnInfo> KeyColumns,
    long RootPage);
