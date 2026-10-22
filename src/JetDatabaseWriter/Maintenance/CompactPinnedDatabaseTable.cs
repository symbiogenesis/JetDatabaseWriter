namespace JetDatabaseWriter.Maintenance;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Schema.Models;

/// <summary>Independent immutable original evidence and untrusted private allocations for one local table.</summary>
/// <param name="Owner">The original pinned definition identity.</param>
/// <param name="Definition">The complete independent original definition.</param>
/// <param name="Indexes">Independent original stored entries in physical descriptor order.</param>
/// <param name="Allocation">The complete private allocation claim to validate.</param>
/// <param name="OpenRows">Opens a distinct immutable reopened row ledger; the coordinator disposes the returned stream.</param>
/// <param name="OpenLongValues">Opens a distinct immutable reopened LVAL ledger; the coordinator disposes the returned stream.</param>
/// <param name="HydratedColumns">Optional independently verified calculated ResultType projections.</param>
/// <param name="ActiveIndexOrdinal">Optional independently rechecked ordinary covering-index and native-map selection; unassigned rows remain pinned and reported.</param>
/// <remarks>
/// Factories may open disk-backed scratch and run only while this table is compared. Their streams must use backing
/// storage distinct from the source, destination, global ledger and each other, including through wrappers.
/// Caller evidence and provider work remain caller-owned and immutable throughout the operation.
/// </remarks>
internal sealed record CompactPinnedDatabaseTable(
    long Owner,
    TDefImage Definition,
    IReadOnlyList<CompactPinnedTableIndexSource> Indexes,
    CompactPinnedTableAssemblyAllocation Allocation,
    Func<CancellationToken, ValueTask<Stream>> OpenRows,
    Func<CancellationToken, ValueTask<Stream>> OpenLongValues,
    IReadOnlyList<ColumnInfo>? HydratedColumns = null,
    int? ActiveIndexOrdinal = null);
