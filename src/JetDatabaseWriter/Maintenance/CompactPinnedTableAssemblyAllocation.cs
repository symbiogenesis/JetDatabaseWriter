namespace JetDatabaseWriter.Maintenance;

using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

/// <summary>Untrusted private allocations and installed pointer-only image, retaining every original page.</summary>
/// <param name="Owner">The original pinned table identity.</param>
/// <param name="FirstPageNumber">The original private append point.</param>
/// <param name="PageCount">The complete appended table, index and map page count.</param>
/// <param name="Table">The raw table, native maps and installed logical patch.</param>
/// <param name="Indexes">Every physical stored-index allocation in original descriptor order.</param>
internal sealed record CompactPinnedTableAssemblyAllocation(
    long Owner,
    long FirstPageNumber,
    long PageCount,
    CompactRawTableBuilderResult Table,
    IReadOnlyList<CompactRawIndexAssemblyAllocation> Indexes)
{
    /// <summary>Gets a value indicating whether the complete database has been validated; always false.</summary>
    public bool DatabaseValidated { get; }

    /// <summary>Enumerates every appended allocation, including all map storage, in physical assembly order.</summary>
    /// <param name="cancellationToken">Cancels before each allocation is yielded.</param>
    /// <returns>Untrusted role/owner observations for independent global allocation comparison; excludes the pinned prefix.</returns>
    internal async IAsyncEnumerable<CompactGlobalAllocationRecord> AllocationsAsync([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await Task.CompletedTask.ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        if (this.Table.Data.PageCount > 0)
        {
            yield return new(CompactGlobalAllocationKind.Data, this.Owner, new(this.Table.Data.FirstPageNumber, this.Table.Data.PageCount));
        }

        cancellationToken.ThrowIfCancellationRequested();
        if (this.Table.LongValues.PageCount > 0)
        {
            yield return new(CompactGlobalAllocationKind.LongValue, this.Owner, new(this.Table.LongValues.FirstPageNumber, this.Table.LongValues.PageCount));
        }

        cancellationToken.ThrowIfCancellationRequested();
        yield return this.Map(this.Table.OwnedPages);
        cancellationToken.ThrowIfCancellationRequested();
        yield return this.Map(this.Table.FreePages);
        foreach (CompactRawTableLongValueMaps column in this.Table.LongValueMaps)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return this.Map(column.OwnedPages);
            cancellationToken.ThrowIfCancellationRequested();
            yield return this.Map(column.FreePages);
        }

        foreach (CompactRawIndexAssemblyAllocation index in this.Indexes)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return new(CompactGlobalAllocationKind.Index, this.Owner, new(index.Tree.FirstPageNumber, index.Tree.PageCount));
            cancellationToken.ThrowIfCancellationRequested();
            yield return this.Map(index.OwnedPages);
        }
    }

    private CompactGlobalAllocationRecord Map(CompactUsageMapBuilderResult map)
        => new(CompactGlobalAllocationKind.UsageMap, this.Owner, new(map.FirstPageNumber, map.PageCount));
}
