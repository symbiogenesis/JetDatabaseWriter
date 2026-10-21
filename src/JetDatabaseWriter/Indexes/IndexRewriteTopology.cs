namespace JetDatabaseWriter.Indexes;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>Restores source physical-index sharing on a newly created, still empty replacement table.</summary>
internal static class IndexRewriteTopology
{
    /// <summary>Consolidates only descriptors whose surviving logical indexes shared one source identity.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="source">The original native definition.</param>
    /// <param name="targetPage">The private replacement definition, before row copying and FK emission.</param>
    /// <param name="tableDefs">The current definition reader.</param>
    /// <param name="pager">The transactional page store.</param>
    /// <param name="allocator">The page allocator.</param>
    /// <param name="catalogRows">The catalog used to prove surviving map references.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="JetCorruptDataException">The source identities or private replacement storage cannot be verified.</exception>
    internal static async ValueTask PreserveSharingAsync(
        JetFormat format,
        TDefImage source,
        long targetPage,
        TableDefReader tableDefs,
        Pager pager,
        PageAllocator allocator,
        CatalogRowReader catalogRows,
        CancellationToken cancellationToken)
    {
        var sourceIndexes = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        foreach (TDefLogicalIndexDescriptor index in source.LogicalIndexes)
        {
            if (!sourceIndexes.TryAdd(index.Name, index.Entry.IndexNum2))
            {
                throw new JetCorruptDataException(JetErrorCode.CorruptIndex, $"The source table definition has duplicate logical index name '{index.Name}'.", new JetErrorInfo { PageNumber = source.TDefPageNumber, IndexName = index.Name, Reason = "The table definition contains duplicate logical-index names" });
            }
        }

        LogicalTDefChain chain = await tableDefs.ReadTDefChainAsync(targetPage, cancellationToken).ConfigureAwait(false);
        TDefImage target = TDefCodec.Parse(format, chain.Bytes)
            ?? throw new JetCorruptDataException(JetErrorCode.CorruptIndex, "The private replacement index definition is malformed.", new JetErrorInfo { PageNumber = targetPage });
        int[] representative = new int[target.RealIndexes.Count];
        for (int index = 0; index < representative.Length; index++)
        {
            representative[index] = index;
        }

        var bySourceIdentity = new Dictionary<int, int>();
        foreach (TDefLogicalIndexDescriptor index in target.LogicalIndexes)
        {
            if (!sourceIndexes.TryGetValue(index.Name, out int sourceIdentity))
            {
                continue;
            }

            if (bySourceIdentity.TryGetValue(sourceIdentity, out int first))
            {
                representative[index.Entry.IndexNum2] = first;
            }
            else
            {
                bySourceIdentity.Add(sourceIdentity, index.Entry.IndexNum2);
            }
        }

        var retained = new List<int>(representative.Length);
        for (int index = 0; index < representative.Length; index++)
        {
            if (representative[index] == index)
            {
                retained.Add(index);
            }
        }

        if (retained.Count == representative.Length)
        {
            return;
        }

        byte[] emptyLeaf = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, targetPage, [], enablePrefixCompression: false);
        var removedLeaves = new HashSet<long>();
        var mapStorage = new HashSet<long>();
        for (int index = 0; index < representative.Length; index++)
        {
            if (representative[index] == index)
            {
                continue;
            }

            cancellationToken.ThrowIfCancellationRequested();
            TDefIndexDescriptor descriptor = target.RealIndexes[index];
            byte[] leaf = await pager.ReadPageAsync(descriptor.RootPage, cancellationToken).ConfigureAwait(false);
            try
            {
                if (!leaf.AsSpan().SequenceEqual(emptyLeaf))
                {
                    throw new JetCorruptDataException(JetErrorCode.CorruptIndex, "A replacement index to consolidate is not its private empty initial leaf.", new JetErrorInfo { PageNumber = descriptor.RootPage });
                }
            }
            finally
            {
                PageBuffers.Return(leaf);
            }

            _ = removedLeaves.Add(descriptor.RootPage);
            if (UsageMap.TryReadPointer(chain.Bytes, descriptor.Slot.FirstDpOffset - 4, out UsageMapPointer pointer))
            {
                byte[] page = await pager.ReadPageAsync(pointer.PageNumber, cancellationToken).ConfigureAwait(false);
                try
                {
                    if (UsageMap.TryGetRowBound(page, format.DataPage, format.PageSize, pointer.RowIndex, out RowBound row))
                    {
                        _ = mapStorage.Add(pointer.PageNumber);
                        await UsageMap.CollectReferenceBitmapPagesAsync(page, row, pager.PageCount, pager.ReadPageAsync, PageBuffers.Return, mapStorage, cancellationToken).ConfigureAwait(false);
                    }
                }
                finally
                {
                    PageBuffers.Return(page);
                }
            }
        }

        int width = format.TDef.RealIdxEntrySz;
        int oldColumnStart = format.TDef.BlockEnd + (target.RealIndexes.Count * width);
        int newColumnStart = format.TDef.BlockEnd + (retained.Count * width);
        int columnLength = target.IndexSection!.Value.RealIdxDescStart - oldColumnStart;
        int newPhysicalStart = newColumnStart + columnLength;
        int newLogicalStart = newPhysicalStart + (retained.Count * format.Index.RealIdxPhysSize);
        int oldLogicalStart = target.IndexSection.Value.LogIdxStart;
        int oldEnd = checked(Ri32(chain.Bytes, 8) + 8);
        int newEnd = newLogicalStart + oldEnd - oldLogicalStart;
        byte[] bytes = new byte[chain.Bytes.Length];
        chain.Bytes.AsSpan(0, format.TDef.BlockEnd).CopyTo(bytes);
        chain.Bytes.AsSpan(oldColumnStart, columnLength).CopyTo(bytes.AsSpan(newColumnStart));
        var newNumbers = new Dictionary<int, int>(retained.Count);
        for (int index = 0; index < retained.Count; index++)
        {
            int original = retained[index];
            newNumbers.Add(original, index);
            _ = removedLeaves.Remove(target.RealIndexes[original].RootPage);
            chain.Bytes.AsSpan(format.TDef.BlockEnd + (original * width), width).CopyTo(bytes.AsSpan(format.TDef.BlockEnd + (index * width)));
            chain.Bytes.AsSpan(target.RealIndexes[original].Slot.PhysStart, format.Index.RealIdxPhysSize).CopyTo(bytes.AsSpan(newPhysicalStart + (index * format.Index.RealIdxPhysSize)));
        }

        chain.Bytes.AsSpan(oldLogicalStart, oldEnd - oldLogicalStart).CopyTo(bytes.AsSpan(newLogicalStart));
        for (int index = 0; index < target.LogicalIndexes.Count; index++)
        {
            TDefLogicalIndexDescriptor logical = target.LogicalIndexes[index];
            int offset = logical.Entry.FieldsOffset - oldLogicalStart + newLogicalStart;
            Wi32(bytes, offset + Constants.TableDefinition.Jet3.LogicalIdx.IndexNum2Offset, newNumbers[representative[logical.Entry.IndexNum2]]);
        }

        Wi32(bytes, format.TDef.NumRealIdx, retained.Count);
        Wi32(bytes, 8, newEnd - 8);
        await chain.WriteAsync(bytes, newEnd, allocator.AllocatePageAsync, pager.WritePageAsync, allocator.DeallocatePageAsync, format.WritesTDefFreeSpace, cancellationToken).ConfigureAwait(false);
        foreach (long page in removedLeaves)
        {
            await allocator.DeallocatePageAsync(page, cancellationToken).ConfigureAwait(false);
        }

        HashSet<long> unreferenced = await UsageMapOwnership.FindUnreferencedStoragePagesAsync(format, tableDefs, catalogRows, pager, mapStorage, ignoredTdefPage: 0, cancellationToken).ConfigureAwait(false);
        foreach (long page in unreferenced)
        {
            await allocator.DeallocatePageAsync(page, cancellationToken).ConfigureAwait(false);
        }
    }
}
