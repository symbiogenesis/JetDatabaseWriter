namespace JetDatabaseWriter.Maintenance;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;

/// <content>Independently inspects native descriptor-map membership while retaining ambiguous storage.</content>
internal sealed partial class CompactIndexOwnershipAudit
{
    private Dictionary<(long Owner, long Root, UsageMapPointer Pointer), MapInspection> inspectedMaps = [];
    private long mapWorkingBytes;
    private long mapReads;
    private long mapSteps;
    private long memberships;
    private long inspectedDescriptors;
    private IPageSource? mapSource;
    private JetFormat? mapFormat;

    /// <summary>Independently inspects one declared descriptor map before its root's page observations.</summary>
    /// <param name="source">The same immutable decrypted source used for the tree audit.</param>
    /// <param name="format">The verified native source format.</param>
    /// <param name="ownerTDefPage">The previously declared descriptor owner.</param>
    /// <param name="rootPage">The previously declared nonzero physical root.</param>
    /// <param name="pointer">The exact previously declared nonzero ownership-map row.</param>
    /// <param name="visitor">Consumes map membership, bitmap sharing and conservative pinned evidence.</param>
    /// <param name="cancellationToken">Cancels reads, pointer, directory and bitmap inspection.</param>
    /// <returns>A task completing after all native map pointers and bits have been inspected.</returns>
    /// <remarks>
    /// Repeated declarations of the same owner, root and exact map share immutable inspection evidence.
    /// Every observed tree page must belong to each independently inspected map for that root.
    /// Unused members are explicit pinned gaps; membership never proves physical or exclusive ownership.
    /// </remarks>
    internal async ValueTask InspectMapAsync(IPageSource source, JetFormat format, long ownerTDefPage, long rootPage, UsageMapPointer pointer, Func<CompactIndexOwnershipRecord, CancellationToken, ValueTask> visitor, CancellationToken cancellationToken)
    {
        Guard.NotNull(source, nameof(source));
        Guard.NotNull(format, nameof(format));
        Guard.NotNull(visitor, nameof(visitor));
        this.CheckAvailable();
        try
        {
            this.CheckMapSource(source, format, pointer.PageNumber, cancellationToken);
            if (rootPage == 0 || !this.inspectedMaps.TryGetValue((ownerTDefPage, rootPage, pointer), out MapInspection? inspection))
            {
                throw Corrupt(pointer.PageNumber, "An index-map inspection has no matching nonzero-root descriptor declaration.");
            }

            if (inspection.Inspected)
            {
                return;
            }

            // Two pooled pages, a full raw directory and async/collection scratch.
            long scratch = (4L * format.PageSize) + 65_536;
            using IDisposable? reservation = this.ReserveMapStorage(scratch, pointer.PageNumber);
            try
            {
                byte[] page = await this.ReadMapPageAsync(source, format, pointer.PageNumber, cancellationToken).ConfigureAwait(false);
                try
                {
                    RowBound row = this.ValidateMapDirectory(page, format, pointer, cancellationToken);
                    if (page[row.RowStart] == Constants.UsageMap.InlineMapType)
                    {
                        if (row.RowSize <= Constants.UsageMap.InlineMapHeaderSize)
                        {
                            throw Corrupt(pointer.PageNumber, "An inline index ownership map has no bitmap bytes.");
                        }

                        long first = BinaryPrimitives.ReadInt32LittleEndian(page.AsSpan(row.RowStart + 1));
                        if (first < 0)
                        {
                            throw Corrupt(pointer.PageNumber, "An inline index ownership map has a negative base page.");
                        }

                        await this.InspectMapBitsAsync(source, format, inspection, page, row.RowStart + Constants.UsageMap.InlineBitmapOffset, row.RowStart + row.RowSize, first, visitor, cancellationToken).ConfigureAwait(false);
                    }
                    else if (page[row.RowStart] == Constants.UsageMap.ReferenceMapType)
                    {
                        int count = UsageMap.ReferencePointerCount(row.RowSize);
                        var seen = new HashSet<long>();
                        for (int index = 0; index < count; index++)
                        {
                            this.StepMap(pointer.PageNumber, cancellationToken);
                            long bitmap = BinaryPrimitives.ReadUInt32LittleEndian(page.AsSpan(row.RowStart + 1 + (index * 4)));
                            if (bitmap == 0)
                            {
                                continue;
                            }

                            this.ValidatePage(bitmap);
                            if (bitmap <= 1 || !seen.Add(bitmap))
                            {
                                throw Corrupt(bitmap, "An index ownership map repeats or reserves a bitmap reference.");
                            }

                            await this.ClaimMapBitmapAsync(inspection, bitmap, visitor, cancellationToken).ConfigureAwait(false);
                            byte[] bits = await this.ReadMapPageAsync(source, format, bitmap, cancellationToken).ConfigureAwait(false);
                            try
                            {
                                if (bits[0] != Constants.PageTypes.UsageMap || bits[1] != 1)
                                {
                                    throw Corrupt(bitmap, "An index ownership bitmap has an invalid native page type.");
                                }

                                await this.InspectMapBitsAsync(source, format, inspection, bits, Constants.UsageMap.ReferenceMapBitmapOffset, format.PageSize, (long)index * UsageMap.PagesPerReferenceMapPage(format.PageSize), visitor, cancellationToken).ConfigureAwait(false);
                            }
                            finally
                            {
                                PageBuffers.Return(bits);
                            }
                        }
                    }
                    else
                    {
                        throw Corrupt(pointer.PageNumber, "An index ownership map has an unknown native map type.");
                    }
                }
                finally
                {
                    PageBuffers.Return(page);
                }

                inspection.Inspected = true;
                this.inspectedDescriptors += inspection.Declarations;
            }
            finally
            {
                this.mapWorkingBytes -= scratch;
            }

            this.CheckMapSource(source, format, pointer.PageNumber, cancellationToken);
        }
        catch
        {
            this.failed = true;
            throw;
        }
    }

    private void DeclareMap(long owner, long root, UsageMapPointer pointer)
    {
        if (this.inspectedMaps.TryGetValue((owner, root, pointer), out MapInspection? prior))
        {
            prior.Declarations++;
            if (prior.Inspected)
            {
                this.inspectedDescriptors++;
            }

            return;
        }

        IDisposable? reservation = this.ReserveMapStorage(256, pointer.PageNumber);
        try
        {
            var inspection = new MapInspection(owner, root, pointer, reservation);
            this.inspectedMaps.Add((owner, root, pointer), inspection);
            if (root != 0)
            {
                PageState state = this.pages[root];
                state.OwnershipMaps ??= [];
                state.OwnershipMaps.Add(inspection);
            }

            reservation = null;
        }
        finally
        {
            reservation?.Dispose();
        }
    }

    private RowBound ValidateMapDirectory(byte[] page, JetFormat format, UsageMapPointer pointer, CancellationToken token)
    {
        if (page[0] != Constants.PageTypes.Data)
        {
            throw Corrupt(pointer.PageNumber, "An index ownership-map row is not stored on a DATA page.");
        }

        int count = BinaryPrimitives.ReadUInt16LittleEndian(page.AsSpan(format.DataPage.NumRows));
        if (count > (format.PageSize - format.DataPage.RowsStart) / 2 || pointer.RowIndex >= count)
        {
            throw Corrupt(pointer.PageNumber, "An index ownership-map directory is oversized or omits the declared row.");
        }

        int endDirectory = format.DataPage.RowsStart + (count * 2);
        int target = BinaryPrimitives.ReadUInt16LittleEndian(page.AsSpan(format.DataPage.RowsStart + (pointer.RowIndex * 2)));
        int targetStart = target & Constants.DataPage.RowOffsetMask;
        if ((target & ~Constants.DataPage.RowOffsetMask) != 0 || targetStart < endDirectory || targetStart >= format.PageSize)
        {
            throw Corrupt(pointer.PageNumber, "An index ownership-map row is absent, deleted, moved or outside its directory.");
        }

        int end = format.PageSize;
        for (int index = 0; index < count; index++)
        {
            this.StepMap(pointer.PageNumber, token);
            int raw = BinaryPrimitives.ReadUInt16LittleEndian(page.AsSpan(format.DataPage.RowsStart + (index * 2)));
            int start = raw & Constants.DataPage.RowOffsetMask;
            bool deleted = (raw & Constants.DataPage.DeletedRowFlag) != 0;
            if ((raw & ~(Constants.DataPage.RowOffsetMask | Constants.DataPage.NonLiveRowFlags)) != 0
                || (!deleted && (start < endDirectory || start >= format.PageSize))
                || (deleted && start != 0 && (start < endDirectory || start > format.PageSize))
                || (index != pointer.RowIndex && start == targetStart))
            {
                throw Corrupt(pointer.PageNumber, "An index ownership-map directory has invalid bounds, flags or an aliased row.");
            }

            if (start > targetStart && start < end)
            {
                end = start;
            }
        }

        if (end - targetStart < Constants.UsageMap.InlineMapHeaderSize)
        {
            throw Corrupt(pointer.PageNumber, "An index ownership-map row is truncated.");
        }

        return new(pointer.RowIndex, targetStart, end - targetStart);
    }

    private async ValueTask InspectMapBitsAsync(IPageSource source, JetFormat format, MapInspection map, byte[] page, int first, int end, long firstPage, Func<CompactIndexOwnershipRecord, CancellationToken, ValueTask> visitor, CancellationToken token)
    {
        for (int offset = first; offset < end; offset++)
        {
            this.StepMap(map.Pointer.PageNumber, token);
            for (int bit = 0; bit < 8; bit++)
            {
                if ((page[offset] & (1 << bit)) == 0)
                {
                    continue;
                }

                long member = firstPage + ((offset - first) * 8L) + bit;
                this.ValidatePage(member);
                this.ChargeReference(map.Pointer.PageNumber);
                if (this.memberships >= this.budget.MaxMemberships)
                {
                    throw Limit(member, "The retained index-map membership coordinates exceed MaxMemberships.");
                }

                IDisposable? reservation = this.ReserveMapStorage(128, member);
                try
                {
                    if (!map.Members.TryAdd(member, new(reservation)))
                    {
                        throw Corrupt(member, "An index ownership map repeats a member coordinate.");
                    }

                    reservation = null;
                    this.memberships++;
                }
                finally
                {
                    reservation?.Dispose();
                }

                await this.EmitAsync(new() { Kind = "map-member", PageNumber = member, OwnerTDefPage = map.Owner, RootPageNumber = map.Root, SourcePageNumber = map.Pointer.PageNumber, MapRowIndex = map.Pointer.RowIndex }, visitor, token).ConfigureAwait(false);
                this.CheckMapSource(source, format, member, token);
            }
        }
    }

    private async ValueTask ClaimMapBitmapAsync(MapInspection map, long bitmap, Func<CompactIndexOwnershipRecord, CancellationToken, ValueTask> visitor, CancellationToken token)
    {
        this.ChargeReference(map.Pointer.PageNumber);
        CompactIndexOwnershipRecord record = new() { Kind = "map-bitmap", PageNumber = bitmap, OwnerTDefPage = map.Owner, RootPageNumber = map.Root, SourcePageNumber = map.Pointer.PageNumber, MapRowIndex = map.Pointer.RowIndex };
        if (this.pages.TryGetValue(bitmap, out PageState? state))
        {
            if (!state.IsBitmapPage)
            {
                await this.EmitAsync(record with { Kind = "storage-role-contradiction", RelatedOwnerTDefPage = state.Owner, RelatedRootPageNumber = state.FirstRoot, Role = "index-map-bitmap" }, visitor, token).ConfigureAwait(false);
                throw Corrupt(bitmap, "An index ownership bitmap is also a declared map DATA page or an index page or target.");
            }

            record = record with { Kind = "shared-map-bitmap", RelatedOwnerTDefPage = state.Owner, RelatedRootPageNumber = state.FirstRoot };
        }
        else
        {
            this.AddPage(bitmap, new(map.Owner, map.Root) { IsMapPage = true, IsBitmapPage = true });
        }

        await this.EmitAsync(record, visitor, token).ConfigureAwait(false);
    }

    private async ValueTask ObserveMapPageAsync(CompactIndexRecord record, Func<CompactIndexOwnershipRecord, CancellationToken, ValueTask> visitor, CancellationToken token)
    {
        if (this.pages[record.RootPageNumber].OwnershipMaps is not { } maps)
        {
            return;
        }

        foreach (MapInspection map in maps)
        {
            this.Step(record.PageNumber, token);
            if (!map.Inspected)
            {
                continue;
            }

            if (!map.Members.TryGetValue(record.PageNumber, out MapMember? member))
            {
                await this.EmitAsync(new() { Kind = "unmapped-index-page", PageNumber = record.PageNumber, OwnerTDefPage = map.Owner, RootPageNumber = map.Root, SourcePageNumber = map.Pointer.PageNumber, MapRowIndex = map.Pointer.RowIndex }, visitor, token).ConfigureAwait(false);
                throw Corrupt(record.PageNumber, "An observed index page is absent from a declared independently inspected ownership map.");
            }

            member.Observed = true;
            map.RootObserved |= record.PageNumber == record.RootPageNumber;
        }
    }

    private async ValueTask<bool> CompleteMapsAsync(Func<CompactIndexOwnershipRecord, CancellationToken, ValueTask> visitor, CancellationToken token)
    {
        bool complete = true;
        foreach (MapInspection map in this.inspectedMaps.Values)
        {
            this.Step(map.Pointer.PageNumber, token);
            if (!map.Inspected)
            {
                complete = false;
                await this.EmitAsync(new() { Kind = "uninspected-ownership-map", PageNumber = map.Pointer.PageNumber, OwnerTDefPage = map.Owner, RootPageNumber = map.Root, MapRowIndex = map.Pointer.RowIndex }, visitor, token).ConfigureAwait(false);
            }

            if (map.Inspected && !map.RootObserved)
            {
                throw Corrupt(map.Root, "An independently inspected ownership map has no subsequent root-page observation.");
            }

            foreach (KeyValuePair<long, MapMember> item in map.Members)
            {
                this.Step(item.Key, token);
                if (!item.Value.Observed)
                {
                    complete = false;
                    await this.EmitAsync(new() { Kind = "unobserved-map-member", PageNumber = item.Key, OwnerTDefPage = map.Owner, RootPageNumber = map.Root, SourcePageNumber = map.Pointer.PageNumber, MapRowIndex = map.Pointer.RowIndex }, visitor, token).ConfigureAwait(false);
                }
            }
        }

        return complete;
    }

    private async ValueTask<byte[]> ReadMapPageAsync(IPageSource source, JetFormat format, long page, CancellationToken token)
    {
        this.CheckMapSource(source, format, page, token);
        this.ValidatePage(page);
        if (this.mapReads >= this.budget.MaxPageReads)
        {
            throw Limit(page, "Independent index-map reads exceed MaxPageReads.");
        }

        this.mapReads++;
        byte[] buffer = await source.ReadUncachedPageAsync(page, token).ConfigureAwait(false);
        try
        {
            this.CheckMapSource(source, format, page, token);
            if (buffer.Length < format.PageSize)
            {
                throw Corrupt(page, "An index ownership map read is truncated.");
            }

            return buffer;
        }
        catch
        {
            PageBuffers.Return(buffer);
            throw;
        }
    }

    private void CheckMapSource(IPageSource source, JetFormat format, long page, CancellationToken token)
    {
        source.ThrowIfDisposedOrCancelled(token);
        if (source.PageCount != this.pageCount || source.PageSize != format.PageSize
            || (this.mapSource is not null && (!ReferenceEquals(this.mapSource, source) || !ReferenceEquals(this.mapFormat, format))))
        {
            throw Corrupt(page, "The independent index-map source identity or geometry changed or disagrees with its audit.");
        }

        this.mapSource = source;
        this.mapFormat = format;
    }

    private void StepMap(long page, CancellationToken token)
    {
        token.ThrowIfCancellationRequested();
        if (this.mapSteps >= this.budget.MaxMapSteps)
        {
            throw Limit(page, "Independent index-map inspection exceeds MaxMapSteps.");
        }

        this.budget.Accounting?.Charge(CompactDiscoveryCharge.MapSteps, 1, page);
        this.mapSteps++;
    }

    private IDisposable? ReserveMapStorage(long bytes, long page)
    {
        if (bytes > this.budget.MaxWorkingBytes - this.storage - (this.mapRows.Count * 256L) - this.mapWorkingBytes)
        {
            throw Limit(page, "Independent index-map storage exceeds MaxWorkingBytes.");
        }

        IDisposable? reservation = this.budget.Accounting?.ReserveWorkingBytes(bytes, page);
        this.mapWorkingBytes += bytes;
        return reservation;
    }

    private void DisposeMaps()
    {
        foreach (MapInspection map in this.inspectedMaps.Values)
        {
            foreach (MapMember member in map.Members.Values)
            {
                member.Reservation?.Dispose();
            }

            map.Reservation?.Dispose();
        }

        this.inspectedMaps = [];
    }

    private sealed class MapInspection(long owner, long root, UsageMapPointer pointer, IDisposable? reservation)
    {
        internal long Owner { get; } = owner;

        internal long Root { get; } = root;

        internal UsageMapPointer Pointer { get; } = pointer;

        internal IDisposable? Reservation { get; } = reservation;

        internal Dictionary<long, MapMember> Members { get; } = [];

        internal long Declarations { get; set; } = 1;

        internal bool Inspected { get; set; }

        internal bool RootObserved { get; set; }
    }

    private sealed class MapMember(IDisposable? reservation)
    {
        internal IDisposable? Reservation { get; } = reservation;

        internal bool Observed { get; set; }
    }
}
