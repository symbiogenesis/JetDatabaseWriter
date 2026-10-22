namespace JetDatabaseWriter.Tests.Maintenance;

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Maintenance;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema.Models;
using Xunit;

/// <summary>Independent native index-map membership never authorizes reclaiming unused storage.</summary>
public sealed class CompactIndexMapAuditTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task InlineMap_ProvesObservedMembershipAndRetainsUnusedMembers(DatabaseFormat kind)
    {
        var format = JetFormat.ForNewDatabase(kind);
        var pages = new MemoryPages(format.PageSize, new() { [8] = Inline(format, 3, 5) });
        List<CompactIndexOwnershipRecord> records = [];
        using var audit = new CompactIndexOwnershipAudit(20, new());
        await audit.DeclareDescriptorAsync(2, 3, new(0, 8), 0, Visit, Ct);
        await audit.InspectMapAsync(pages, format, 2, 3, new(0, 8), Visit, Ct);
        await audit.ObserveAsync(Page(3), Visit, Ct);
        await audit.ObserveAsync(Terminal(3), Visit, Ct);
        await audit.CompleteAsync(Visit, Ct);

        Assert.Equal(1, pages.Reads);
        Assert.Equal<long>([3, 5], records.Where(static record => record.Kind == "map-member").Select(static record => record.PageNumber));
        Assert.Equal(5, Assert.Single(records, static record => record.Kind == "unobserved-map-member").PageNumber);
        CompactIndexOwnershipRecord result = Assert.Single(records, static record => record.Kind == "ownership-terminal");
        Assert.True(result.DeclaredMapsInspected);
        Assert.True(result.ObservedIndexPagesMapped);
        Assert.False(result.DeclaredMapMembersObserved);
        Assert.All(records, static record => Assert.True(record.IsPinned));

        ValueTask Visit(CompactIndexOwnershipRecord record, CancellationToken token)
        {
            token.ThrowIfCancellationRequested();
            records.Add(record);
            return default;
        }
    }

    [Fact]
    public async Task EqualGeometryDifferentSource_RefusesMixedMapEvidence()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        var source = new MemoryPages(format.PageSize, new() { [8] = Inline(format, 3) });
        var other = new MemoryPages(format.PageSize, new() { [8] = Inline(format, 4) });
        using var audit = new CompactIndexOwnershipAudit(20, new());
        await audit.DeclareDescriptorAsync(2, 3, new(0, 8), 0, Ignore, Ct);
        await audit.InspectMapAsync(source, format, 2, 3, new(0, 8), Ignore, Ct);
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await audit.InspectMapAsync(other, format, 2, 3, new(0, 8), Ignore, Ct));
        Assert.Equal(0, other.Reads);
    }

    [Fact]
    public async Task EveryDescriptorMap_MustCoverSharedRootPages()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        var pages = new MemoryPages(format.PageSize, new() { [8] = Inline(format, 3), [9] = Inline(format, 4) });
        List<CompactIndexOwnershipRecord> records = [];
        using var audit = new CompactIndexOwnershipAudit(20, new());
        await audit.DeclareDescriptorAsync(2, 3, new(0, 8), 0, Visit, Ct);
        await audit.InspectMapAsync(pages, format, 2, 3, new(0, 8), Visit, Ct);
        await audit.DeclareDescriptorAsync(2, 3, new(0, 9), 1, Visit, Ct);
        await audit.InspectMapAsync(pages, format, 2, 3, new(0, 9), Visit, Ct);
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await audit.ObserveAsync(Page(3), Visit, Ct));
        Assert.Single(records, static record => record.Kind == "unmapped-index-page" && record.PageNumber == 3 && record.SourcePageNumber == 9);
        await Assert.ThrowsAsync<JetOperationException>(async () => await audit.CompleteAsync(Visit, Ct));

        ValueTask Visit(CompactIndexOwnershipRecord record, CancellationToken token)
        {
            token.ThrowIfCancellationRequested();
            records.Add(record);
            return default;
        }
    }

    [Fact]
    public async Task ReferenceBitmapSharing_RemainsExplicitAcrossOwners()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        byte[] bitmap = new byte[format.PageSize];
        bitmap[0] = Constants.PageTypes.UsageMap;
        bitmap[1] = 1;
        bitmap[Constants.UsageMap.ReferenceMapBitmapOffset] = (1 << 3) | (1 << 4);
        var pages = new MemoryPages(format.PageSize, new() { [8] = Reference(format, 10), [9] = Reference(format, 10), [10] = bitmap });
        List<CompactIndexOwnershipRecord> records = [];
        using var audit = new CompactIndexOwnershipAudit(20, new());
        await audit.DeclareDescriptorAsync(2, 3, new(0, 8), 0, Visit, Ct);
        await audit.InspectMapAsync(pages, format, 2, 3, new(0, 8), Visit, Ct);
        await audit.DeclareDescriptorAsync(7, 4, new(0, 9), 0, Visit, Ct);
        await audit.InspectMapAsync(pages, format, 7, 4, new(0, 9), Visit, Ct);
        CompactIndexOwnershipRecord alias = Assert.Single(records, static record => record.Kind == "shared-map-bitmap");
        Assert.Equal(10, alias.PageNumber);
        Assert.Equal(2, alias.RelatedOwnerTDefPage);
        Assert.Equal(7, alias.OwnerTDefPage);
        Assert.False(alias.ExclusiveOwnershipProven);

        ValueTask Visit(CompactIndexOwnershipRecord record, CancellationToken token)
        {
            token.ThrowIfCancellationRequested();
            records.Add(record);
            return default;
        }
    }

    [Theory]
    [InlineData("duplicate-bitmap")]
    [InlineData("out-of-range-bit")]
    [InlineData("alias-directory")]
    [InlineData("bitmap-role")]
    public async Task MalformedMap_RefusesBeforeSuccessfulTerminal(string damage)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        byte[] map = damage is "duplicate-bitmap" or "bitmap-role" ? Reference(format, damage == "bitmap-role" ? 3u : 10u) : Inline(format, 3);
        if (damage == "duplicate-bitmap")
        {
            BinaryPrimitives.WriteUInt32LittleEndian(map.AsSpan(format.PageSize - format.UsageMapRowSize + 5), 10);
        }
        else if (damage == "out-of-range-bit")
        {
            map[format.PageSize - format.UsageMapRowSize + Constants.UsageMap.InlineBitmapOffset + 2] = 1 << 4;
        }
        else if (damage == "alias-directory")
        {
            BinaryPrimitives.WriteUInt16LittleEndian(map.AsSpan(format.DataPage.NumRows), 2);
            BinaryPrimitives.WriteUInt16LittleEndian(map.AsSpan(format.DataPage.RowsStart + 2), checked((ushort)(format.PageSize - format.UsageMapRowSize)));
        }

        byte[] bitmap = new byte[format.PageSize];
        bitmap[0] = Constants.PageTypes.UsageMap;
        bitmap[1] = 1;
        var pages = new MemoryPages(format.PageSize, new() { [8] = map, [10] = bitmap });
        using var audit = new CompactIndexOwnershipAudit(20, new());
        await audit.DeclareDescriptorAsync(2, 3, new(0, 8), 0, Ignore, Ct);
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await audit.InspectMapAsync(pages, format, 2, 3, new(0, 8), Ignore, Ct));
        await Assert.ThrowsAsync<JetOperationException>(async () => await audit.CompleteAsync(Ignore, Ct));
    }

    [Theory]
    [InlineData("reads")]
    [InlineData("steps")]
    [InlineData("members")]
    public async Task ExhaustedMapWork_RefusesAndRetainsAdmissionUntilDisposal(string limit)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        var accounting = new CompactDiscoveryAccounting(new());
        CompactIndexOwnershipBudget budget = limit switch
        {
            "reads" => new() { Accounting = accounting, MaxPageReads = 1 },
            "steps" => new() { Accounting = accounting, MaxMapSteps = 1 },
            _ => new() { Accounting = accounting, MaxMemberships = 1 },
        };
        byte[] bitmap = new byte[format.PageSize];
        bitmap[0] = Constants.PageTypes.UsageMap;
        bitmap[1] = 1;
        bitmap[Constants.UsageMap.ReferenceMapBitmapOffset] = (1 << 3) | (1 << 4);
        var pages = new MemoryPages(format.PageSize, new() { [8] = Reference(format, 10), [10] = bitmap });
        var audit = new CompactIndexOwnershipAudit(20, budget);
        try
        {
            await audit.DeclareDescriptorAsync(2, 3, new(0, 8), 0, Ignore, Ct);
            await Assert.ThrowsAsync<JetLimitationException>(async () => await audit.InspectMapAsync(pages, format, 2, 3, new(0, 8), Ignore, Ct));
            Assert.True(accounting.ActiveWorkingBytes > 0);
        }
        finally
        {
            audit.Dispose();
        }

        Assert.Equal(0, accounting.ActiveWorkingBytes);
    }

    [Theory]
    [InlineData("FixedText/NativeFixedTextJet3Cp1252.mdb")]
    [InlineData("FixedText/NativeFixedTextJet4.mdb")]
    [InlineData("NorthwindTraders.accdb")]
    public async Task NativeCatalogMaps_CoverEveryObservedTreePage(string fixture)
    {
        byte[] image = await File.ReadAllBytesAsync(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Databases", fixture), Ct);
        byte[] original = image.ToArray();
        await using var stream = new MemoryStream(image, writable: false);
        await using var file = DatabaseFile.ForReader(stream, image, ReadOnlyMemory<char>.Empty, string.Empty, leaveOpen: true, cancellationToken: Ct);
        TDefImage? definition = await file.TableDefs.ReadImageAsync(2, Ct);
        Assert.NotNull(definition);
        using var audit = new CompactIndexOwnershipAudit(file.Pages.PageCount, new());
        for (int ordinal = 0; ordinal < definition.RealIndexes.Count; ordinal++)
        {
            TDefIndexDescriptor index = definition.RealIndexes[ordinal];
            uint raw = BinaryPrimitives.ReadUInt32LittleEndian(definition.Bytes[(index.Slot.FirstDpOffset - 4)..]);
            UsageMapPointer pointer = new(checked((int)(raw & 255)), checked((int)(raw >> 8)));
            await audit.DeclareDescriptorAsync(2, index.RootPage, pointer, ordinal, Ignore, Ct);
            if (index.RootPage != 0 && pointer != default)
            {
                await audit.InspectMapAsync(file.Pages, file.Format, 2, index.RootPage, pointer, Ignore, Ct);
                await CompactIndexAudit.ScanAsync(file.Pages, file.Format, 2, index.RootPage, new(), (record, token) => audit.ObserveAsync(record, Ignore, token), Ct);
            }
        }

        await audit.CompleteAsync(Ignore, Ct);
        Assert.Equal(original, image);
    }

    private static byte[] Inline(JetFormat format, params int[] members)
    {
        byte[] page = RowPage(format);
        int offset = format.PageSize - format.UsageMapRowSize + Constants.UsageMap.InlineBitmapOffset;
        foreach (int member in members)
        {
            page[offset + (member / 8)] |= checked((byte)(1 << (member % 8)));
        }

        return page;
    }

    private static byte[] Reference(JetFormat format, uint bitmap)
    {
        byte[] page = RowPage(format);
        int start = format.PageSize - format.UsageMapRowSize;
        page[start] = Constants.UsageMap.ReferenceMapType;
        BinaryPrimitives.WriteUInt32LittleEndian(page.AsSpan(start + 1), bitmap);
        return page;
    }

    private static byte[] RowPage(JetFormat format)
    {
        byte[] page = new byte[format.PageSize];
        page[0] = Constants.PageTypes.Data;
        page[1] = 1;
        BinaryPrimitives.WriteUInt16LittleEndian(page.AsSpan(format.DataPage.NumRows), 1);
        BinaryPrimitives.WriteUInt16LittleEndian(page.AsSpan(format.DataPage.RowsStart), checked((ushort)(format.PageSize - format.UsageMapRowSize)));
        return page;
    }

    private static CompactIndexRecord Page(long root) => new() { Kind = "index-page", OwnerTDefPage = 2, RootPageNumber = root, PageNumber = root };

    private static CompactIndexRecord Terminal(long root) => new() { Kind = "index-terminal", OwnerTDefPage = 2, RootPageNumber = root, PageNumber = root, NavigableGraphAcyclic = true, EntryEdgeFencesValidated = true };

    private static ValueTask Ignore(CompactIndexOwnershipRecord record, CancellationToken token)
    {
        token.ThrowIfCancellationRequested();
        Assert.True(record.IsPinned);
        return default;
    }

    private sealed class MemoryPages(int pageSize, Dictionary<long, byte[]> images) : IPageSource
    {
        public int PageSize => pageSize;

        public long PageCount => 20;

        public bool IsDisposed => false;

        internal int Reads { get; private set; }

        public ValueTask<byte[]> ReadPageAsync(long pageNumber, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            this.Reads++;
            byte[] buffer = ArrayPool<byte>.Shared.Rent(pageSize);
            images[pageNumber].CopyTo(buffer, 0);
            return ValueTask.FromResult(buffer);
        }

        public void ThrowIfDisposed()
        {
        }

        public void ThrowIfDisposedOrCancelled(CancellationToken cancellationToken) => cancellationToken.ThrowIfCancellationRequested();
    }
}
