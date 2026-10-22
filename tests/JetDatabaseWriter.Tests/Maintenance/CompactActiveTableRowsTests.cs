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
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Maintenance;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema.Models;
using Xunit;

/// <summary>Active observations require independent native maps, complete covering keys and preserved surplus rows.</summary>
public sealed class CompactActiveTableRowsTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task ExactMapAndCoveringIndex_AdmitOnlyMappedRowsAndReportEverySurplus(DatabaseFormat kind)
    {
        byte[] bytes = await CreateAsync(kind);
        await using DatabaseFile file = Open(bytes);
        (long owner, TDefImage image) = await FindAsync(file);
        var pages = new ChangedPages(file.Pages);
        var physical = new List<(RowLocation Location, byte[] Bytes)>();
        await CompactRawRowReader.ScanAsync(
            pages,
            file.Format,
            owner,
            new(),
            (location, payload, _) =>
            {
                physical.Add((location, payload.ToArray()));
                return default;
            },
            Ct);
        long clonePage = pages.PageCount;
        pages.ExtraPages = 1;
        pages.Changes[clonePage] = await file.Pages.ReadPageCopyAsync(physical[0].Location.PageNumber, Ct);
        int unassigned = 0;
        var accounting = new CompactDiscoveryAccounting(new());
        using (CompactActiveTableRows authority = await CompactActiveTableRows.ValidateAsync(pages, file.Format, owner, image, 0, new() { Accounting = accounting }, ObserveAsync, Ct))
        {
            Assert.Equal(3, authority.Result.ActiveRows);
            Assert.Equal(3, authority.Result.UnassignedRows);
            Assert.Equal(6, authority.Result.PhysicalRows);
            Assert.False(authority.Result.DiscoveryComplete);
            Assert.False(authority.Result.OwnershipProven);
            Assert.False(authority.Result.DatabaseValidated);
            Assert.Equal(3, unassigned);
            authority.CheckSource(pages, image, Ct);
            int active = 0;
            await authority.ScanRowsAsync(
                (location, payload, _) =>
                {
                    Assert.NotEqual(clonePage, location.PageNumber);
                    Assert.Equal(physical[active].Bytes, payload.ToArray());
                    active++;
                    return default;
                },
                Ct);
            Assert.Equal(3, active);
            Assert.Equal(6, unassigned);
            Assert.True(accounting.ActiveWorkingBytes > 0);
        }

        Assert.Equal(0, accounting.ActiveWorkingBytes);
        Assert.Equal(bytes, await ReadAllAsync(file));

        ValueTask ObserveAsync(CompactActiveTableRowRecord record, CancellationToken token)
        {
            token.ThrowIfCancellationRequested();
            Assert.True(record.IsPinned);
            Assert.Equal(owner, record.OwnerTDefPage);
            if (record.Kind == "unassigned-row")
            {
                Assert.Equal(clonePage, record.Location!.Value.PageNumber);
                Assert.Equal(physical[unassigned % 3].Bytes, record.RawBytes.ToArray());
                unassigned++;
            }

            return default;
        }
    }

    [Fact]
    public async Task NativeUmsaetze_PreservesAll2049UnassignedPhysicalRows()
    {
        byte[] bytes = await File.ReadAllBytesAsync(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Databases", "mdbtools", "nwind.mdb"), Ct);
        await using DatabaseFile file = Open(bytes);
        TDefImage image = (await file.TableDefs.ReadImageAsync(753, Ct))!;
        int ordinal = image.LogicalIndexes.Single(static index => index.Name == "PrimaryKey").Entry.IndexNum2;
        int unassigned = 0;
        using CompactActiveTableRows authority = await CompactActiveTableRows.ValidateAsync(
            file.Pages,
            file.Format,
            753,
            image,
            ordinal,
            new(),
            (record, _) =>
            {
                if (record.Kind == "unassigned-row")
                {
                    Assert.True(record.IsPinned);
                    Assert.NotEmpty(record.RawBytes.ToArray());
                    unassigned++;
                }

                return default;
            },
            Ct);
        Assert.Equal(830, authority.Result.ActiveRows);
        Assert.Equal(2049, authority.Result.UnassignedRows);
        Assert.Equal(2879, authority.Result.PhysicalRows);
        Assert.Equal(2049, unassigned);
        int active = 0;
        await authority.ScanRowsAsync(
            (_, _, _) =>
            {
                active++;
                return default;
            },
            Ct);
        Assert.Equal(830, active);
        Assert.Equal(4098, unassigned);
        Assert.False(authority.Result.DiscoveryComplete);
        Assert.False(authority.Result.OwnershipProven);
        Assert.Equal(bytes, await ReadAllAsync(file));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task EveryStoredIndex_IsComparedWithAgreedActiveRows(bool damageSecondary)
    {
        byte[] bytes = await CreateAsync(DatabaseFormat.Jet4Mdb, secondaryIndex: true);
        await using DatabaseFile file = Open(bytes);
        (long owner, TDefImage image) = await FindAsync(file);
        Assert.Equal(2, image.RealIndexes.Count);
        int primary = image.LogicalIndexes.Single(static index => index.Name == "PrimaryKey").Entry.IndexNum2;
        int secondary = image.LogicalIndexes.Single(static index => index.Name == "ByName").Entry.IndexNum2;
        var pages = new ChangedPages(file.Pages);
        var locations = new List<RowLocation>();
        await CompactRawRowReader.ScanAsync(
            file.Pages,
            file.Format,
            owner,
            new(),
            (location, _, _) =>
            {
                locations.Add(location);
                return default;
            },
            Ct);
        long clonePage = pages.PageCount;
        pages.ExtraPages = 1;
        pages.Changes[clonePage] = await file.Pages.ReadPageCopyAsync(locations[0].PageNumber, Ct);
        var parent = new CompactDiscoveryAccounting(new());
        using (CompactActiveTableRows active = await CompactActiveTableRows.ValidateAsync(pages, file.Format, owner, image, primary, new() { Accounting = parent }, static (_, _) => default, Ct))
        {
            if (damageSecondary)
            {
                TDefIndexDescriptor index = image.RealIndexes[secondary];
                ColumnInfo keyColumn = image.Columns.Single(column => column.ColNum == index.Columns[0].ColNum);
                List<IndexEntry> entries = [];
                for (int row = 0; row < locations.Count; row++)
                {
                    entries.Add(new(IndexKeyEncoder.EncodeColumnEntry(file.Format, keyColumn, row == 1 ? "wrong" : "row" + (row + 1)), locations[row].PageNumber, checked((byte)locations[row].RowIndex)));
                }

                entries.Sort(static (left, right) => left.Key.AsSpan().SequenceCompareTo(right.Key));
                pages.Changes[index.RootPage] = IndexPageCodec.BuildLeafPage(file.Format.IndexPage, file.Format.PageSize, checked((int)owner), entries, enablePrefixCompression: false);
                await Assert.ThrowsAsync<JetCorruptDataException>(async () => await active.ValidateIndexesAsync(Ct));
                Assert.Throws<JetOperationException>(() => active.CheckSource(pages, image, Ct));
            }
            else
            {
                long before = parent.PageReads;
                Assert.Equal(2, await active.ValidateIndexesAsync(Ct));
                Assert.True(parent.PageReads > before);
                Assert.Equal(3, active.Result.ActiveRows);
                Assert.Equal(3, active.Result.UnassignedRows);
                Assert.False(active.Result.OwnershipProven);
            }
        }

        Assert.Equal(0, parent.ActiveWorkingBytes);
    }

    [Fact]
    public async Task AllIndexAudit_SerializesConsumptionAndRetainsAdmissionUntilExpiry()
    {
        byte[] bytes = await CreateAsync(DatabaseFormat.Jet4Mdb);
        await using DatabaseFile file = Open(bytes);
        (long owner, TDefImage image) = await FindAsync(file);
        var pages = new ChangedPages(file.Pages);
        IReadOnlyList<long> mapped = await file.OwnedPages.GetOwnedDataPagesAsync(owner, Ct);
        pages.Changes[pages.PageCount] = await file.Pages.ReadPageCopyAsync(mapped[0], Ct);
        pages.ExtraPages = 1;
        var parent = new CompactDiscoveryAccounting(new());
        CompactActiveTableRows? active = null;
        try
        {
            active = await CompactActiveTableRows.ValidateAsync(pages, file.Format, owner, image, 0, new() { Accounting = parent }, ObserveAsync, Ct);
            await Assert.ThrowsAsync<ObjectDisposedException>(async () => await active.ValidateIndexesAsync(Ct));
            Assert.Equal(0, parent.ActiveWorkingBytes);
        }
        finally
        {
            active?.Dispose();
        }

        async ValueTask ObserveAsync(CompactActiveTableRowRecord record, CancellationToken token)
        {
            if (record.Kind == "unassigned-row" && active is not null)
            {
                await Assert.ThrowsAsync<JetOperationException>(async () => await active.ScanRowsAsync(static (_, _, _) => default, token));
                await Assert.ThrowsAsync<JetOperationException>(async () => await active.ValidateIndexesAsync(token));
                active.Dispose();
                Assert.True(parent.ActiveWorkingBytes > 0);
            }
        }
    }

    [Fact]
    public async Task RepeatedAllIndexAudits_ConsumeLifetimeReadAllowance()
    {
        byte[] bytes = await CreateAsync(DatabaseFormat.Jet4Mdb, secondaryIndex: true);
        await using DatabaseFile file = Open(bytes);
        (long owner, TDefImage image) = await FindAsync(file);
        int primary = image.LogicalIndexes.Single(static index => index.Name == "PrimaryKey").Entry.IndexNum2;
        var measured = new CompactDiscoveryAccounting(new());
        using (CompactActiveTableRows active = await CompactActiveTableRows.ValidateAsync(file.Pages, file.Format, owner, image, primary, new() { Accounting = measured }, static (_, _) => default, Ct))
        {
            Assert.Equal(2, await active.ValidateIndexesAsync(Ct));
        }

        var parent = new CompactDiscoveryAccounting(new());
        using (CompactActiveTableRows active = await CompactActiveTableRows.ValidateAsync(file.Pages, file.Format, owner, image, primary, new() { Accounting = parent, Work = new() { MaxPageReads = measured.PageReads } }, static (_, _) => default, Ct))
        {
            Assert.Equal(2, await active.ValidateIndexesAsync(Ct));
            await Assert.ThrowsAsync<JetLimitationException>(async () => await active.ValidateIndexesAsync(Ct));
            Assert.Equal(measured.PageReads, parent.PageReads);
            Assert.Throws<JetOperationException>(() => active.CheckSource(file.Pages, image, Ct));
        }

        Assert.Equal(0, parent.ActiveWorkingBytes);
    }

    [Fact]
    public async Task SourceAndImageIdentityExpiryAndMutation_AreRefused()
    {
        byte[] bytes = await CreateAsync(DatabaseFormat.Jet4Mdb);
        await using DatabaseFile file = Open(bytes);
        (long owner, TDefImage image) = await FindAsync(file);
        var pages = new ChangedPages(file.Pages);
        var accounting = new CompactDiscoveryAccounting(new());
        CompactActiveTableRows authority = await CompactActiveTableRows.ValidateAsync(pages, file.Format, owner, image, 0, new() { Accounting = accounting }, static (_, _) => default, Ct);
        try
        {
            Assert.Throws<JetCorruptDataException>(() => authority.CheckSource(new ChangedPages(file.Pages), image, Ct));
            TDefImage otherImage = (await file.TableDefs.ReadImageAsync(owner, Ct))!;
            Assert.Throws<JetCorruptDataException>(() => authority.CheckSource(pages, otherImage, Ct));
            long rowPage = 0;
            await authority.ScanRowsAsync(
                (location, _, _) =>
                {
                    rowPage = location.PageNumber;
                    return default;
                },
                Ct);
            byte[] changed = await file.Pages.ReadPageCopyAsync(rowPage, Ct);
            int start = BinaryPrimitives.ReadUInt16LittleEndian(changed.AsSpan(file.Format.DataPage.RowsStart));
            changed[start + file.Format.RowFields.NumCols] ^= 1;
            pages.Changes[rowPage] = changed;
            await Assert.ThrowsAsync<JetCorruptDataException>(async () => await authority.ScanRowsAsync(static (_, _, _) => default, Ct));
        }
        finally
        {
            authority.Dispose();
        }

        Assert.Equal(0, accounting.ActiveWorkingBytes);
        Assert.Throws<ObjectDisposedException>(() => authority.CheckSource(pages, image, Ct));
    }

    [Theory]
    [InlineData("map-type")]
    [InlineData("map-missing-row")]
    [InlineData("row-owner")]
    [InlineData("index-key")]
    [InlineData("definition-projection")]
    public async Task IndependentEvidenceDisagreement_RefusesAndReleasesStorage(string damage)
    {
        byte[] bytes = await CreateAsync(DatabaseFormat.Jet4Mdb);
        await using DatabaseFile file = Open(bytes);
        (long owner, TDefImage image) = await FindAsync(file);
        var pages = new ChangedPages(file.Pages);
        long mapPage = image.Header.OwnedPagesReference >> 8;
        int mapRow = (int)(image.Header.OwnedPagesReference & 0xFF);
        byte[] map = await file.Pages.ReadPageCopyAsync(mapPage, Ct);
        int start = BinaryPrimitives.ReadUInt16LittleEndian(map.AsSpan(file.Format.DataPage.RowsStart + (mapRow * 2)));
        if (damage == "map-type")
        {
            map[start] = 0x7F;
            pages.Changes[mapPage] = map;
        }
        else if (damage == "map-missing-row")
        {
            Array.Clear(map, start + Constants.UsageMap.InlineBitmapOffset, file.Format.PageSize - start - Constants.UsageMap.InlineBitmapOffset);
            pages.Changes[mapPage] = map;
        }
        else if (damage == "definition-projection")
        {
            image = new(image.CopyBytes(), image.Header with { Counters = image.Header.Counters with { RowCount = 4 } }, [.. image.Columns], image.HasDeletedColumns, image.Issues, image.IndexSection, [.. image.RealIndexes], [.. image.LogicalIndexes]) { TDefPageNumber = owner };
        }
        else if (damage == "row-owner")
        {
            IReadOnlyList<long> mapped = await file.OwnedPages.GetOwnedDataPagesAsync(owner, Ct);
            long rowPage = mapped[0];
            byte[] row = await file.Pages.ReadPageCopyAsync(rowPage, Ct);
            BinaryPrimitives.WriteInt32LittleEndian(row.AsSpan(file.Format.DataPage.TDefOff), checked((int)owner + 1));
            pages.Changes[rowPage] = row;
        }
        else
        {
            long root = image.RealIndexes[0].RootPage;
            byte[] index = await file.Pages.ReadPageCopyAsync(root, Ct);
            index[file.Format.IndexPage.FirstEntryOffset + 2] ^= 1;
            pages.Changes[root] = index;
        }

        var accounting = new CompactDiscoveryAccounting(new());
        await Assert.ThrowsAnyAsync<IOException>(async () => await CompactActiveTableRows.ValidateAsync(pages, file.Format, owner, image, 0, new() { Accounting = accounting }, static (_, _) => default, Ct));
        Assert.Equal(0, accounting.ActiveWorkingBytes);
    }

    [Fact]
    public async Task LifetimeWorkBudget_IsSharedWithParentAndNotResetForConsumption()
    {
        byte[] bytes = await CreateAsync(DatabaseFormat.Jet4Mdb);
        await using DatabaseFile file = Open(bytes);
        (long owner, TDefImage image) = await FindAsync(file);
        var parent = new CompactDiscoveryAccounting(new());
        using (CompactActiveTableRows measured = await CompactActiveTableRows.ValidateAsync(file.Pages, file.Format, owner, image, 0, new() { Accounting = parent }, static (_, _) => default, Ct))
        {
            Assert.True(parent.PageReads > 0);
        }

        long reads = parent.PageReads;
        var limits = new CompactDiscoveryBudget { MaxPageReads = reads };
        var surrounding = new CompactDiscoveryAccounting(new());
        using (CompactActiveTableRows exact = await CompactActiveTableRows.ValidateAsync(file.Pages, file.Format, owner, image, 0, new() { Accounting = surrounding, Work = limits }, static (_, _) => default, Ct))
        {
            await Assert.ThrowsAsync<JetLimitationException>(async () => await exact.ScanRowsAsync(static (_, _, _) => default, Ct));
        }

        Assert.Equal(reads, surrounding.PageReads);
        Assert.Equal(0, surrounding.ActiveWorkingBytes);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CoveringIndexInvocation_RequiresSecondChildAndParentObjectAllowance(bool parentLimited)
    {
        byte[] bytes = await CreateAsync(DatabaseFormat.Jet4Mdb);
        await using DatabaseFile file = Open(bytes);
        (long owner, TDefImage image) = await FindAsync(file);
        var pages = new ChangedPages(file.Pages);
        var parent = new CompactDiscoveryAccounting(new() { MaxObjects = parentLimited ? 1 : 10 });
        int observations = 0;
        CompactActiveTableRows? unexpected = null;
        try
        {
            await Assert.ThrowsAsync<JetLimitationException>(async () => unexpected = await CompactActiveTableRows.ValidateAsync(
                pages,
                file.Format,
                owner,
                image,
                0,
                new() { Accounting = parent, Work = new() { MaxObjects = parentLimited ? 10 : 1 } },
                (_, _) =>
                {
                    observations++;
                    return default;
                },
                Ct));
        }
        finally
        {
            unexpected?.Dispose();
        }

        Assert.Equal(1, parent.ReadCount(CompactDiscoveryCharge.Objects));
        Assert.Equal(0, observations);

        // The independent physical row scan reads every page once. The refused
        // covering invocation must not read its root again or deliver a terminal.
        Assert.Equal(1, pages.ReadCounts[image.RealIndexes[0].RootPage]);
        Assert.Equal(0, parent.ActiveWorkingBytes);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task LifetimeObjectAllowance_AdmitsExactConsumptionThenRefusesBeforeIoOrCallbacks(bool parentLimited)
    {
        byte[] bytes = await CreateAsync(DatabaseFormat.Jet4Mdb);
        await using DatabaseFile file = Open(bytes);
        (long owner, TDefImage image) = await FindAsync(file);
        var pages = new ChangedPages(file.Pages);
        var parent = new CompactDiscoveryAccounting(new() { MaxObjects = parentLimited ? 3 : 10 });
        int observations = 0;
        int activeRows = 0;
        using (CompactActiveTableRows authority = await CompactActiveTableRows.ValidateAsync(
            pages,
            file.Format,
            owner,
            image,
            0,
            new() { Accounting = parent, Work = new() { MaxObjects = parentLimited ? 10 : 3 } },
            (_, _) =>
            {
                observations++;
                return default;
            },
            Ct))
        {
            Assert.Equal(2, parent.ReadCount(CompactDiscoveryCharge.Objects));
            await authority.ScanRowsAsync(VisitAsync, Ct);
            Assert.Equal(3, activeRows);
            Assert.Equal(3, parent.ReadCount(CompactDiscoveryCharge.Objects));
            int reads = pages.ReadCounts.Values.Sum();
            int delivered = observations;
            await Assert.ThrowsAsync<JetLimitationException>(async () => await authority.ScanRowsAsync(VisitAsync, Ct));
            Assert.Equal(3, parent.ReadCount(CompactDiscoveryCharge.Objects));
            Assert.Equal(reads, pages.ReadCounts.Values.Sum());
            Assert.Equal(delivered, observations);
            Assert.Equal(3, activeRows);
            Assert.Throws<JetOperationException>(() => authority.CheckSource(pages, image, Ct));
            await Assert.ThrowsAsync<JetOperationException>(async () => await authority.ScanRowsAsync(VisitAsync, Ct));
            Assert.Equal(reads, pages.ReadCounts.Values.Sum());
            Assert.Equal(delivered, observations);
            Assert.True(parent.ActiveWorkingBytes > 0);
        }

        Assert.Equal(0, parent.ActiveWorkingBytes);

        ValueTask VisitAsync(RowLocation location, ReadOnlyMemory<byte> payload, CancellationToken token)
        {
            token.ThrowIfCancellationRequested();
            Assert.True(location.PageNumber > 0);
            Assert.Equal(location.RowSize, payload.Length);
            activeRows++;
            return default;
        }
    }

    [Theory]
    [InlineData("reads")]
    [InlineData("rows")]
    [InlineData("fields")]
    [InlineData("map-steps")]
    [InlineData("references")]
    [InlineData("records")]
    [InlineData("working")]
    [InlineData("key")]
    public async Task EveryIndependentAdmissionFailure_ReleasesChildAndParentStorage(string category)
    {
        byte[] bytes = await CreateAsync(DatabaseFormat.Jet4Mdb);
        await using DatabaseFile file = Open(bytes);
        (long owner, TDefImage image) = await FindAsync(file);
        CompactDiscoveryBudget work = category switch
        {
            "reads" => new() { MaxPageReads = 1 },
            "rows" => new() { MaxRows = 1 },
            "fields" => new() { MaxFields = 1 },
            "map-steps" => new() { MaxMapSteps = 1 },
            "references" => new() { MaxReferences = 1 },
            "records" => new() { MaxRecords = 1 },
            "working" => new() { MaxWorkingBytes = 1 },
            _ => new(),
        };
        var parent = new CompactDiscoveryAccounting(new());
        await Assert.ThrowsAsync<JetLimitationException>(async () => await CompactActiveTableRows.ValidateAsync(file.Pages, file.Format, owner, image, 0, new() { Accounting = parent, Work = work, MaxKeyBytes = category == "key" ? 1 : 65_536 }, static (_, _) => default, Ct));
        Assert.Equal(0, parent.ActiveWorkingBytes);
    }

    [Fact]
    public async Task ObserverFailureCancellationAndConcurrentExpiry_ReleaseAdmission()
    {
        byte[] bytes = await CreateAsync(DatabaseFormat.Jet4Mdb);
        await using DatabaseFile file = Open(bytes);
        (long owner, TDefImage image) = await FindAsync(file);
        var parent = new CompactDiscoveryAccounting(new());
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await CompactActiveTableRows.ValidateAsync(file.Pages, file.Format, owner, image, 0, new() { Accounting = parent }, static (_, _) => throw new InvalidOperationException("observer"), Ct));
        Assert.Equal(0, parent.ActiveWorkingBytes);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await CompactActiveTableRows.ValidateAsync(file.Pages, file.Format, owner, image, 0, new() { Accounting = parent }, async (_, _) => await cancellation.CancelAsync(), cancellation.Token));
        Assert.Equal(0, parent.ActiveWorkingBytes);
        CompactActiveTableRows authority = await CompactActiveTableRows.ValidateAsync(file.Pages, file.Format, owner, image, 0, new() { Accounting = parent }, static (_, _) => default, Ct);
        try
        {
            await Assert.ThrowsAsync<ObjectDisposedException>(async () => await authority.ScanRowsAsync(
                (_, _, _) =>
                {
                    authority.Dispose();
                    Assert.True(parent.ActiveWorkingBytes > 0);
                    return default;
                },
                Ct));
        }
        finally
        {
            authority.Dispose();
        }

        Assert.Equal(0, parent.ActiveWorkingBytes);
    }

    [Theory]
    [InlineData("FixedText/NativeFixedTextJet3Cp1252.mdb")]
    [InlineData("FixedText/NativeFixedTextJet4.mdb")]
    [InlineData("NorthwindTraders.accdb")]
    [InlineData("ComplexFields.accdb")]
    [InlineData("mdbtools/nwind.mdb")]
    public async Task NativeSupportedCoveringRoots_AgreeWithIndependentActiveMapRows(string fixture)
    {
        byte[] bytes = await File.ReadAllBytesAsync(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Databases", fixture), Ct);
        await using DatabaseFile file = Open(bytes);
        int covered = 0;
        int allIndexes = 0;
        await CompactStorageInventory.ScanAsync(file.Pages, file.Format, file.TableDefs, new(), static (_, _) => default, Ct, async (owner, image, token) =>
        {
            if (owner <= 2)
            {
                return;
            }

            for (int ordinal = 0; ordinal < image.RealIndexes.Count; ordinal++)
            {
                if (!CompactIndexRowAudit.SupportsDescriptor(image, ordinal) || image.RealIndexes[ordinal].RootPage == 0)
                {
                    continue;
                }

                using CompactActiveTableRows authority = await CompactActiveTableRows.ValidateAsync(file.Pages, file.Format, owner, image, ordinal, new(), static (_, _) => default, token);
                Assert.Equal(image.Header.Counters.RowCount, authority.Result.ActiveRows);
                Assert.False(authority.Result.OwnershipProven);
                if (ordinal == 0 && Enumerable.Range(0, image.RealIndexes.Count).All(index => CompactIndexRowAudit.SupportsDescriptor(image, index)))
                {
                    allIndexes += await authority.ValidateIndexesAsync(token);
                }

                covered++;
            }
        });
        Assert.True(covered > 0);
        Assert.True(allIndexes > 0);
        Assert.Equal(bytes, await ReadAllAsync(file));
    }

    private static async Task<byte[]> CreateAsync(DatabaseFormat kind, bool secondaryIndex = false)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, kind, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct))
        {
            await writer.CreateTableAsync("Items", [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Name", typeof(string), 40)], secondaryIndex ? [new("ByName", "Name")] : [], Ct);
            for (int index = 1; index <= 3; index++)
            {
                await writer.InsertRowAsync("Items", [index, "row" + index], Ct);
            }
        }

        return stream.ToArray();
    }

    private static DatabaseFile Open(byte[] bytes)
        => DatabaseFile.ForReader(new MemoryStream(bytes, writable: false), bytes.AsSpan(0, Math.Min(4096, bytes.Length)).ToArray(), ReadOnlyMemory<char>.Empty, string.Empty, leaveOpen: false);

    private static async Task<(long Owner, TDefImage Image)> FindAsync(DatabaseFile file)
    {
        (long, TDefImage) found = default;
        await CompactStorageInventory.ScanAsync(file.Pages, file.Format, file.TableDefs, new(), static (_, _) => default, Ct, (owner, image, _) =>
        {
            if (image.Columns.Count == 2 && image.Columns[0].Name == "Id" && image.Columns[1].Name == "Name")
            {
                found = (owner, image);
            }

            return default;
        });
        Assert.NotNull(found.Item2);
        return found;
    }

    private static async Task<byte[]> ReadAllAsync(DatabaseFile file)
    {
        await using var output = new MemoryStream();
        for (long page = 0; page < file.Pages.PageCount; page++)
        {
            byte[] buffer = await file.Pages.ReadPageAsync(page, Ct);
            try
            {
                await output.WriteAsync(buffer.AsMemory(0, file.Format.PageSize), Ct);
            }
            finally
            {
                PageBuffers.Return(buffer);
            }
        }

        return output.ToArray();
    }

    private sealed class ChangedPages(IPageSource inner) : IPageSource
    {
        internal Dictionary<long, byte[]> Changes { get; } = [];

        internal long ExtraPages { get; set; }

        internal Dictionary<long, int> ReadCounts { get; } = [];

        public int PageSize => inner.PageSize;

        public long PageCount => inner.PageCount + this.ExtraPages;

        public bool IsDisposed => inner.IsDisposed;

        public ValueTask<byte[]> ReadPageAsync(long pageNumber, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            this.ReadCounts[pageNumber] = this.ReadCounts.GetValueOrDefault(pageNumber) + 1;
            if (!this.Changes.TryGetValue(pageNumber, out byte[]? changed))
            {
                return inner.ReadPageAsync(pageNumber, cancellationToken);
            }

            byte[] buffer = ArrayPool<byte>.Shared.Rent(this.PageSize);
            changed.CopyTo(buffer, 0);
            return new(buffer);
        }

        public void ThrowIfDisposed() => inner.ThrowIfDisposed();

        public void ThrowIfDisposedOrCancelled(CancellationToken cancellationToken) => inner.ThrowIfDisposedOrCancelled(cancellationToken);
    }
}
