namespace JetDatabaseWriter.Tests.Maintenance;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Maintenance;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using Xunit;

/// <summary>Native stored Id keys independently identify catalog rows after owner and hint damage.</summary>
public sealed class CompactCatalogRecoveryTests
{
    [Theory]
    [InlineData("Jet3Test.mdb")]
    [InlineData("Jackcess/adox_jet4.mdb")]
    [InlineData("Jackcess/V2007/complexDataTestV2007.accdb")]
    public async Task NativeIdIndexRecoversDirectCatalogIdentitiesWithoutOwnersOrAllocationHints(string relativePath)
    {
        byte[] bytes = await File.ReadAllBytesAsync(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Databases", relativePath), Ct);
        CatalogRow[] expected;
        CatalogRow target;
        JetFormat format;
        await using (DatabaseFile intact = Open(bytes))
        {
            format = intact.Format;
            TDefImage image = Assert.IsType<TDefImage>(await intact.TableDefs.ReadImageAsync(2, Ct));
            var decoder = new CatalogRowReader(format, intact.TableDefs, intact.OwnedPages);
            expected = (await decoder.GetCatalogRowsAsync(image.Definition, Ct, scanPhysicalPages: true)).ToArray();
            target = expected.First(row =>
            {
                int slot = checked(((int)row.PageNumber * format.PageSize) + format.DataPage.RowsStart + (row.RowIndex * 2));
                return (BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(slot, 2)) & Constants.DataPage.NonLiveRowFlags) == 0;
            });
        }

        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(checked(((int)target.PageNumber * format.PageSize) + format.DataPage.TDefOff), 4), 0);
        bytes.AsSpan((2 * format.PageSize) + format.TDef.UsedPages, 4).Clear();
        bytes.AsSpan((2 * format.PageSize) + format.TDef.NumRows, 4).Clear();
        byte[] original = bytes.ToArray();
        await using DatabaseFile damaged = Open(bytes);
        var pages = new CompactInventoryPageSource(damaged.Pages, format, 100_000);
        using var definitions = new TableDefReader(pages, format, cacheResults: false);
        using var owned = new OwnedDataPages(pages, format, cacheResults: false);
        var rows = new CatalogRowReader(format, definitions, owned);
        TDefImage catalog = Assert.IsType<TDefImage>(await definitions.ReadImageAsync(2, Ct));
        List<CatalogRow> physical = await rows.GetCatalogRowsAsync(catalog.Definition, Ct, scanPhysicalPages: true);
        Assert.DoesNotContain(physical, row => row.PageNumber == target.PageNumber);
        CompactCatalogRecovery.Result recovered = await CompactCatalogRecovery.ScanAsync(pages, format, catalog, rows, new(), physical, 100_000, 100_000, 512L * 1024 * 1024, Ct);
        Assert.Contains(recovered.Rows, row => row.Identity == target);
        Assert.All(recovered.Rows, row => Assert.Contains(row.Identity, expected));
        Assert.Equal(original, bytes);
    }

    [Theory]
    [InlineData("Jet3Test.mdb", false)]
    [InlineData("Jet3Test.mdb", true)]
    [InlineData("Jackcess/adox_jet4.mdb", false)]
    [InlineData("Jackcess/adox_jet4.mdb", true)]
    [InlineData("Jackcess/V2007/complexDataTestV2007.accdb", false)]
    [InlineData("Jackcess/V2007/complexDataTestV2007.accdb", true)]
    public async Task NativeIdIndexRecoversMovedCatalogIdentityWithIndependentPayloadOwner(string relativePath, bool chained)
    {
        (byte[] bytes, JetFormat format, CatalogRow target) = await CreateMovedCatalogAsync(relativePath, chained);
        byte[] original = bytes.ToArray();
        CompactCatalogRecovery.Result recovered = await RecoverAsync(bytes, format);
        CompactCatalogRecovery.RecoveredRow evidence = Assert.Single(recovered.Rows, row => row.Identity == target);
        Assert.Equal(target.PageNumber, evidence.Location.PageNumber);
        Assert.Equal(target.RowIndex, evidence.Location.RowIndex);
        Assert.Equal((bytes.Length / format.PageSize) - 1, evidence.Location.DataPageNumber);
        Assert.Equal(0, evidence.Location.DataRowIndex);
        Assert.True(evidence.Location.IsOverflow);
        Assert.True(recovered.OverflowHops >= (chained ? 2 : 1));
        Assert.False(evidence.HasDamagedMovedOwner);
        Assert.Equal(original, bytes);
    }

    [Theory]
    [InlineData("Jet3Test.mdb", false)]
    [InlineData("Jet3Test.mdb", true)]
    [InlineData("Jackcess/adox_jet4.mdb", false)]
    [InlineData("Jackcess/adox_jet4.mdb", true)]
    [InlineData("Jackcess/V2007/complexDataTestV2007.accdb", false)]
    [InlineData("Jackcess/V2007/complexDataTestV2007.accdb", true)]
    public async Task NativeIdIndexRecoversMovedCatalogIdentityWithDamagedPayloadOwners(string relativePath, bool chained)
    {
        (byte[] bytes, JetFormat format, CatalogRow target) = await CreateMovedCatalogAsync(relativePath, chained);
        DamageMovedOwners(bytes, format, chained ? 2 : 1);
        byte[] original = bytes.ToArray();
        CompactCatalogRecovery.Result recovered = await RecoverAsync(bytes, format);
        CompactCatalogRecovery.RecoveredRow evidence = Assert.Single(recovered.Rows, row => row.Identity == target);
        Assert.Equal((bytes.Length / format.PageSize) - 1, evidence.Location.DataPageNumber);
        Assert.True(evidence.Location.IsOverflow);
        Assert.True(evidence.HasDamagedMovedOwner);
        Assert.Empty(recovered.Diagnostics);
        Assert.Equal(original, bytes);
    }

    [Theory]
    [InlineData("live-payload")]
    [InlineData("cycle")]
    [InlineData("missing-payload")]
    [InlineData("key-mismatch")]
    [InlineData("payload-alias")]
    [InlineData("payload-start-alias")]
    [InlineData("payload-live-alias")]
    [InlineData("payload-intermediate-alias")]
    public async Task MovedCatalogIdentityRefusesUnprovenOrAmbiguousPayload(string damage)
    {
        (byte[] bytes, JetFormat format, CatalogRow target) = await CreateMovedCatalogAsync("Jackcess/adox_jet4.mdb", chained: damage == "payload-intermediate-alias");
        int header = checked((int)target.PageNumber * format.PageSize);
        int payload = bytes.Length - format.PageSize;
        int targetSlot = header + format.DataPage.RowsStart + (target.RowIndex * 2);
        int headerStart = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(targetSlot, 2)) & Constants.DataPage.RowOffsetMask;
        int payloadStart = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(payload + format.DataPage.RowsStart, 2)) & Constants.DataPage.RowOffsetMask;
        switch (damage)
        {
            case "live-payload":
                BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(payload + format.DataPage.RowsStart, 2), checked((ushort)payloadStart));
                break;
            case "cycle":
                BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(payload + format.DataPage.RowsStart, 2), checked((ushort)(payloadStart | Constants.DataPage.NonLiveRowFlags)));
                WritePointer(bytes.AsSpan(payload + payloadStart, 4), payload / format.PageSize, 0);
                break;
            case "missing-payload":
                WritePointer(bytes.AsSpan(header + headerStart, 4), bytes.Length / format.PageSize, 0);
                break;
            case "key-mismatch":
                bytes[payload + payloadStart + format.RowFields.NumCols] ^= 1;
                break;
            default:
                int count = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(header + format.DataPage.NumRows, 2));
                int other = Enumerable.Range(0, count).First(row => row != target.RowIndex
                    && (BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(header + format.DataPage.RowsStart + (row * 2), 2)) & Constants.DataPage.NonLiveRowFlags) == 0);
                CompactCatalogRecovery.Result beforeAlias = await RecoverAsync(bytes, format);
                CompactCatalogRecovery.RecoveredRow later = Assert.Single(beforeAlias.Rows, row => row.Identity.PageNumber == target.PageNumber && row.Identity.RowIndex == other);
                Assert.True(target.Id < later.Identity.Id);
                int slot = header + format.DataPage.RowsStart + (other * 2);
                int start = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(slot, 2)) & Constants.DataPage.RowOffsetMask;
                BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(slot, 2), checked((ushort)(start | Constants.DataPage.OverflowRowFlag)));
                byte payloadRow = 0;
                if (damage is "payload-start-alias" or "payload-live-alias")
                {
                    BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(payload + format.DataPage.NumRows, 2), 2);
                    int flags = damage == "payload-live-alias" ? 0 : Constants.DataPage.DeletedRowFlag;
                    BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(payload + format.DataPage.RowsStart + 2, 2), checked((ushort)(payloadStart | flags)));
                    payloadRow = 1;
                }

                WritePointer(bytes.AsSpan(header + start, 4), (payload / format.PageSize) - (damage == "payload-intermediate-alias" ? 1 : 0), payloadRow);
                break;
        }

        DamageMovedOwners(bytes, format, damage == "payload-intermediate-alias" ? 2 : 1);
        byte[] original = bytes.ToArray();
        CompactCatalogRecovery.Result recovered = await RecoverAsync(bytes, format);
        Assert.DoesNotContain(recovered.Rows, row => row.Identity == target);
        Assert.NotEmpty(recovered.Diagnostics);
        Assert.Equal(original, bytes);
    }

    [Theory]
    [InlineData("owner")]
    [InlineData("leaf-chain")]
    [InlineData("incomplete-chain")]
    [InlineData("late-read")]
    public async Task DamagedMovedPayloadOwnersRequireCompletelyAuditedIndependentIndexEvidence(string damage)
    {
        (byte[] bytes, JetFormat format, CatalogRow target) = await CreateMovedCatalogAsync("Jackcess/adox_jet4.mdb", chained: true);
        CompactCatalogRecovery.Result intactIndex = await RecoverAsync(bytes, format);
        long indexPage = Assert.Single(intactIndex.Rows, row => row.Identity == target).IndexPage;
        DamageMovedOwners(bytes, format, 2);
        if (damage == "owner")
        {
            BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(checked((int)indexPage * format.PageSize) + 4, 4), 0);
        }
        else if (damage == "leaf-chain")
        {
            BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(checked((int)indexPage * format.PageSize) + format.IndexPage.NextPageOffset, 4), checked((uint)indexPage));
        }

        if (damage == "incomplete-chain")
        {
            BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(checked((int)indexPage * format.PageSize) + format.IndexPage.PrevPageOffset, 4), checked((uint)indexPage));
        }

        byte[] original = bytes.ToArray();
        bool reachedPayload = false;
        CompactCatalogRecovery.Result recovered = await RecoverAsync(
            bytes,
            format,
            decorateRecovery: pages => new ObservedPages(pages, number =>
            {
                if (damage == "late-read" && reachedPayload && number != (bytes.Length / format.PageSize) - 1)
                {
                    throw new JetCorruptDataException(JetErrorCode.CorruptIndex, "The independent index evidence is corrupt after a positive candidate.");
                }

                reachedPayload |= number == (bytes.Length / format.PageSize) - 1;
            }));
        Assert.DoesNotContain(recovered.Rows, row => row.Identity == target);
        Assert.NotEmpty(recovered.Diagnostics);
        if (damage == "late-read")
        {
            Assert.True(reachedPayload);
        }

        Assert.Equal(original, bytes);
    }

    [Fact]
    public async Task MovedCatalogRecoveryChargesOverflowReferencesBeforeFollowingThem()
    {
        (byte[] bytes, JetFormat format, _) = await CreateMovedCatalogAsync("Jackcess/adox_jet4.mdb", chained: true);
        DamageMovedOwners(bytes, format, 2);
        await using DatabaseFile damaged = Open(bytes);
        var pages = new CompactInventoryPageSource(damaged.Pages, format, 100_000);
        using var definitions = new TableDefReader(pages, format, cacheResults: false);
        using var owned = new OwnedDataPages(pages, format, cacheResults: false);
        var rows = new CatalogRowReader(format, definitions, owned);
        TDefImage catalog = Assert.IsType<TDefImage>(await definitions.ReadImageAsync(2, Ct));
        List<CatalogRow> physical = await rows.GetCatalogRowsAsync(catalog.Definition, Ct, scanPhysicalPages: true);
        CompactCatalogRecovery.Result expected = await CompactCatalogRecovery.ScanAsync(pages, format, catalog, rows, new(), physical, 100_000, 100_000, 512L * 1024 * 1024, Ct);
        Assert.Equal(2, expected.OverflowHops);
        await Assert.ThrowsAsync<JetLimitationException>(async () => await CompactCatalogRecovery.ScanAsync(pages, format, catalog, rows, new(), physical, expected.References - 1, 100_000, 512L * 1024 * 1024, Ct));
    }

    [Theory]
    [InlineData(8)]
    [InlineData(9)]
    public async Task MovedCatalogRecoveryHonorsFixedOverflowHopBound(int hops)
    {
        (byte[] bytes, JetFormat format, CatalogRow target) = await CreateMovedCatalogAsync("Jackcess/adox_jet4.mdb", chained: false, additionalHops: hops - 1);
        DamageMovedOwners(bytes, format, hops);
        if (hops == Constants.DataPage.MaxOverflowHops)
        {
            CompactCatalogRecovery.Result recovered = await RecoverAsync(bytes, format);
            Assert.Contains(recovered.Rows, row => row.Identity == target);
            Assert.Equal(hops, recovered.OverflowHops);
        }
        else
        {
            await Assert.ThrowsAsync<JetLimitationException>(async () => await RecoverAsync(bytes, format));
        }
    }

    [Fact]
    public async Task MovedCatalogRecoveryStopsForCancellationAndPartialHopWorkAllowance()
    {
        (byte[] bytes, JetFormat format, CatalogRow target) = await CreateMovedCatalogAsync("Jackcess/adox_jet4.mdb", chained: true);
        DamageMovedOwners(bytes, format, 2);
        CompactCatalogRecovery.Result expected = await RecoverAsync(bytes, format);
        CompactCatalogRecovery.RecoveredRow moved = Assert.Single(expected.Rows, row => row.Identity == target);
        long directoryAndHopSteps = expected.DirectorySlots + expected.OverflowHops;
        await Assert.ThrowsAsync<JetLimitationException>(async () => await RecoverAsync(bytes, format, maxDirectorySlots: directoryAndHopSteps - 1));
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        bool reachedPayload = false;
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await RecoverAsync(
            bytes,
            format,
            decorateRecovery: pages => new ObservedPages(pages, number =>
            {
                if (number == moved.Location.DataPageNumber)
                {
                    reachedPayload = true;
                    cancellation.Cancel();
                }
            }),
            recoveryCancellation: cancellation.Token));
        Assert.True(reachedPayload);
    }

    [Fact]
    public async Task DamagedMovedOwnerRecoveryRequiresAuditToReturnAfterTerminalObservation()
    {
        (byte[] bytes, JetFormat format, CatalogRow target) = await CreateMovedCatalogAsync("Jackcess/adox_jet4.mdb", chained: true);
        DamageMovedOwners(bytes, format, 2);
        int checks = 0;
        CompactCatalogRecovery.Result complete = await RecoverAsync(bytes, format, decorateRecovery: pages => new ObservedPages(pages, static _ => { }, () => checks++));
        Assert.Contains(complete.Rows, row => row.Identity == target && row.HasDamagedMovedOwner);
        Assert.True(checks > 0);
        int repeatedChecks = 0;
        CompactCatalogRecovery.Result interrupted = await RecoverAsync(bytes, format, decorateRecovery: pages => new ObservedPages(
            pages,
            static _ => { },
            () =>
            {
                if (++repeatedChecks == checks)
                {
                    throw new JetCorruptDataException(JetErrorCode.CorruptIndex, "Injected corruption at the final post-terminal source guard.");
                }
            }));
        Assert.Equal(checks, repeatedChecks);
        Assert.DoesNotContain(interrupted.Rows, row => row.Identity == target);
        Assert.NotEmpty(interrupted.Diagnostics);
    }

    private static async Task<(byte[] Bytes, JetFormat Format, CatalogRow Target)> CreateMovedCatalogAsync(string relativePath, bool chained, int additionalHops = 0)
    {
        byte[] bytes = await File.ReadAllBytesAsync(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Databases", relativePath), Ct);
        CatalogRow target;
        JetFormat format;
        await using (DatabaseFile intact = Open(bytes))
        {
            format = intact.Format;
            TDefImage image = Assert.IsType<TDefImage>(await intact.TableDefs.ReadImageAsync(2, Ct));
            var decoder = new CatalogRowReader(format, intact.TableDefs, intact.OwnedPages);
            target = (await decoder.GetCatalogRowsAsync(image.Definition, Ct, scanPhysicalPages: true)).First(row =>
            {
                int slot = checked(((int)row.PageNumber * format.PageSize) + format.DataPage.RowsStart + (row.RowIndex * 2));
                return (BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(slot, 2)) & Constants.DataPage.NonLiveRowFlags) == 0;
            });
        }

        int header = checked((int)target.PageNumber * format.PageSize);
        int slotOffset = header + format.DataPage.RowsStart + (target.RowIndex * 2);
        int start = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(slotOffset, 2));
        int end = format.PageSize;
        int count = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(header + format.DataPage.NumRows, 2));
        for (int row = 0; row < count; row++)
        {
            int candidate = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(header + format.DataPage.RowsStart + (row * 2), 2)) & Constants.DataPage.RowOffsetMask;
            if (candidate > start && candidate < end)
            {
                end = candidate;
            }
        }

        byte[] stored = bytes.AsSpan(header + start, end - start).ToArray();
        int firstPayload = bytes.Length / format.PageSize;
        int hops = (chained ? 2 : 1) + additionalHops;
        Array.Resize(ref bytes, bytes.Length + (hops * format.PageSize));
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(header + format.DataPage.TDefOff, 4), 0);
        BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(slotOffset, 2), checked((ushort)(start | Constants.DataPage.OverflowRowFlag)));
        WritePointer(bytes.AsSpan(header + start, 4), firstPayload, 0);
        int payload = firstPayload * format.PageSize;
        for (int hop = 0; hop < hops - 1; hop++)
        {
            byte[] pointer = new byte[4];
            WritePointer(pointer, firstPayload + hop + 1, 0);
            SetPayload(bytes.AsSpan(payload, format.PageSize), format, pointer, Constants.DataPage.NonLiveRowFlags);
            payload += format.PageSize;
        }

        SetPayload(bytes.AsSpan(payload, format.PageSize), format, stored, Constants.DataPage.DeletedRowFlag);
        bytes.AsSpan((2 * format.PageSize) + format.TDef.UsedPages, 4).Clear();
        bytes.AsSpan((2 * format.PageSize) + format.TDef.NumRows, 4).Clear();
        return (bytes, format, target);
    }

    private static void DamageMovedOwners(byte[] bytes, JetFormat format, int hops)
    {
        for (int hop = 1; hop <= hops; hop++)
        {
            BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(bytes.Length - (hop * format.PageSize) + format.DataPage.TDefOff, 4), 0);
        }
    }

    private static void SetPayload(Span<byte> page, JetFormat format, ReadOnlySpan<byte> stored, int flags)
    {
        page[0] = Constants.PageTypes.Data;
        BinaryPrimitives.WriteUInt32LittleEndian(page.Slice(format.DataPage.TDefOff, 4), 2);
        BinaryPrimitives.WriteUInt16LittleEndian(page.Slice(format.DataPage.NumRows, 2), 1);
        int offset = format.PageSize - stored.Length;
        stored.CopyTo(page[offset..]);
        BinaryPrimitives.WriteUInt16LittleEndian(page.Slice(format.DataPage.RowsStart, 2), checked((ushort)(offset | flags)));
    }

    private static void WritePointer(Span<byte> pointer, int page, byte row)
    {
        pointer[0] = row;
        pointer[1] = checked((byte)(page & 0xFF));
        pointer[2] = checked((byte)((page >> 8) & 0xFF));
        pointer[3] = checked((byte)((page >> 16) & 0xFF));
    }

    private static async Task<CompactCatalogRecovery.Result> RecoverAsync(byte[] bytes, JetFormat format, long maxDirectorySlots = 100_000, Func<IPageSource, IPageSource>? decorateRecovery = null, CancellationToken? recoveryCancellation = null)
    {
        await using DatabaseFile damaged = Open(bytes);
        var pages = new CompactInventoryPageSource(damaged.Pages, format, 100_000);
        using var definitions = new TableDefReader(pages, format, cacheResults: false);
        using var owned = new OwnedDataPages(pages, format, cacheResults: false);
        var rows = new CatalogRowReader(format, definitions, owned);
        TDefImage catalog = Assert.IsType<TDefImage>(await definitions.ReadImageAsync(2, Ct));
        List<CatalogRow> physical = await rows.GetCatalogRowsAsync(catalog.Definition, Ct, scanPhysicalPages: true);
        return await CompactCatalogRecovery.ScanAsync(decorateRecovery?.Invoke(pages) ?? pages, format, catalog, rows, new(), physical, 100_000, maxDirectorySlots, 512L * 1024 * 1024, recoveryCancellation ?? Ct);
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static DatabaseFile Open(byte[] bytes)
        => DatabaseFile.ForReader(new MemoryStream(bytes, writable: false), bytes.AsSpan(0, Math.Min(4096, bytes.Length)).ToArray(), ReadOnlyMemory<char>.Empty, string.Empty, leaveOpen: false);

    private sealed class ObservedPages(IPageSource inner, Action<long> beforeRead, Action? onCheck = null) : IPageSource
    {
        public int PageSize => inner.PageSize;

        public long PageCount => inner.PageCount;

        public bool IsDisposed => inner.IsDisposed;

        public ValueTask<byte[]> ReadPageAsync(long pageNumber, CancellationToken cancellationToken = default)
        {
            beforeRead(pageNumber);
            return inner.ReadPageAsync(pageNumber, cancellationToken);
        }

        public void ThrowIfDisposed() => inner.ThrowIfDisposed();

        public void ThrowIfDisposedOrCancelled(CancellationToken cancellationToken)
        {
            inner.ThrowIfDisposedOrCancelled(cancellationToken);
            onCheck?.Invoke();
        }
    }
}
