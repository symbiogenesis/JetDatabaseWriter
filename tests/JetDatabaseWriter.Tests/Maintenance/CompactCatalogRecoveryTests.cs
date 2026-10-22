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
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Maintenance;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.ValueDecoding;
using JetDatabaseWriter.ValueDecoding.Models;
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
    [InlineData("Jet3Test.mdb", false)]
    [InlineData("Jet3Test.mdb", true)]
    [InlineData("Jackcess/adox_jet4.mdb", false)]
    [InlineData("Jackcess/adox_jet4.mdb", true)]
    [InlineData("Jackcess/V2007/complexDataTestV2007.accdb", false)]
    [InlineData("Jackcess/V2007/complexDataTestV2007.accdb", true)]
    public async Task InventoryRecoversIntactCatalogHeaderWhoseMovedPayloadOwnersAreDamaged(string relativePath, bool chained)
    {
        (byte[] bytes, JetFormat format, CatalogRow target) = await CreateMovedCatalogAsync(relativePath, chained);
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(checked((int)target.PageNumber * format.PageSize) + format.DataPage.TDefOff, 4), 2);
        DamageMovedOwners(bytes, format, chained ? 2 : 1);
        byte[] original = bytes.ToArray();
        await using DatabaseFile damaged = Open(bytes);
        var records = new List<CompactStorageRecord>();
        await CompactStorageInventory.ScanAsync(
            damaged.Pages,
            format,
            damaged.TableDefs,
            new(),
            (record, _) =>
            {
                records.Add(record);
                return ValueTask.CompletedTask;
            },
            Ct);
        Assert.Contains(records, record => record.Kind == "catalog-object" && record.ObjectId == target.Id
            && record.PageNumber == target.PageNumber && record.TargetRowIndex == target.RowIndex);
        Assert.Contains(records, record => record.Role == "catalog-recovered-bookmark" && record.ObjectId == target.Id);
        Assert.All(records, record => Assert.True(record.IsPinned));
        Assert.Equal(original, bytes);
    }

    [Theory]
    [InlineData("Jet3Test.mdb", false)]
    [InlineData("Jet3Test.mdb", true)]
    [InlineData("Jackcess/adox_jet4.mdb", false)]
    [InlineData("Jackcess/adox_jet4.mdb", true)]
    [InlineData("Jackcess/V2007/complexDataTestV2007.accdb", false)]
    [InlineData("Jackcess/V2007/complexDataTestV2007.accdb", true)]
    public async Task ParentIdNameIndexRecoversCatalogIdentityWhenIdEvidenceIsUnavailable(string relativePath, bool missingIdRoot)
    {
        (byte[] bytes, JetFormat format, CatalogRow target) = await CreateMovedCatalogAsync(relativePath, chained: true);
        DamageMovedOwners(bytes, format, 2);
        await using (DatabaseFile file = Open(bytes))
        {
            TDefImage catalog = Assert.IsType<TDefImage>(await file.TableDefs.ReadImageAsync(2, Ct));
            int idColumn = catalog.Definition.FindColumn("Id")!.ColNum;
            TDefIndexDescriptor id = Assert.Single(catalog.RealIndexes, index => index.Columns.Count == 1 && index.Columns[0].ColNum == idColumn);
            Assert.InRange(id.Slot.FirstDpOffset, 0, format.PageSize - 4);
            int damageOffset = missingIdRoot
                ? (2 * format.PageSize) + id.Slot.FirstDpOffset
                : checked((int)id.RootPage * format.PageSize) + 4;
            bytes.AsSpan(damageOffset, 4).Clear();
        }

        byte[] original = bytes.ToArray();
        CompactCatalogRecovery.Result recovered = await RecoverAsync(bytes, format);
        CompactCatalogRecovery.RecoveredRow evidence = Assert.Single(recovered.Rows, row => row.Identity == target);
        Assert.True(evidence.HasDamagedMovedOwner);
        Assert.Equal("ParentIdName", evidence.IdentityKey);
        Assert.NotEmpty(recovered.Diagnostics);
        Assert.Equal(original, bytes);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task LaterIndependentIdentityDescriptorRecoversWhenEarlierKeyPathsAreDamaged(bool secondary)
    {
        (byte[] bytes, JetFormat format, CatalogRow target) = await CreateMovedCatalogAsync("Jackcess/adox_jet4.mdb", chained: true);
        DamageMovedOwners(bytes, format, 2);
        await using DatabaseFile file = Open(bytes);
        TDefImage catalog = Assert.IsType<TDefImage>(await file.TableDefs.ReadImageAsync(2, Ct));
        ColumnInfo id = catalog.Definition.FindColumn("Id")!;
        ColumnInfo parent = catalog.Definition.FindColumn("ParentId")!;
        ColumnInfo name = catalog.Definition.FindColumn("Name")!;
        TDefIndexDescriptor original = Assert.Single(catalog.RealIndexes, index => secondary ? index.Columns.Count == 2 : index.Columns.Count == 1 && index.Columns[0].ColNum == id.ColNum);
        byte[] key = secondary
            ? [.. IndexKeyEncoder.EncodeColumnEntry(format, parent, checked((int)target.ParentId), original.Columns[0].Ascending), .. IndexKeyEncoder.EncodeColumnEntry(format, name, target.Name, original.Columns[1].Ascending)]
            : IndexKeyEncoder.EncodeColumnEntry(format, id, checked((int)target.Id), original.Columns[0].Ascending);
        foreach (TDefIndexDescriptor index in catalog.RealIndexes)
        {
            bytes.AsSpan(checked((int)index.RootPage * format.PageSize) + 4, 4).Clear();
        }

        uint replacement = checked((uint)(bytes.Length / format.PageSize));
        Array.Resize(ref bytes, bytes.Length + format.PageSize);
        IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, [new(key, target.PageNumber, checked((byte)target.RowIndex))], enablePrefixCompression: true).CopyTo(bytes, checked((int)replacement * format.PageSize));
        var alternate = new TDefIndexDescriptor(original.Slot, replacement, original.Columns.ToList(), original.RawDescriptor.ToArray());
        TDefImage AddAlternate(TDefImage image) => new(image.CopyBytes(), image.Header, image.Columns.ToList(), image.HasDeletedColumns, image.Issues, image.IndexSection, [.. image.RealIndexes, alternate], image.LogicalIndexes.ToList());
        byte[] unchanged = bytes.ToArray();
        CompactCatalogRecovery.Result undeclared = await RecoverAsync(bytes, format);
        Assert.Empty(undeclared.Rows);
        Assert.NotEmpty(undeclared.Diagnostics);
        CompactCatalogRecovery.Result recovered = await RecoverAsync(bytes, format, alterCatalog: AddAlternate);
        CompactCatalogRecovery.RecoveredRow evidence = Assert.Single(recovered.Rows, row => row.Identity == target);
        Assert.Equal(secondary ? "ParentIdName" : "Id", evidence.IdentityKey);
        Assert.Equal(replacement, evidence.IndexPage);
        Assert.True(evidence.HasDamagedMovedOwner);
        Assert.Contains(recovered.Diagnostics, diagnostic => diagnostic.Reason.Contains("cannot be completely audited", StringComparison.Ordinal));
        Assert.Equal(unchanged, bytes);
        await Assert.ThrowsAsync<JetLimitationException>(async () => await RecoverAsync(bytes, format, maxReferences: recovered.References - 1, alterCatalog: AddAlternate));
        await Assert.ThrowsAsync<JetLimitationException>(async () => await RecoverAsync(bytes, format, maxDirectorySlots: recovered.DirectorySlots + recovered.OverflowHops - 1, alterCatalog: AddAlternate));
    }

    [Theory]
    [InlineData("live-payload", false)]
    [InlineData("cycle", false)]
    [InlineData("missing-payload", false)]
    [InlineData("key-mismatch", false)]
    [InlineData("payload-alias", false)]
    [InlineData("payload-start-alias", false)]
    [InlineData("payload-live-alias", false)]
    [InlineData("payload-intermediate-alias", false)]
    [InlineData("live-payload", true)]
    [InlineData("cycle", true)]
    [InlineData("missing-payload", true)]
    [InlineData("key-mismatch", true)]
    [InlineData("payload-alias", true)]
    [InlineData("payload-start-alias", true)]
    [InlineData("payload-live-alias", true)]
    [InlineData("payload-intermediate-alias", true)]
    public async Task MovedCatalogIdentityRefusesUnprovenOrAmbiguousPayload(string damage, bool secondary)
    {
        (byte[] bytes, JetFormat format, CatalogRow target) = await CreateMovedCatalogAsync("Jackcess/adox_jet4.mdb", chained: damage == "payload-intermediate-alias");
        if (secondary)
        {
            await RemoveIdRootAsync(bytes, format);
        }

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
                int field = format.RowFields.NumCols;
                if (secondary)
                {
                    await using DatabaseFile file = Open(bytes);
                    TDefImage catalog = Assert.IsType<TDefImage>(await file.TableDefs.ReadImageAsync(2, Ct));
                    field += catalog.Definition.FindColumn("ParentId")!.FixedOff;
                }

                bytes[payload + payloadStart + field] ^= 1;
                break;
            default:
                int count = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(header + format.DataPage.NumRows, 2));
                int other = Enumerable.Range(0, count).First(row => row != target.RowIndex
                    && (BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(header + format.DataPage.RowsStart + (row * 2), 2)) & Constants.DataPage.NonLiveRowFlags) == 0);
                CompactCatalogRecovery.Result beforeAlias = await RecoverAsync(bytes, format);
                CompactCatalogRecovery.RecoveredRow later = Assert.Single(beforeAlias.Rows, row => row.Identity.PageNumber == target.PageNumber && row.Identity.RowIndex == other);
                Assert.True(secondary || target.Id < later.Identity.Id);
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
        CompactCatalogRecovery.Result recovered = await RecoverAsync(bytes, format, allowSecondaryIndex: secondary);
        Assert.DoesNotContain(recovered.Rows, row => row.Identity.PageNumber == target.PageNumber && row.Identity.RowIndex == target.RowIndex);
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
            allowSecondaryIndex: false,
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
    [InlineData("late-read")]
    [InlineData("references")]
    [InlineData("directory")]
    [InlineData("cancel")]
    [InlineData("io")]
    public async Task SecondaryIdentityRecoveryRequiresCompleteAuditAndAggregateWork(string failure)
    {
        (byte[] bytes, JetFormat format, CatalogRow target) = await CreateMovedCatalogAsync("Jackcess/adox_jet4.mdb", chained: true);
        DamageMovedOwners(bytes, format, 2);
        await RemoveIdRootAsync(bytes, format);
        CompactCatalogRecovery.Result expected = await RecoverAsync(bytes, format);
        Assert.Contains(expected.Rows, row => row.Identity == target && row.IdentityKey == "ParentIdName");
        if (failure == "directory")
        {
            await Assert.ThrowsAsync<JetLimitationException>(async () => await RecoverAsync(bytes, format, maxDirectorySlots: expected.DirectorySlots + expected.OverflowHops - 1));
            return;
        }

        if (failure == "references")
        {
            await Assert.ThrowsAsync<JetLimitationException>(async () => await RecoverAsync(bytes, format, maxReferences: expected.References - 1));
            return;
        }

        bool reachedPayload = false;
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        var io = new IOException("Secondary identity evidence read failed.");
        IPageSource Observe(IPageSource pages) => new ObservedPages(pages, number =>
        {
            if (reachedPayload && number != (bytes.Length / format.PageSize) - 1)
            {
                if (failure == "io")
                {
                    throw io;
                }

                if (failure == "cancel")
                {
                    cancellation.Cancel();
                }
                else
                {
                    throw new JetCorruptDataException(JetErrorCode.CorruptIndex, "Secondary identity evidence is corrupt after its positive candidate.");
                }
            }

            reachedPayload |= number == (bytes.Length / format.PageSize) - 1;
        });
        Task<CompactCatalogRecovery.Result> RunAsync() => RecoverAsync(bytes, format, decorateRecovery: Observe, recoveryCancellation: cancellation.Token);
        if (failure == "io")
        {
            Assert.Same(io, await Assert.ThrowsAsync<IOException>(RunAsync));
        }
        else if (failure == "cancel")
        {
            await Assert.ThrowsAnyAsync<OperationCanceledException>(RunAsync);
        }
        else
        {
            CompactCatalogRecovery.Result recovered = await RunAsync();
            Assert.DoesNotContain(recovered.Rows, row => row.Identity == target);
            Assert.NotEmpty(recovered.Diagnostics);
        }

        Assert.True(reachedPayload);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task IdentityRecoveryRefusesIncompleteNativeNameCodeUnits(bool compressed, bool secondary)
    {
        (byte[] bytes, JetFormat format, CatalogRow target) = await CreateMovedCatalogAsync("Jackcess/adox_jet4.mdb", chained: false);
        if (secondary)
        {
            await RemoveIdRootAsync(bytes, format);
        }

        await using DatabaseFile file = Open(bytes);
        TDefImage catalog = Assert.IsType<TDefImage>(await file.TableDefs.ReadImageAsync(2, Ct));
        catalog = WithNameCompression(catalog, compressed);
        ColumnInfo name = catalog.Definition.FindColumn("Name")!;
        int payload = bytes.Length - format.PageSize;
        int start = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(payload + format.DataPage.RowsStart, 2)) & Constants.DataPage.RowOffsetMask;
        byte[] originalRow = bytes.AsSpan(payload + start).ToArray();
        Assert.True(RowDecodePlan.TryParseRowLayout(format.RowFields, originalRow, 0, originalRow.Length, hasVarColumns: true, out RowLayout layout));
        ColumnSlice slice = RowDecodePlan.ResolveColumnSlice(format.RowFields, originalRow, 0, originalRow.Length, layout, name);
        byte[] encodedName = format.EncodeText(target.Name, compress: compressed);
        byte[] incompleteName = compressed ? [.. encodedName, 0, 0x41] : [.. encodedName, 0x41];
        int delta = incompleteName.Length - slice.DataLen;
        byte[] changed = new byte[originalRow.Length + delta];
        originalRow.AsSpan(0, slice.DataStart).CopyTo(changed);
        incompleteName.CopyTo(changed, slice.DataStart);
        originalRow.AsSpan(slice.DataStart + slice.DataLen).CopyTo(changed.AsSpan(slice.DataStart + incompleteName.Length));
        BinaryPrimitives.WriteUInt16LittleEndian(changed.AsSpan(layout.VarTableStart - 2 + delta, 2), checked((ushort)(layout.Eod + delta)));
        for (int variable = 0; variable < layout.VarLen; variable++)
        {
            int oldOffset = layout.VarTableStart + ((layout.VarLen - 1 - variable) * 2);
            int value = BinaryPrimitives.ReadUInt16LittleEndian(originalRow.AsSpan(oldOffset, 2));
            BinaryPrimitives.WriteUInt16LittleEndian(changed.AsSpan(oldOffset + delta, 2), checked((ushort)(value + (variable > name.VarIdx ? delta : 0))));
        }

        Assert.Equal(target.Name, ScalarColumnReader.DecodeSimpleColumnValue(format, changed, 0, changed.Length, name));
        SetPayload(bytes.AsSpan(payload, format.PageSize), format, changed, Constants.DataPage.DeletedRowFlag);
        DamageMovedOwners(bytes, format, 1);
        CompactCatalogRecovery.Result recovered = await RecoverAsync(bytes, format, alterCatalog: image => WithNameCompression(image, compressed));
        Assert.DoesNotContain(recovered.Rows, row => row.Identity.PageNumber == target.PageNumber && row.Identity.RowIndex == target.RowIndex);
        Assert.Contains(recovered.Diagnostics, diagnostic => diagnostic.Reason.Contains("incomplete", StringComparison.Ordinal));
    }

    [Fact]
    public async Task SecondaryIdentityRecoveryRefusesUnsupportedNameCollation()
    {
        (byte[] bytes, JetFormat format, _) = await CreateMovedCatalogAsync("Jackcess/adox_jet4.mdb", chained: true);
        DamageMovedOwners(bytes, format, 2);
        await RemoveIdRootAsync(bytes, format);
        CompactCatalogRecovery.Result recovered = await RecoverAsync(bytes, format, alterCatalog: catalog => new(
            catalog.CopyBytes(),
            catalog.Header,
            catalog.Columns.Select(column => column.Name == "Name" ? column with { TextSortOrder = new(0x0411, 0, true) } : column).ToList(),
            catalog.HasDeletedColumns,
            catalog.Issues,
            catalog.IndexSection,
            catalog.RealIndexes.ToList(),
            catalog.LogicalIndexes.ToList()));
        Assert.Empty(recovered.Rows);
        Assert.Contains(recovered.Diagnostics, diagnostic => diagnostic.Reason.Contains("collation is unsupported", StringComparison.Ordinal));
    }

    [Fact]
    public async Task SecondaryIdentityRecoveryRequiresNativeSignedInt32IdStorage()
    {
        (byte[] bytes, JetFormat format, _) = await CreateMovedCatalogAsync("Jackcess/adox_jet4.mdb", chained: false);
        await RemoveIdRootAsync(bytes, format);
        DamageMovedOwners(bytes, format, 1);
        CompactCatalogRecovery.Result recovered = await RecoverAsync(bytes, format, alterCatalog: image => new(
            image.CopyBytes(),
            image.Header,
            image.Columns.Select(column => column.Name == "Id" ? column with { Type = ColumnType.BigIntType, Size = 8 } : column).ToList(),
            image.HasDeletedColumns,
            image.Issues,
            image.IndexSection,
            image.RealIndexes.ToList(),
            image.LogicalIndexes.ToList()));
        Assert.Empty(recovered.Rows);
        Assert.NotEmpty(recovered.Diagnostics);
    }

    [Theory]
    [InlineData("ParentId")]
    [InlineData("Type")]
    [InlineData("Flags")]
    public async Task PrimaryIdentityRecoveryRequiresNativeIdentityFieldStorage(string field)
    {
        (byte[] bytes, JetFormat format, CatalogRow target) = await CreateMovedCatalogAsync("Jackcess/adox_jet4.mdb", chained: false);
        CompactCatalogRecovery.Result recovered = await RecoverAsync(bytes, format, allowSecondaryIndex: false, alterCatalog: image => new(
            image.CopyBytes(),
            image.Header,
            image.Columns.Select(column => column.Name == field ? column with { Type = ColumnType.BinaryType } : column).ToList(),
            image.HasDeletedColumns,
            image.Issues,
            image.IndexSection,
            image.RealIndexes.ToList(),
            image.LogicalIndexes.ToList()));
        Assert.DoesNotContain(recovered.Rows, row => row.Identity.PageNumber == target.PageNumber && row.Identity.RowIndex == target.RowIndex);
        Assert.NotEmpty(recovered.Diagnostics);
    }

    [Fact]
    public async Task PrimaryIdentityRecoveryRefusesOverlappingNativeIdentityFields()
    {
        (byte[] bytes, JetFormat format, CatalogRow target) = await CreateMovedCatalogAsync("Jackcess/adox_jet4.mdb", chained: false);
        CompactCatalogRecovery.Result recovered = await RecoverAsync(bytes, format, allowSecondaryIndex: false, alterCatalog: image => new(
            image.CopyBytes(),
            image.Header,
            image.Columns.Select(column => column.Name == "ParentId" ? column with { FixedOff = image.Definition.FindColumn("Id")!.FixedOff } : column).ToList(),
            image.HasDeletedColumns,
            image.Issues,
            image.IndexSection,
            image.RealIndexes.ToList(),
            image.LogicalIndexes.ToList()));
        Assert.DoesNotContain(recovered.Rows, row => row.Identity.PageNumber == target.PageNumber && row.Identity.RowIndex == target.RowIndex);
        Assert.NotEmpty(recovered.Diagnostics);
    }

    [Theory]
    [InlineData("iD")]
    [InlineData("PARENTid")]
    [InlineData("nAmE")]
    [InlineData("TYPE")]
    [InlineData("flags")]
    public async Task IdentityRecoveryRefusesDuplicateRequiredDescriptorNames(string field)
    {
        (byte[] bytes, JetFormat format, CatalogRow target) = await CreateMovedCatalogAsync("Jackcess/adox_jet4.mdb", chained: false);
        CompactCatalogRecovery.Result recovered = await RecoverAsync(bytes, format, allowSecondaryIndex: false, alterCatalog: image => WithDuplicateIdentityColumn(image, field));
        Assert.DoesNotContain(recovered.Rows, row => row.Identity.PageNumber == target.PageNumber && row.Identity.RowIndex == target.RowIndex);
        Assert.Contains(recovered.Diagnostics, diagnostic => diagnostic.Reason.Contains("duplicate required", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData("iD")]
    [InlineData("PARENTid")]
    [InlineData("nAmE")]
    [InlineData("TYPE")]
    [InlineData("flags")]
    public async Task PhysicalIdentityScanRefusesDuplicateRequiredDescriptorNames(string field)
    {
        (byte[] bytes, JetFormat format, CatalogRow target) = await CreateMovedCatalogAsync("Jackcess/adox_jet4.mdb", chained: false);
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(checked((int)target.PageNumber * format.PageSize) + format.DataPage.TDefOff, 4), 2);
        await using DatabaseFile file = Open(bytes);
        TDefImage catalog = WithDuplicateIdentityColumn(Assert.IsType<TDefImage>(await file.TableDefs.ReadImageAsync(2, Ct)), field);
        var pages = new CompactInventoryPageSource(file.Pages, format, 100_000);
        using var definitions = new TableDefReader(pages, format, cacheResults: false);
        using var owned = new OwnedDataPages(pages, format, cacheResults: false);
        var decoder = new CatalogRowReader(format, definitions, owned);
        JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CompactCatalogRecovery.ScanPhysicalAsync(pages, format, catalog, decoder, new(), 100_000, 100_000, Ct));
        Assert.Contains("duplicate required", error.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PhysicalIdentityScanRequiresLosslessNameWithoutDependingOnAnIndex(bool compressed)
    {
        (byte[] bytes, JetFormat format, CatalogRow target) = await CreateMovedCatalogAsync("Jackcess/adox_jet4.mdb", chained: false);
        await using DatabaseFile file = Open(bytes);
        TDefImage catalog = Assert.IsType<TDefImage>(await file.TableDefs.ReadImageAsync(2, Ct));
        catalog = WithNameCompression(catalog, compressed);
        ColumnInfo name = catalog.Definition.FindColumn("Name")!;
        int payload = bytes.Length - format.PageSize;
        int start = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(payload + format.DataPage.RowsStart, 2)) & Constants.DataPage.RowOffsetMask;
        Assert.True(RowDecodePlan.TryParseRowLayout(format.RowFields, bytes, payload + start, format.PageSize - start, hasVarColumns: true, out RowLayout layout));
        ColumnSlice slice = RowDecodePlan.ResolveColumnSlice(format.RowFields, bytes, payload + start, format.PageSize - start, layout, name);
        byte[] invalid = compressed ? [0xFF, 0xFE, 0, 0x41] : [0x00, 0xD8];
        invalid.CopyTo(bytes, payload + start + slice.DataStart);
        Assert.True(name.VarIdx + 1 < layout.VarLen);
        BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(payload + start + layout.VarTableStart + ((layout.VarLen - 2 - name.VarIdx) * 2), 2), checked((ushort)(slice.DataStart + invalid.Length)));
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(checked((int)target.PageNumber * format.PageSize) + format.DataPage.TDefOff, 4), 2);
        var pages = new CompactInventoryPageSource(file.Pages, format, 100_000);
        using var definitions = new TableDefReader(pages, format, cacheResults: false);
        using var owned = new OwnedDataPages(pages, format, cacheResults: false);
        var decoder = new CatalogRowReader(format, definitions, owned);
        JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CompactCatalogRecovery.ScanPhysicalAsync(pages, format, catalog, decoder, new(), 100_000, 100_000, Ct));
        Assert.Equal(JetErrorCode.CorruptCatalog, error.ErrorCode);
    }

    [Fact]
    public async Task SecondaryIdentityRecoveryRunsWhenAuditedIdGraphHasAStaleStoredKey()
    {
        (byte[] bytes, JetFormat format, CatalogRow target) = await CreateMovedCatalogAsync("Jackcess/adox_jet4.mdb", chained: true);
        DamageMovedOwners(bytes, format, 2);
        int payload = bytes.Length - format.PageSize;
        int start = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(payload + format.DataPage.RowsStart, 2)) & Constants.DataPage.RowOffsetMask;
        int storedId = checked((int)(target.Id ^ 0x10000));
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(payload + start + format.RowFields.NumCols, 4), storedId);
        CompactCatalogRecovery.Result recovered = await RecoverAsync(bytes, format);
        CompactCatalogRecovery.RecoveredRow evidence = Assert.Single(recovered.Rows, row => row.Identity.PageNumber == target.PageNumber && row.Identity.RowIndex == target.RowIndex);
        Assert.Equal(storedId, evidence.Identity.Id);
        Assert.Equal("ParentIdName", evidence.IdentityKey);
        Assert.Contains(recovered.Diagnostics, diagnostic => diagnostic.Reason.Contains("key disagrees", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData(0xD800)]
    [InlineData(0xDC00)]
    public async Task SecondaryIdentityRecoveryRefusesLossyFixedNameEvenWhenReplacementKeyMatches(int surrogate)
    {
        (byte[] bytes, JetFormat format, CatalogRow target) = await CreateMovedCatalogAsync("Jackcess/adox_jet4.mdb", chained: false);
        await RemoveIdRootAsync(bytes, format);
        await using DatabaseFile file = Open(bytes);
        TDefImage catalog = Assert.IsType<TDefImage>(await file.TableDefs.ReadImageAsync(2, Ct));
        ColumnInfo originalName = catalog.Definition.FindColumn("Name")!;
        ColumnInfo parent = catalog.Definition.FindColumn("ParentId")!;
        int payload = bytes.Length - format.PageSize;
        int start = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(payload + format.DataPage.RowsStart, 2)) & Constants.DataPage.RowOffsetMask;
        Assert.True(RowDecodePlan.TryParseRowLayout(format.RowFields, bytes, payload + start, format.PageSize - start, hasVarColumns: true, out RowLayout layout));
        ColumnSlice slice = RowDecodePlan.ResolveColumnSlice(format.RowFields, bytes, payload + start, format.PageSize - start, layout, originalName);
        Assert.Equal(0, slice.DataLen & 1);
        ColumnInfo name = originalName with { Flags = (byte)(originalName.Flags | Constants.ColumnDescriptorFlags.Fixed), FixedOff = slice.DataStart - format.RowFields.NumCols, Size = slice.DataLen };
        for (int offset = 0; offset < slice.DataLen; offset += 2)
        {
            BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(payload + start + slice.DataStart + offset, 2), checked((ushort)(offset == 0 ? surrogate : 'A')));
        }

        BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(payload + start + layout.VarTableStart + ((layout.VarLen - 1) * 2), 2), checked((ushort)(slice.DataStart + slice.DataLen)));
        string replacementName = "\uFFFD" + new string('A', (slice.DataLen / 2) - 1);
        Assert.Equal(replacementName, ScalarColumnReader.DecodeSimpleColumnValue(format, bytes, payload + start, format.PageSize - start, name));
        TDefIndexDescriptor index = Assert.Single(catalog.RealIndexes, candidate => candidate.Columns.Count == 2 && candidate.Columns[0].ColNum == parent.ColNum && candidate.Columns[1].ColNum == name.ColNum);
        byte[] key = [.. IndexKeyEncoder.EncodeColumnEntry(format, parent, checked((int)target.ParentId), index.Columns[0].Ascending), .. IndexKeyEncoder.EncodeColumnEntry(format, name, replacementName, index.Columns[1].Ascending)];
        IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, [new IndexEntry(key, target.PageNumber, checked((byte)target.RowIndex))], enablePrefixCompression: true).CopyTo(bytes, checked((int)index.RootPage * format.PageSize));
        DamageMovedOwners(bytes, format, 1);
        CompactCatalogRecovery.Result recovered = await RecoverAsync(bytes, format, alterCatalog: image => new(
            image.CopyBytes(),
            image.Header,
            image.Columns.Select(column => column.ColNum == name.ColNum ? name : column).ToList(),
            image.HasDeletedColumns,
            image.Issues,
            image.IndexSection,
            image.RealIndexes.ToList(),
            image.LogicalIndexes.ToList()));
        Assert.DoesNotContain(recovered.Rows, row => row.Identity.PageNumber == target.PageNumber && row.Identity.RowIndex == target.RowIndex);
        Assert.NotEmpty(recovered.Diagnostics);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SecondaryIdentityRecoveryCannotResurrectPrimaryConflictsOmittedBySecondaryIndex(bool payloadAlias)
    {
        (byte[] bytes, JetFormat format, CatalogRow target) = await CreateMovedCatalogAsync("Jackcess/adox_jet4.mdb", chained: false);
        await using DatabaseFile file = Open(bytes);
        TDefImage catalog = Assert.IsType<TDefImage>(await file.TableDefs.ReadImageAsync(2, Ct));
        ColumnInfo id = catalog.Definition.FindColumn("Id")!;
        ColumnInfo parent = catalog.Definition.FindColumn("ParentId")!;
        ColumnInfo name = catalog.Definition.FindColumn("Name")!;
        TDefIndexDescriptor identityIndex = Assert.Single(catalog.RealIndexes, index => index.Columns.Count == 1 && index.Columns[0].ColNum == id.ColNum);
        TDefIndexDescriptor secondary = Assert.Single(catalog.RealIndexes, index => index.Columns.Count == 2 && index.Columns[0].ColNum == parent.ColNum && index.Columns[1].ColNum == name.ColNum);
        int header = checked((int)target.PageNumber * format.PageSize);
        int count = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(header + format.DataPage.NumRows, 2));
        int other = Enumerable.Range(target.RowIndex + 1, count - target.RowIndex - 1).First(row =>
            (BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(header + format.DataPage.RowsStart + (row * 2), 2)) & Constants.DataPage.NonLiveRowFlags) == 0);
        int slot = header + format.DataPage.RowsStart + (other * 2);
        int start = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(slot, 2)) & Constants.DataPage.RowOffsetMask;
        int payload = bytes.Length - format.PageSize;
        int payloadStart = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(payload + format.DataPage.RowsStart, 2)) & Constants.DataPage.RowOffsetMask;
        byte[] stored = bytes.AsSpan(payload + payloadStart).ToArray();
        int secondPayloadPage = payload / format.PageSize;
        if (!payloadAlias)
        {
            secondPayloadPage++;
            Array.Resize(ref bytes, bytes.Length + format.PageSize);
            SetPayload(bytes.AsSpan(bytes.Length - format.PageSize, format.PageSize), format, stored, Constants.DataPage.DeletedRowFlag);
            byte[] idKey = IndexKeyEncoder.EncodeColumnEntry(format, id, checked((int)target.Id), identityIndex.Columns[0].Ascending);
            IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, [new(idKey, target.PageNumber, checked((byte)target.RowIndex)), new(idKey, target.PageNumber, checked((byte)other))], enablePrefixCompression: true).CopyTo(bytes, checked((int)identityIndex.RootPage * format.PageSize));
        }

        BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(slot, 2), checked((ushort)(start | Constants.DataPage.OverflowRowFlag)));
        WritePointer(bytes.AsSpan(header + start, 4), secondPayloadPage, 0);
        byte[] secondaryKey = [.. IndexKeyEncoder.EncodeColumnEntry(format, parent, checked((int)target.ParentId), secondary.Columns[0].Ascending), .. IndexKeyEncoder.EncodeColumnEntry(format, name, target.Name, secondary.Columns[1].Ascending)];
        IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, [new(secondaryKey, target.PageNumber, checked((byte)target.RowIndex))], enablePrefixCompression: true).CopyTo(bytes, checked((int)secondary.RootPage * format.PageSize));
        DamageMovedOwners(bytes, format, payloadAlias ? 1 : 2);
        CompactCatalogRecovery.Result recovered = await RecoverAsync(bytes, format);
        Assert.DoesNotContain(recovered.Rows, row => row.Identity.PageNumber == target.PageNumber && row.Identity.RowIndex == target.RowIndex);
        Assert.NotEmpty(recovered.Diagnostics);
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
        CompactCatalogRecovery.Result complete = await RecoverAsync(bytes, format, allowSecondaryIndex: false, decorateRecovery: pages => new ObservedPages(pages, static _ => { }, () => checks++));
        Assert.Contains(complete.Rows, row => row.Identity == target && row.HasDamagedMovedOwner);
        Assert.True(checks > 0);
        int repeatedChecks = 0;
        CompactCatalogRecovery.Result interrupted = await RecoverAsync(bytes, format, allowSecondaryIndex: false, decorateRecovery: pages => new ObservedPages(
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

    private static TDefImage WithNameCompression(TDefImage image, bool compressed)
        => new(
            image.CopyBytes(),
            image.Header,
            image.Columns.Select(column => column.Name == "Name"
                ? column with { ExtraFlags = compressed ? (byte)(column.ExtraFlags | Constants.CompressedUnicodeExtFlagMask) : (byte)(column.ExtraFlags & ~Constants.CompressedUnicodeExtFlagMask) }
                : column).ToList(),
            image.HasDeletedColumns,
            image.Issues,
            image.IndexSection,
            image.RealIndexes.ToList(),
            image.LogicalIndexes.ToList());

    private static TDefImage WithDuplicateIdentityColumn(TDefImage image, string field)
    {
        ColumnInfo original = Assert.IsType<ColumnInfo>(image.Definition.FindColumn(field));
        return new(
            image.CopyBytes(),
            image.Header,
            image.Columns.Select(column => column.Name == "DateCreate" ? original with { ColNum = column.ColNum, Name = field } : column).ToList(),
            image.HasDeletedColumns,
            image.Issues,
            image.IndexSection,
            image.RealIndexes.ToList(),
            image.LogicalIndexes.ToList());
    }

    private static async Task RemoveIdRootAsync(byte[] bytes, JetFormat format)
    {
        await using DatabaseFile file = Open(bytes);
        TDefImage catalog = Assert.IsType<TDefImage>(await file.TableDefs.ReadImageAsync(2, Ct));
        int idColumn = catalog.Definition.FindColumn("Id")!.ColNum;
        TDefIndexDescriptor id = Assert.Single(catalog.RealIndexes, index => index.Columns.Count == 1 && index.Columns[0].ColNum == idColumn);
        Assert.InRange(id.Slot.FirstDpOffset, 0, format.PageSize - 4);
        bytes.AsSpan((2 * format.PageSize) + id.Slot.FirstDpOffset, 4).Clear();
    }

    private static async Task<CompactCatalogRecovery.Result> RecoverAsync(byte[] bytes, JetFormat format, long maxDirectorySlots = 100_000, bool allowSecondaryIndex = true, long maxReferences = 100_000, Func<IPageSource, IPageSource>? decorateRecovery = null, CancellationToken? recoveryCancellation = null, Func<TDefImage, TDefImage>? alterCatalog = null)
    {
        await using DatabaseFile damaged = Open(bytes);
        var pages = new CompactInventoryPageSource(damaged.Pages, format, 100_000);
        using var definitions = new TableDefReader(pages, format, cacheResults: false);
        using var owned = new OwnedDataPages(pages, format, cacheResults: false);
        var rows = new CatalogRowReader(format, definitions, owned);
        TDefImage catalog = Assert.IsType<TDefImage>(await definitions.ReadImageAsync(2, Ct));
        if (!allowSecondaryIndex)
        {
            catalog = new(catalog.CopyBytes(), catalog.Header, catalog.Columns.ToList(), catalog.HasDeletedColumns, catalog.Issues, catalog.IndexSection, catalog.RealIndexes.Where(index => index.Columns.Count == 1).ToList(), catalog.LogicalIndexes.ToList());
        }

        catalog = alterCatalog?.Invoke(catalog) ?? catalog;
        List<CatalogRow> physical = await rows.GetCatalogRowsAsync(catalog.Definition, Ct, scanPhysicalPages: true);
        return await CompactCatalogRecovery.ScanAsync(decorateRecovery?.Invoke(pages) ?? pages, format, catalog, rows, new(), physical, maxReferences, maxDirectorySlots, 512L * 1024 * 1024, recoveryCancellation ?? Ct);
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
