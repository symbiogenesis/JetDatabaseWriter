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
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Bounded raw index observations never grant reclamation or interpret stored key values.</summary>
public sealed class CompactIndexAuditTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task Tree_StreamsCanonicalKeysAndAllKnownReferencesWithoutMutatingPages(DatabaseFormat kind)
    {
        var format = JetFormat.ForNewDatabase(kind);
        MemoryPages source = CreateTree(format);
        byte[][] original = source.Images.Values.Select(static page => page.ToArray()).ToArray();
        List<CompactIndexRecord> records = await ScanAsync(source, format);
        Assert.Equal(3, source.Reads);
        Assert.Equal<long>([3, 4, 5], records.Where(static record => record.Kind == "index-page").Select(static record => record.PageNumber).Order());
        CompactIndexRecord[] leaves = records.Where(static record => record.Kind == "leaf-entry").ToArray();
        Assert.Equal(3, leaves.Length);
        Assert.Equal(new byte[] { 1, 2, 3 }, leaves[0].CanonicalKey);
        Assert.Equal(9, leaves[0].TargetPageNumber);
        Assert.Equal((byte)0, leaves[0].TargetRowIndex);
        Assert.Equal(2, records.Count(static record => record.Role == "child"));
        Assert.Contains(records, static record => record.Role == "next" && record.TargetPageNumber == 5);
        Assert.Contains(records, static record => record.Role == "tail" && record.TargetPageNumber == 5);
        Assert.All(records, static record => Assert.True(record.IsPinned));
        CompactIndexRecord terminal = Assert.Single(records, static record => record.Kind == "index-terminal");
        Assert.True(terminal.NavigableGraphAcyclic);
        Assert.True(terminal.EntryEdgeFencesValidated);
        Assert.True(terminal.AllObservedEdgeFencesValidated);
        Assert.True(terminal.ObservedLeafChainOrdered);
        Assert.True(terminal.ObservedKeyRoutingValidated);
        Assert.True(terminal.BookmarkInclusiveOrderValidated);
        Assert.False(terminal.TreeCompletenessProven);
        Assert.NotNull(terminal.Coverage);
        for (int i = 0; i < original.Length; i++)
        {
            Assert.Equal(original[i], source.Images.Values.ElementAt(i));
        }
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    public async Task PrefixIncludingBookmarkAndChild_ReconstructsRawCanonicalFields(DatabaseFormat kind)
    {
        var format = JetFormat.ForNewDatabase(kind);
        byte[] root = new byte[format.PageSize];
        root[0] = 3;
        BinaryPrimitives.WriteUInt32LittleEndian(root.AsSpan(4, 4), 2);
        root[format.IndexPage.PrefLenOffset] = 12;
        byte[] first = [0x7F, 0x80, 0, 0, 1, 0, 0, 9, 2, 0, 0, 0, 4];
        first.CopyTo(root, format.IndexPage.FirstEntryOffset);
        root[format.IndexPage.FirstEntryOffset + 13] = 5;
        root[format.IndexPage.BitmaskOffset + 1] = 0x60;
        BinaryPrimitives.WriteUInt16LittleEndian(root.AsSpan(2, 2), checked((ushort)(format.PageSize - format.IndexPage.FirstEntryOffset - 14)));
        var source = new MemoryPages(format.PageSize, new() { [3] = root, [4] = Leaf(format, []), [5] = Leaf(format, []) });
        List<CompactIndexRecord> records = await ScanAsync(source, format);
        CompactIndexRecord[] entries = records.Where(static record => record.Kind == "intermediate-entry").ToArray();
        Assert.Equal(2, entries.Length);
        Assert.All(entries, static record => Assert.Equal(new byte[] { 0x7F, 0x80, 0, 0, 1 }, record.CanonicalKey));
        Assert.All(entries, static record => Assert.Equal(9, record.TargetPageNumber));
        Assert.All(entries, static record => Assert.Equal((byte)2, record.TargetRowIndex));
        Assert.Equal<long?>([4, 5], entries.Select(static record => record.ChildPageNumber));
    }

    [Theory]
    [InlineData("FixedText/NativeFixedTextJet3Cp1252.mdb")]
    [InlineData("FixedText/NativeFixedTextJet4.mdb")]
    [InlineData("NorthwindTraders.accdb")]
    public async Task NativeCatalogTrees_StreamKeysMatchingTheEstablishedNativeCodec(string fixture)
    {
        byte[] image = await File.ReadAllBytesAsync(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Databases", fixture), Ct);
        await using var stream = new MemoryStream(image, writable: false);
        await using var file = DatabaseFile.ForReader(stream, image, ReadOnlyMemory<char>.Empty, string.Empty, leaveOpen: true, cancellationToken: Ct);
        List<long> roots = await IndexLeafChain.ReadRealIndexRootsAsync(file, 2, Ct);
        int pages = 0;
        int entries = 0;
        foreach (long root in roots.Where(static root => root > 0))
        {
            await CompactIndexAudit.ScanAsync(
                file.Pages,
                file.Format,
                2,
                root,
                new(),
                async (record, token) =>
                {
                    if (record.Kind == "index-page")
                    {
                        pages++;
                    }

                    if (record.Kind is "leaf-entry" or "intermediate-entry")
                    {
                        byte[] raw = await file.Pages.ReadPageCopyAsync(record.PageNumber, token);
                        IndexEntry expected = record.Kind == "leaf-entry"
                            ? IndexPageCodec.DecodeLeafEntries(file.Format.IndexPage, raw, file.Format.PageSize)[record.EntryIndex!.Value]
                            : IndexPageCodec.DecodeIntermediateEntries(file.Format.IndexPage, raw, file.Format.PageSize)[record.EntryIndex!.Value].Entry;
                        Assert.Equal(expected.Key, record.CanonicalKey);
                        Assert.Equal(expected.DataPage, record.TargetPageNumber);
                        Assert.Equal(expected.DataRow, record.TargetRowIndex);
                        entries++;
                    }
                },
                Ct);
        }

        Assert.True(pages > 0);
        Assert.True(entries > 0);
    }

    [Fact]
    public async Task NativeJet3CompressedBookmarkLeaf_PreservesEveryDuplicateKeyAndBookmark()
    {
        byte[] image = await File.ReadAllBytesAsync(TestDatabases.MdbtoolsNwind, Ct);
        var format = JetFormat.FromHeader(image);
        byte[] leaf = image.AsSpan(593 * format.PageSize, format.PageSize).ToArray();
        List<IndexEntry> expected = IndexPageCodec.DecodeLeafEntries(format.IndexPage, leaf, format.PageSize);
        long owner = BinaryPrimitives.ReadUInt32LittleEndian(leaf.AsSpan(4, 4));
        var source = new MemoryPages(format.PageSize, new() { [593] = leaf }) { PageCount = image.Length / format.PageSize };
        int entries = 0;
        await CompactIndexAudit.ScanAsync(
            source,
            format,
            owner,
            593,
            new(),
            (record, _) =>
            {
                if (record.Kind == "leaf-entry")
                {
                    Assert.Equal(new byte[] { 0x7F, 0x80, 0, 0, 1 }, record.CanonicalKey);
                    Assert.Equal(expected[entries].DataPage, record.TargetPageNumber);
                    Assert.Equal(expected[entries].DataRow, record.TargetRowIndex);
                    entries++;
                }

                return ValueTask.CompletedTask;
            },
            Ct);
        Assert.Equal(249, entries);
    }

    [Theory]
    [InlineData("owner")]
    [InlineData("type")]
    [InlineData("free-space")]
    [InlineData("sentinel")]
    [InlineData("extra-bit")]
    [InlineData("prefix")]
    [InlineData("row-pointer")]
    [InlineData("child-pointer")]
    [InlineData("header-pointer")]
    [InlineData("order")]
    [InlineData("empty-intermediate")]
    public async Task MalformedTree_RefusesWithStructuredPageAndPreservesSource(string damage)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        MemoryPages source = CreateTree(format);
        byte[] root = source.Images[3];
        switch (damage)
        {
            case "owner":
                root[4] = 8;
                break;
            case "type":
                root[0] = 1;
                break;
            case "free-space":
                BinaryPrimitives.WriteUInt16LittleEndian(root.AsSpan(2, 2), ushort.MaxValue);
                break;
            case "sentinel":
                TogglePayloadBit(format, root, format.PageSize - JetTypeInfo.Ru16(root, 2) - format.IndexPage.FirstEntryOffset);
                break;
            case "extra-bit":
                TogglePayloadBit(format, root, format.PageSize - JetTypeInfo.Ru16(root, 2) - format.IndexPage.FirstEntryOffset + 1);
                break;
            case "prefix":
                BinaryPrimitives.WriteUInt16LittleEndian(root.AsSpan(format.IndexPage.PrefLenOffset, 2), ushort.MaxValue);
                break;
            case "row-pointer":
                root[format.IndexPage.FirstEntryOffset + 3] = 1;
                break;
            case "child-pointer":
                root[format.IndexPage.FirstEntryOffset + 7] = 1;
                break;
            case "header-pointer":
                BinaryPrimitives.WriteUInt32LittleEndian(root.AsSpan(format.IndexPage.TailPageOffset, 4), 1000);
                break;
            case "order":
                source.Images[4] = Leaf(format, [new([2], 9, 0), new([1], 9, 1)]);
                break;
            default:
                Array.Clear(root, format.IndexPage.BitmaskOffset, root.Length - format.IndexPage.BitmaskOffset);
                BinaryPrimitives.WriteUInt16LittleEndian(root.AsSpan(2, 2), checked((ushort)(format.PageSize - format.IndexPage.FirstEntryOffset)));
                BinaryPrimitives.WriteUInt32LittleEndian(root.AsSpan(format.IndexPage.TailPageOffset, 4), 0);
                break;
        }

        byte[] original = root.ToArray();
        JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await ScanAsync(source, format));
        Assert.Equal(JetErrorCode.CorruptIndex, error.ErrorCode);
        Assert.Equal(damage == "order" ? 4 : 3, error.ErrorInfo.PageNumber);
        Assert.NotNull(error.ErrorInfo.Reason);
        Assert.Equal(original, root);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RepeatedChild_RefusesCyclesAndSharedReachability(bool cycle)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        MemoryPages source = CreateTree(format);
        source.Images[3] = IndexPageCodec.BuildIntermediatePage(format.IndexPage, format.PageSize, 2, [new(new([1], 9, 0), 4), new(new([2], 9, 1), cycle ? 3 : 4)], 0, 0, 0);
        JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await ScanAsync(source, format));
        Assert.Equal(3, error.ErrorInfo.PageNumber);
        Assert.Equal(1, source.Reads);
    }

    [Theory]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(6)]
    public async Task TailCycles_RefuseSelfRootAndOtherAncestors(int target)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        MemoryPages source = CreateTree(format);
        source.Images[4] = IndexPageCodec.BuildIntermediatePage(format.IndexPage, format.PageSize, 2, [new(new([1], 9, 0), 6)], 0, 0, 0);
        source.Images[6] = IndexPageCodec.BuildIntermediatePage(format.IndexPage, format.PageSize, 2, [new(new([1], 9, 0), 7)], 0, 0, target);
        source.Images[7] = Leaf(format, []);
        JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await ScanAsync(source, format));
        Assert.Equal(6, error.ErrorInfo.PageNumber);
    }

    [Fact]
    public async Task MixedTailAndEntryAliasCycle_RefusesBeforeTerminalAndPreservesSource()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        MemoryPages source = CreateAliasGraph(format, cycle: true);
        byte[][] original = source.Images.Values.Select(static page => page.ToArray()).ToArray();
        var records = new List<CompactIndexRecord>();
        JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CompactIndexAudit.ScanAsync(
            source,
            format,
            2,
            3,
            new(),
            (record, _) =>
            {
                records.Add(record);
                return ValueTask.CompletedTask;
            },
            Ct));
        Assert.Equal(JetErrorCode.CorruptIndex, error.ErrorCode);
        Assert.Equal(6, error.ErrorInfo.PageNumber);
        Assert.Contains("cycle", error.ErrorInfo.Reason, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(records, static record => record.Kind == "index-terminal");
        Assert.Equal(4, source.Reads);
        for (int i = 0; i < original.Length; i++)
        {
            Assert.Equal(original[i], source.Images.Values.ElementAt(i));
        }
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task TailAndEntryAliasDag_ReadsEveryPageOnceAndRetainsEveryReference(DatabaseFormat kind)
    {
        var format = JetFormat.ForNewDatabase(kind);
        MemoryPages source = CreateAliasGraph(format, cycle: false);
        List<CompactIndexRecord> records = await ScanAsync(source, format);
        Assert.Equal(4, source.Reads);
        Assert.Equal<long>([3, 4, 5, 6], records.Where(static record => record.Kind == "index-page").Select(static record => record.PageNumber).Order());
        Assert.Contains(records, static record => record.Role == "tail" && record.PageNumber == 4 && record.TargetPageNumber == 6);
        Assert.Contains(records, static record => record.Role == "child" && record.PageNumber == 5 && record.TargetPageNumber == 6);
        CompactIndexRecord terminal = Assert.Single(records, static record => record.Kind == "index-terminal");
        Assert.True(terminal.NavigableGraphAcyclic);
        Assert.False(terminal.TreeCompletenessProven);
        Assert.All(records, static record => Assert.True(record.IsPinned));
    }

    [Fact]
    public async Task GraphBudget_CountsEveryDistinctNodeAndNavigableEdgeIncludingAliases()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        MemoryPages source = CreateAliasGraph(format, cycle: false);
        JetLimitationException error = await Assert.ThrowsAsync<JetLimitationException>(async () => await ScanAsync(source, format, new() { MaxGraphSteps = 28 }));
        Assert.Equal(4, source.Reads);
        Assert.Contains("MaxGraphSteps", error.ErrorInfo.Reason, StringComparison.Ordinal);
        source = CreateAliasGraph(format, cycle: false);
        List<CompactIndexRecord> records = await ScanAsync(source, format, new() { MaxGraphSteps = 29 });
        Assert.True(Assert.Single(records, static record => record.Kind == "index-terminal").NavigableGraphAcyclic);
    }

    [Fact]
    public async Task WorkingBudget_ReservesGraphEdgesAndTraversalStateBeforeReading()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        MemoryPages source = CreateAliasGraph(format, cycle: false);
        const long maxEntries = 100;
        const long maxReferences = 120;
        long pages = source.PageCount - 1;
        long edges = Math.Min(maxReferences, maxEntries + pages);
        long retained = Math.Min(new CompactIndexBudget().MaxRetainedKeyBytes, 6 * pages * format.PageSize);
        long storage = (576 * pages) + (16 * edges) + retained + (16 * format.PageSize) + 65_536;
        await Assert.ThrowsAsync<JetLimitationException>(async () => await ScanAsync(source, format, new() { MaxEntries = maxEntries, MaxReferences = maxReferences, MaxWorkingBytes = storage - 1 }));
        Assert.Equal(0, source.Reads);
        List<CompactIndexRecord> records = await ScanAsync(source, format, new() { MaxEntries = maxEntries, MaxReferences = maxReferences, MaxWorkingBytes = storage });
        Assert.Single(records, static record => record.Kind == "index-terminal");
    }

    [Fact]
    public async Task CancellationBeforeGraphProof_NeverEmitsTerminal()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        MemoryPages source = CreateAliasGraph(format, cycle: false);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        var records = new List<CompactIndexRecord>();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await CompactIndexAudit.ScanAsync(
            source,
            format,
            2,
            3,
            new(),
            async (record, _) =>
            {
                records.Add(record);
                if (record.Kind == "index-page" && record.PageNumber == 6)
                {
                    await cancellation.CancelAsync();
                }
            },
            cancellation.Token));
        Assert.Equal(4, source.Reads);
        Assert.All(records, static record => Assert.False(record.NavigableGraphAcyclic));
        Assert.DoesNotContain(records, static record => record.Kind == "index-terminal");
    }

    [Fact]
    public async Task TailOnlyLeaf_IsReadAndValidatedWithTheSamePageBudget()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        MemoryPages source = CreateTree(format);
        BinaryPrimitives.WriteUInt32LittleEndian(source.Images[3].AsSpan(format.IndexPage.TailPageOffset, 4), 6);
        source.Images[6] = Leaf(format, [new([3], 9, 3)]);
        List<CompactIndexRecord> records = await ScanAsync(source, format);
        Assert.Equal(4, source.Reads);
        Assert.Contains(records, static record => record.Kind == "leaf-entry" && record.PageNumber == 6);
        Assert.False(Assert.Single(records, static record => record.Kind == "index-terminal").ObservedLeafChainOrdered);
        source.Images[6][4] = 8;
        JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await ScanAsync(source, format));
        Assert.Equal(6, error.ErrorInfo.PageNumber);
        await Assert.ThrowsAsync<JetLimitationException>(async () => await ScanAsync(source, format, new() { MaxTreePages = 3 }));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task EmptyIntermediateWithTail_TraversesTheKnownNavigableShape(DatabaseFormat kind)
    {
        var format = JetFormat.ForNewDatabase(kind);
        MemoryPages source = CreateTree(format);
        byte[] root = Leaf(format, []);
        root[0] = Constants.PageTypes.IndexIntermediate;
        BinaryPrimitives.WriteUInt32LittleEndian(root.AsSpan(format.IndexPage.TailPageOffset, 4), 4);
        source.Images[3] = root;
        List<CompactIndexRecord> records = await ScanAsync(source, format);
        Assert.Equal(2, source.Reads);
        Assert.Contains(records, static record => record.Kind == "leaf-entry" && record.PageNumber == 4);
        Assert.DoesNotContain(records, static record => record.Kind == "intermediate-entry");
    }

    [Fact]
    public async Task EqualKeyBookmarkOrder_IsPreservedWithoutInventingAnOrderingRule()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        var source = new MemoryPages(format.PageSize, new() { [3] = Leaf(format, [new([1], 9, 5), new([1], 8, 1)]) });
        List<CompactIndexRecord> records = await ScanAsync(source, format);
        CompactIndexRecord[] entries = records.Where(static record => record.Kind == "leaf-entry").ToArray();
        Assert.Equal<long?>([9, 8], entries.Select(static record => record.TargetPageNumber));
        Assert.Equal<byte?>([5, 1], entries.Select(static record => record.TargetRowIndex));
    }

    [Theory]
    [InlineData("upper")]
    [InlineData("lower")]
    public async Task ChildKeysOutsideSeparatorFences_RefuseBeforeTerminal(string fence)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        MemoryPages source = CreateTree(format);
        long damagedPage = fence == "upper" ? 4 : 5;
        source.Images[damagedPage] = Leaf(format, [new(fence == "upper" ? [1, 2, 5] : [1, 2, 3], 9, 1)]);
        byte[][] original = source.Images.Values.Select(static page => page.ToArray()).ToArray();
        var records = new List<CompactIndexRecord>();
        JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CompactIndexAudit.ScanAsync(
            source,
            format,
            2,
            3,
            new(),
            (record, _) =>
            {
                records.Add(record);
                return ValueTask.CompletedTask;
            },
            Ct));
        Assert.Equal(JetErrorCode.CorruptIndex, error.ErrorCode);
        Assert.Equal(damagedPage, error.ErrorInfo.PageNumber);
        Assert.Contains("separator", error.ErrorInfo.Reason, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(records, static record => record.Kind == "index-terminal");
        for (int i = 0; i < original.Length; i++)
        {
            Assert.Equal(original[i], source.Images.Values.ElementAt(i));
        }
    }

    [Fact]
    public async Task TailOnlyLeafBeforeItsPreviousLeaf_RefusesGlobalOrderDamage()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        var source = new MemoryPages(format.PageSize, new()
        {
            [3] = IndexPageCodec.BuildIntermediatePage(format.IndexPage, format.PageSize, 2, [new(new([2], 9, 0), 4)], 0, 0, 5),
            [4] = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, [new([2], 9, 0)], 0, 5, 0, false),
            [5] = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, [new([1], 9, 1)], 4, 0, 0, false),
        });
        JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await ScanAsync(source, format));
        Assert.Equal(4, error.ErrorInfo.PageNumber);
        Assert.Contains("leaf", error.ErrorInfo.Reason, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task EqualChildKeysAndNonExactUpperFence_PreserveNativeKeyComparison(DatabaseFormat kind)
    {
        var format = JetFormat.ForNewDatabase(kind);
        var source = new MemoryPages(format.PageSize, new()
        {
            [3] = IndexPageCodec.BuildIntermediatePage(format.IndexPage, format.PageSize, 2, [new(new([1], 9, 0), 4), new(new([2], 9, 1), 5)], 0, 0, 5),
            [4] = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, [new([1], 9, 5)], 0, 5, 0, false),
            [5] = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, [new([1], 8, 1)], 4, 0, 0, false),
        });
        List<CompactIndexRecord> records = await ScanAsync(source, format);
        Assert.Equal(2, records.Count(static record => record.Kind == "leaf-entry"));
        Assert.Single(records, static record => record.Kind == "index-terminal");
    }

    [Fact]
    public async Task TailOnlyDescendant_IsExplicitlyOutsideEntryEdgeFenceProof()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        var source = new MemoryPages(format.PageSize, new()
        {
            [3] = IndexPageCodec.BuildIntermediatePage(format.IndexPage, format.PageSize, 2, [new(new([1], 9, 0), 4)], 0, 0, 0),
            [4] = TailIntermediate(format, 5),
            [5] = Leaf(format, [new([99], 9, 0)]),
        });
        List<CompactIndexRecord> records = await ScanAsync(source, format);
        Assert.Contains(records, static record => record.Kind == "leaf-entry" && record.CanonicalKey is [99]);
        CompactIndexRecord terminal = Assert.Single(records, static record => record.Kind == "index-terminal");
        Assert.True(terminal.EntryEdgeFencesValidated);
        Assert.False(terminal.TreeCompletenessProven);
        Assert.Contains("all tail edges are excluded", terminal.Coverage, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, true)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    public async Task CompleteObservedFences_IncludeDistinctTailDescendantsWithoutRefusingUncertainLayouts(DatabaseFormat kind, bool exceedsFence)
    {
        var format = JetFormat.ForNewDatabase(kind);
        var source = new MemoryPages(format.PageSize, new()
        {
            [3] = IndexPageCodec.BuildIntermediatePage(format.IndexPage, format.PageSize, 2, [new(new([2], 9, 0), 4)], 0, 0, 5),
            [4] = Leaf(format, [new([2], 9, 0)]),
            [5] = TailIntermediate(format, 6),
            [6] = Leaf(format, [new(exceedsFence ? [1] : [3], 9, 1)]),
        });
        byte[][] original = source.Images.Values.Select(static page => page.ToArray()).ToArray();
        List<CompactIndexRecord> records = await ScanAsync(source, format);
        CompactIndexRecord terminal = Assert.Single(records, static record => record.Kind == "index-terminal");
        Assert.True(terminal.EntryEdgeFencesValidated);
        Assert.Equal(!exceedsFence, terminal.AllObservedEdgeFencesValidated);
        Assert.False(terminal.TreeCompletenessProven);
        Assert.Equal(4, source.Reads);
        Assert.All(records.Where(static record => record.Kind != "index-terminal"), static record => Assert.False(record.AllObservedEdgeFencesValidated));
        Assert.Equal(2, records.Count(static record => record.Kind == "leaf-entry"));
        for (int i = 0; i < original.Length; i++)
        {
            Assert.Equal(original[i], source.Images.Values.ElementAt(i));
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CompleteObservedFences_IncludeNestedTailsWithinAncestorEntryFence(bool exceedsFence)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        var source = new MemoryPages(format.PageSize, new()
        {
            [3] = IndexPageCodec.BuildIntermediatePage(format.IndexPage, format.PageSize, 2, [new(new([2], 9, 0), 4)], 0, 0, 0),
            [4] = TailIntermediate(format, 5),
            [5] = Leaf(format, [new(exceedsFence ? [3] : [1], 9, 0)]),
        });
        List<CompactIndexRecord> records = await ScanAsync(source, format);
        CompactIndexRecord terminal = Assert.Single(records, static record => record.Kind == "index-terminal");
        Assert.True(terminal.EntryEdgeFencesValidated);
        Assert.Equal(!exceedsFence, terminal.AllObservedEdgeFencesValidated);
        Assert.False(terminal.TreeCompletenessProven);
    }

    [Fact]
    public async Task CompleteObservedFences_UseKeysOnlyWhenNativeDuplicateBookmarksHaveUncertainOrder()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        var source = new MemoryPages(format.PageSize, new()
        {
            [3] = IndexPageCodec.BuildIntermediatePage(format.IndexPage, format.PageSize, 2, [new(new([1], 9, 2), 4)], 0, 0, 5),
            [4] = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, [new([1], 9, 2)], 0, 5, 0, true),
            [5] = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, [new([1], 9, 1)], 4, 0, 0, true),
        });
        List<CompactIndexRecord> records = await ScanAsync(source, format);
        CompactIndexRecord terminal = Assert.Single(records, static record => record.Kind == "index-terminal");
        Assert.True(terminal.AllObservedEdgeFencesValidated);
        Assert.False(terminal.BookmarkInclusiveOrderValidated);
        Assert.False(terminal.TreeCompletenessProven);
    }

    [Fact]
    public async Task NonRightmostIntermediateTailAlias_DoesNotExpandItsEntryChildFence()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        var source = new MemoryPages(format.PageSize, new()
        {
            [3] = IndexPageCodec.BuildIntermediatePage(format.IndexPage, format.PageSize, 2, [new(new([1], 9, 0), 4), new(new([3], 9, 2), 5)], 0, 0, 8),
            [4] = IndexPageCodec.BuildIntermediatePage(format.IndexPage, format.PageSize, 2, [new(new([1], 9, 0), 6)], 0, 5, 8),
            [5] = IndexPageCodec.BuildIntermediatePage(format.IndexPage, format.PageSize, 2, [new(new([2], 9, 1), 7), new(new([3], 9, 2), 8)], 4, 0, 8),
            [6] = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, [new([1], 9, 0)], 0, 7, 0, false),
            [7] = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, [new([2], 9, 1)], 6, 8, 0, false),
            [8] = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, [new([3], 9, 2)], 7, 0, 0, false),
        });
        List<CompactIndexRecord> records = await ScanAsync(source, format);
        Assert.Equal(6, source.Reads);
        CompactIndexRecord terminal = Assert.Single(records, static record => record.Kind == "index-terminal");
        Assert.True(terminal.EntryEdgeFencesValidated);
        Assert.False(terminal.AllObservedEdgeFencesValidated);
        Assert.True(terminal.ObservedLeafChainOrdered);
        Assert.True(terminal.ObservedKeyRoutingValidated);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task TailRouting_OnlyCertifiesDescentThatCannotSkipAnObservedEarlierKey(bool skipsEarlierKey)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        var source = new MemoryPages(format.PageSize, new()
        {
            [3] = IndexPageCodec.BuildIntermediatePage(format.IndexPage, format.PageSize, 2, [new(new([1], 9, 0), 4), new(new([3], 9, 2), 8)], 0, 0, 6),
            [4] = IndexPageCodec.BuildIntermediatePage(format.IndexPage, format.PageSize, 2, [new(new([1], 9, 0), 7)], 0, 8, 5),
            [8] = skipsEarlierKey
                ? IndexPageCodec.BuildIntermediatePage(format.IndexPage, format.PageSize, 2, [new(new([3], 9, 2), 6)], 4, 0, 5)
                : TailIntermediate(format, 5),
            [7] = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, [new([1], 9, 0)], 0, 5, 0, true),
            [5] = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, [new([2], 9, 1)], 7, 6, 0, true),
            [6] = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, [new([3], 9, 2)], 5, 0, 0, true),
        });
        var cursor = new IndexCursor(format.IndexPage, (page, _) => ValueTask.FromResult(source.Images[page]), format.PageSize);
        Assert.Equal(!skipsEarlierKey, await cursor.ContainsKeyAsync(3, [2], Ct));
        List<CompactIndexRecord> records = await ScanAsync(source, format);
        CompactIndexRecord terminal = Assert.Single(records, static record => record.Kind == "index-terminal");
        Assert.True(terminal.NavigableGraphAcyclic);
        Assert.True(terminal.EntryEdgeFencesValidated);
        Assert.True(terminal.ObservedLeafChainOrdered);
        Assert.Equal(!skipsEarlierKey, terminal.ObservedKeyRoutingValidated);
        Assert.False(terminal.TreeCompletenessProven);
        Assert.Equal(6, source.Reads);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task TailOnlyDuplicateKeyLeaf_ProvesKeyRoutingWithoutTreatingBookmarksAsSeekBounds(DatabaseFormat kind)
    {
        var format = JetFormat.ForNewDatabase(kind);
        var source = new MemoryPages(format.PageSize, new()
        {
            [3] = IndexPageCodec.BuildIntermediatePage(format.IndexPage, format.PageSize, 2, [new(new([1], 9, 0), 4)], 0, 0, 5),
            [4] = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, [new([1], 9, 0)], 0, 5, 0, true),
            [5] = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, [new([1], 9, 1)], 4, 0, 0, true),
        });
        List<CompactIndexRecord> records = await ScanAsync(source, format);
        Assert.True(Assert.Single(records, static record => record.Kind == "index-terminal").ObservedKeyRoutingValidated);
    }

    [Fact]
    public async Task RoutingProof_CoversLeafKeysWithoutRequiringFenceOnlySeparatorsToExistInLeaves()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        var source = new MemoryPages(format.PageSize, new()
        {
            [3] = IndexPageCodec.BuildIntermediatePage(format.IndexPage, format.PageSize, 2, [new(new([2], 9, 2), 4)], 0, 0, 5),
            [4] = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, [new([1], 9, 0)], 0, 5, 0, true),
            [5] = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, [new([3], 9, 1)], 4, 0, 0, true),
        });
        var cursor = new IndexCursor(format.IndexPage, (page, _) => ValueTask.FromResult(source.Images[page]), format.PageSize);
        Assert.True(await cursor.ContainsKeyAsync(3, [1], Ct));
        Assert.True(await cursor.ContainsKeyAsync(3, [3], Ct));
        Assert.False(await cursor.ContainsKeyAsync(3, [2], Ct));
        List<CompactIndexRecord> records = await ScanAsync(source, format);
        CompactIndexRecord terminal = Assert.Single(records, static record => record.Kind == "index-terminal");
        Assert.True(terminal.ObservedKeyRoutingValidated);
        Assert.True(terminal.AllObservedEdgeFencesValidated);
        Assert.False(terminal.TreeCompletenessProven);
        Assert.Contains("present in the observed leaves", terminal.Coverage, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task LeafChainThroughEmptyPage_ValidatesNonemptyBoundaryOrder(bool decreasing)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        var source = new MemoryPages(format.PageSize, new()
        {
            [3] = IndexPageCodec.BuildIntermediatePage(format.IndexPage, format.PageSize, 2, [new(new([2], 9, 0), 4), new(new([3], 9, 1), 5)], 0, 0, 6),
            [4] = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, [new([2], 9, 0)], 0, 5, 0, false),
            [5] = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, [], 4, 6, 0, false),
            [6] = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, [new(decreasing ? [1] : [4], 9, 2)], 5, 0, 0, false),
        });
        if (decreasing)
        {
            JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await ScanAsync(source, format));
            Assert.Equal(6, error.ErrorInfo.PageNumber);
        }
        else
        {
            List<CompactIndexRecord> records = await ScanAsync(source, format);
            Assert.True(Assert.Single(records, static record => record.Kind == "index-terminal").ObservedLeafChainOrdered);
            Assert.False(Assert.Single(records, static record => record.Kind == "index-terminal").ObservedKeyRoutingValidated);
        }
    }

    [Fact]
    public async Task MissingObservedLeafLink_NeverClaimsACompleteSiblingChain()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        MemoryPages source = CreateTree(format);
        BinaryPrimitives.WriteUInt32LittleEndian(source.Images[4].AsSpan(format.IndexPage.NextPageOffset, 4), 6);
        List<CompactIndexRecord> records = await ScanAsync(source, format);
        CompactIndexRecord terminal = Assert.Single(records, static record => record.Kind == "index-terminal");
        Assert.True(terminal.EntryEdgeFencesValidated);
        Assert.False(terminal.ObservedLeafChainOrdered);
        Assert.False(terminal.ObservedKeyRoutingValidated);
        Assert.False(terminal.TreeCompletenessProven);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task EqualKeysWithDecreasingBookmarks_PreserveObservationsAndWithholdBookmarkProof(bool acrossPages)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        Dictionary<long, byte[]> images = acrossPages
            ? new()
            {
                [3] = IndexPageCodec.BuildIntermediatePage(format.IndexPage, format.PageSize, 2, [new(new([1], 9, 2), 4)], 0, 0, 5),
                [4] = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, [new([1], 9, 2)], 0, 5, 0, true),
                [5] = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, [new([1], 9, 1)], 4, 0, 0, true),
            }
            : new() { [3] = Leaf(format, [new([1], 9, 2), new([1], 9, 1)]) };
        var source = new MemoryPages(format.PageSize, images);
        List<CompactIndexRecord> records = await ScanAsync(source, format);
        CompactIndexRecord terminal = Assert.Single(records, static record => record.Kind == "index-terminal");
        Assert.True(terminal.ObservedLeafChainOrdered);
        Assert.True(terminal.ObservedKeyRoutingValidated);
        Assert.False(terminal.BookmarkInclusiveOrderValidated);
        Assert.Equal<byte?>([2, 1], records.Where(static record => record.Kind == "leaf-entry").Select(static record => record.TargetRowIndex));
    }

    [Fact]
    public async Task EqualKeyBookmarkBeyondEntrySeparator_WithholdsBookmarkFenceProof()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        var source = new MemoryPages(format.PageSize, new()
        {
            [3] = IndexPageCodec.BuildIntermediatePage(format.IndexPage, format.PageSize, 2, [new(new([1], 9, 1), 4)], 0, 0, 0),
            [4] = Leaf(format, [new([1], 9, 2)]),
        });
        List<CompactIndexRecord> records = await ScanAsync(source, format);
        CompactIndexRecord terminal = Assert.Single(records, static record => record.Kind == "index-terminal");
        Assert.True(terminal.EntryEdgeFencesValidated);
        Assert.True(terminal.ObservedKeyRoutingValidated);
        Assert.False(terminal.BookmarkInclusiveOrderValidated);
    }

    [Fact]
    public async Task KeyBeforeBookmarkComparison_PreservesVariableLengthKeyOrder()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        var source = new MemoryPages(format.PageSize, new() { [3] = Leaf(format, [new([1], 10, 0), new([1, 0], 9, 0)]) });
        List<CompactIndexRecord> records = await ScanAsync(source, format);
        Assert.Equal(2, records.Count(static record => record.Kind == "leaf-entry"));
    }

    [Fact]
    public async Task RetainedKeyBudget_ReleasesSupersededLeafBoundaryBuffers()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        IndexEntry[] entries = Enumerable.Range(0, 100).Select(static row => new IndexEntry([1], 9, checked((byte)row))).ToArray();
        var source = new MemoryPages(format.PageSize, new() { [3] = Leaf(format, entries) });
        await Assert.ThrowsAsync<JetLimitationException>(async () => await ScanAsync(source, format, new() { MaxRetainedKeyBytes = 9 }));
        List<CompactIndexRecord> records = await ScanAsync(source, format, new() { MaxRetainedKeyBytes = 10 });
        Assert.Equal(entries.Length, records.Count(static record => record.Kind == "leaf-entry"));
        Assert.True(Assert.Single(records, static record => record.Kind == "index-terminal").ObservedLeafChainOrdered);
    }

    [Theory]
    [InlineData("working")]
    [InlineData("pages")]
    [InlineData("entries")]
    [InlineData("references")]
    [InlineData("records")]
    [InlineData("canonical-bytes")]
    [InlineData("retained-key-bytes")]
    [InlineData("graph")]
    public async Task Budgets_RefuseBeforeExceedingTheirAllowance(string limit)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        MemoryPages source = CreateTree(format);
        CompactIndexBudget budget = limit switch
        {
            "working" => new() { MaxWorkingBytes = 1 },
            "pages" => new() { MaxTreePages = 1 },
            "entries" => new() { MaxEntries = 1 },
            "references" => new() { MaxReferences = 1 },
            "records" => new() { MaxRecords = 1 },
            "graph" => new() { MaxGraphSteps = 1 },
            "retained-key-bytes" => new() { MaxRetainedKeyBytes = 1 },
            _ => new() { MaxCanonicalBytes = 1 },
        };
        if (limit == "graph")
        {
            source.Images[4] = IndexPageCodec.BuildIntermediatePage(format.IndexPage, format.PageSize, 2, [new(new([1], 9, 0), 6)], 0, 0, 5);
            source.Images[6] = Leaf(format, []);
        }

        JetLimitationException error = await Assert.ThrowsAsync<JetLimitationException>(async () => await ScanAsync(source, format, budget));
        Assert.Equal(JetErrorCode.ValueTooLarge, error.ErrorCode);
        if (limit == "working")
        {
            Assert.Equal(0, source.Reads);
        }
    }

    [Fact]
    public async Task CancellationAndRealIoFailures_PropagateWithoutTranslation()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        MemoryPages source = CreateTree(format);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await CompactIndexAudit.ScanAsync(
            source,
            format,
            2,
            3,
            new(),
            async (_, _) => await cancellation.CancelAsync(),
            cancellation.Token));
        Assert.Equal(1, source.Reads);
        var failure = new InvalidDataException("source read failed");
        source = CreateTree(format);
        source.Failure = failure;
        JetCorruptDataException actual = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await ScanAsync(source, format));
        Assert.Same(failure, actual);
    }

    [Fact]
    public async Task TruncatedPageAndChangingGeometry_RefuseWithoutSlicingPastTheRead()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        MemoryPages source = CreateTree(format);
        source.Truncate = true;
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await ScanAsync(source, format));
        source = CreateTree(format);
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CompactIndexAudit.ScanAsync(
            source,
            format,
            2,
            3,
            new(),
            (_, _) =>
            {
                source.PageCount++;
                return ValueTask.CompletedTask;
            },
            Ct));
    }

    private static MemoryPages CreateAliasGraph(JetFormat format, bool cycle)
        => new(format.PageSize, new()
        {
            [3] = IndexPageCodec.BuildIntermediatePage(format.IndexPage, format.PageSize, 2, [new(new([1], 9, 0), 4), new(new([2], 9, 1), 5)], 0, 0, 0),
            [4] = TailIntermediate(format, cycle ? 5 : 6),
            [5] = IndexPageCodec.BuildIntermediatePage(format.IndexPage, format.PageSize, 2, [new(new([1], 9, 0), 6)], 0, 0, 0),
            [6] = cycle ? TailIntermediate(format, 4) : Leaf(format, []),
        });

    private static MemoryPages CreateTree(JetFormat format)
        => new(format.PageSize, new()
        {
            [3] = IndexPageCodec.BuildIntermediatePage(format.IndexPage, format.PageSize, 2, [new(new([1, 2, 4], 9, 1), 4), new(new([2], 9, 2), 5)], 0, 0, 5),
            [4] = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, [new([1, 2, 3], 9, 0), new([1, 2, 4], 9, 1)], 0, 5, 0, true),
            [5] = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, [new([2], 9, 2)], 4, 0, 0, true),
        });

    private static byte[] TailIntermediate(JetFormat format, long tail)
    {
        byte[] page = Leaf(format, []);
        page[0] = Constants.PageTypes.IndexIntermediate;
        BinaryPrimitives.WriteUInt32LittleEndian(page.AsSpan(format.IndexPage.TailPageOffset, 4), checked((uint)tail));
        return page;
    }

    private static byte[] Leaf(JetFormat format, IReadOnlyList<IndexEntry> entries)
        => IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, entries, enablePrefixCompression: false);

    private static void TogglePayloadBit(JetFormat format, byte[] page, int bit)
        => page[format.IndexPage.BitmaskOffset + (bit / 8)] ^= checked((byte)(1 << (bit % 8)));

    private static async Task<List<CompactIndexRecord>> ScanAsync(MemoryPages source, JetFormat format, CompactIndexBudget? budget = null)
    {
        var records = new List<CompactIndexRecord>();
        await CompactIndexAudit.ScanAsync(
            source,
            format,
            2,
            3,
            budget ?? new(),
            (record, _) =>
            {
                records.Add(record);
                return ValueTask.CompletedTask;
            },
            Ct);
        return records;
    }

    private sealed class MemoryPages(int pageSize, Dictionary<long, byte[]> images) : IPageSource
    {
        public int PageSize => pageSize;

        public long PageCount { get; set; } = 20;

        public bool IsDisposed => false;

        internal Dictionary<long, byte[]> Images { get; } = images;

        internal int Reads { get; private set; }

        internal Exception? Failure { get; set; }

        internal bool Truncate { get; set; }

        public ValueTask<byte[]> ReadPageAsync(long pageNumber, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            this.Reads++;
            if (this.Failure is not null)
            {
                throw this.Failure;
            }

            byte[] buffer = ArrayPool<byte>.Shared.Rent(this.Truncate ? 16 : pageSize);
            if (!this.Truncate)
            {
                this.Images[pageNumber].CopyTo(buffer, 0);
            }

            return ValueTask.FromResult(buffer);
        }

        public void ThrowIfDisposed()
        {
        }

        public void ThrowIfDisposedOrCancelled(CancellationToken cancellationToken) => cancellationToken.ThrowIfCancellationRequested();
    }
}
