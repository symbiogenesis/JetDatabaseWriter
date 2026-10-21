namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.LongValues;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>Synthetic opaque markers check lossless schema projection independently of native acceptance.</summary>
public sealed class TDefRewriteBytePreservationTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Gets every format, operation and writer mode.</summary>
    public static TheoryData<DatabaseFormat, string, WriteMode> FormatsAndOperations()
        => TableRewriteIdentityTests.FormatsAndOperations();

    /// <summary>Gets every format and writer mode for chain growth and shrinkage.</summary>
    public static TheoryData<DatabaseFormat, WriteMode> FormatsAndModes()
        => WriteModes.Combine(DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb);

    [Theory]
    [MemberData(nameof(FormatsAndOperations))]
    public async Task Rewrite_PreservesOpaqueHeaderTrailerContinuationAndPadding(DatabaseFormat format, string operation, WriteMode mode)
    {
        await using var stream = new MemoryStream();
        string payload = new('x', 12000);
        var columns = new List<ColumnDefinition>
        {
            new("Id", typeof(int)) { IsPrimaryKey = true },
            new("Spare", typeof(int)),
            new("Payload", typeof(string)),
        };
        columns.AddRange(Enumerable.Range(0, 100).Select(index => new ColumnDefinition($"ContinuationColumn{index:D3}", typeof(int))));
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, WriteModes.WriterOptions(WriteMode.Direct), leaveOpen: true, Ct))
        {
            await writer.CreateTableAsync("Original", columns, Ct);
            await writer.InsertRowAsync("Original", [7, 9, payload, .. Enumerable.Range(0, 100).Select(index => (object)index)], Ct);
        }

        Snapshot before = await StampMarkersAsync(stream);
        Assert.True(before.Pages.Length > 1);
        stream.Position = 0;
        await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, WriteModes.WriterOptions(mode), cancellationToken: Ct))
        {
            await WriteModes.RunAsync(
                writer,
                mode,
                async () =>
                {
                    if (operation == "add")
                    {
                        await writer.AddColumnAsync("Original", new ColumnDefinition("Added", typeof(int)), Ct);
                    }
                    else if (operation == "drop")
                    {
                        await writer.DropColumnAsync("Original", "Spare", Ct);
                    }
                    else
                    {
                        await writer.RenameColumnAsync("Original", "Spare", "Renamed", Ct);
                    }
                },
                Ct);
        }

        await AssertMarkersAsync(stream, before);
        stream.Position = 0;
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct);
        Assert.Equal("Original", Assert.Single(await reader.ListTablesAsync(Ct)));
        using DataTable rows = await reader.ReadTableAsync("Original", cancellationToken: Ct);
        DataRow row = Assert.Single(rows.AsEnumerable());
        Assert.Equal(payload, row["Payload"]);
        Assert.Equal(99, row["ContinuationColumn099"]);
        if (operation != "drop")
        {
            Assert.Equal(9, row[operation == "rename" ? "Renamed" : "Spare"]);
        }

        IndexMetadata primary = Assert.Single(await reader.ListIndexesAsync("Original", Ct), index => index.Kind == IndexKind.PrimaryKey);
        Assert.Single(await reader.SeekRowsAsync("Original", primary.Name, [7], Ct).ToListAsync(Ct));
    }

    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task Rewrite_GrowthAndShrinkPreserveDeclaredOpaqueTrailer(DatabaseFormat format, WriteMode mode)
    {
        await using var stream = new MemoryStream();
        var profile = JetFormat.ForNewDatabase(format);
        var columns = new List<ColumnDefinition> { new("Id", typeof(int)), new("Payload", typeof(string), 80) };
        var builder = new TDefPageBuilder(profile, null!);
        while (true)
        {
            TableDef definition = TDefPageBuilder.BuildTableDefinition(columns, profile);
            byte[] bytes = builder.BuildTDefPage(definition);
            if (Ri32(bytes, 8) + 8 > profile.PageSize - (profile.IsJet3 ? 90 : 140))
            {
                break;
            }

            columns.Add(new ColumnDefinition($"Column{columns.Count:D3}", typeof(int)));
        }

        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, WriteModes.WriterOptions(WriteMode.Direct), leaveOpen: true, Ct))
        {
            await writer.CreateTableAsync("Original", columns, Ct);
            await writer.InsertRowAsync("Original", [7, "surviving payload", .. Enumerable.Range(2, columns.Count - 2).Select(index => (object)index)], Ct);
        }

        Snapshot before = await StampMarkersAsync(stream);
        Assert.Single(before.Pages);
        string added = new('A', 64);
        stream.Position = 0;
        await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, WriteModes.WriterOptions(mode), cancellationToken: Ct))
        {
            await WriteModes.RunAsync(writer, mode, async () => await writer.AddColumnAsync("Original", new ColumnDefinition(added, typeof(int)), Ct), Ct);
        }

        Snapshot grown = await AssertMarkersAsync(stream, before);
        Assert.True(grown.Pages.Length > before.Pages.Length);
        stream.Position = 0;
        await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, WriteModes.WriterOptions(mode), cancellationToken: Ct))
        {
            await WriteModes.RunAsync(writer, mode, async () => await writer.DropColumnAsync("Original", added, Ct), Ct);
        }

        Snapshot shrunk = await AssertMarkersAsync(stream, grown);
        Assert.Single(shrunk.Pages);
        stream.Position = 0;
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct);
        Assert.Equal("Original", Assert.Single(await reader.ListTablesAsync(Ct)));
        using DataTable rows = await reader.ReadTableAsync("Original", cancellationToken: Ct);
        Assert.Equal("surviving payload", Assert.Single(rows.AsEnumerable())["Payload"]);
    }

    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task Rewrite_PreservesNativeLongValueMapOrderAndProjectsAddedDroppedAndRenamedColumns(DatabaseFormat format, WriteMode mode)
    {
        await using var stream = new MemoryStream();
        string payload = new('p', 12000);
        string dropped = new('d', 11000);
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, WriteModes.WriterOptions(WriteMode.Direct), leaveOpen: true, Ct))
        {
            await writer.CreateTableAsync("Original", [new("Id", typeof(int)) { IsPrimaryKey = true }, new("DropMemo", typeof(string)), new("Payload", typeof(string))], Ct);
            await writer.InsertRowAsync("Original", [1, dropped, payload], Ct);
        }

        Snapshot before = await StampMarkersAsync(stream);
        stream.Position = 0;
        await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct))
        {
            LogicalTDefChain chain = await writer.Database.TableDefs.ReadTDefChainAsync(before.Root, Ct);
            Assert.True(LongValueUsageMaps.TryRead(writer.Database.Format, chain.Bytes, out List<LongValueUsageMap> maps, out _));
            Assert.Equal(2, maps.Count);
            byte[] first = chain.Bytes.AsSpan(maps[0].OwnedOffset - 2, 10).ToArray();
            chain.Bytes.AsSpan(maps[1].OwnedOffset - 2, 10).CopyTo(chain.Bytes.AsSpan(maps[0].OwnedOffset - 2));
            first.CopyTo(chain.Bytes.AsSpan(maps[1].OwnedOffset - 2));
            await chain.WriteInPlaceAsync(writer.Pager.ReadPageAsync, PageBuffers.Return, writer.Pager.WritePageAsync, Ct);
        }

        Assert.Equal(["Payload", "DropMemo"], await ReadMapNamesAsync(stream));
        foreach (string operation in new[] { "add", "rename", "drop" })
        {
            stream.Position = 0;
            await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, WriteModes.WriterOptions(mode), cancellationToken: Ct))
            {
                await WriteModes.RunAsync(
                    writer,
                    mode,
                    async () =>
                    {
                        if (operation == "add")
                        {
                            await writer.AddColumnAsync("Original", new ColumnDefinition("AddedMemo", typeof(string)), Ct);
                        }
                        else if (operation == "rename")
                        {
                            await writer.RenameColumnAsync("Original", "Payload", "RenamedPayload", Ct);
                        }
                        else
                        {
                            await writer.DropColumnAsync("Original", "DropMemo", Ct);
                        }
                    },
                    Ct);
            }

            before = await AssertMarkersAsync(stream, before);
            string payloadName = operation == "add" ? "Payload" : "RenamedPayload";
            string[] expected = operation == "drop" ? [payloadName, "AddedMemo"] : [payloadName, "DropMemo", "AddedMemo"];
            Assert.Equal(expected, await ReadMapNamesAsync(stream));
            stream.Position = 0;
            await using AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct);
            using DataTable rows = await reader.ReadTableAsync("Original", cancellationToken: Ct);
            DataRow row = Assert.Single(rows.AsEnumerable());
            Assert.Equal(payload, row[payloadName]);
            if (operation != "drop")
            {
                Assert.Equal(dropped, row["DropMemo"]);
            }
        }
    }

    private static async Task<string[]> ReadMapNamesAsync(MemoryStream stream)
    {
        stream.Position = 0;
        await using AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct);
        long root = (await writer.Services.Catalog.ResolveRequiredTableAsync("Original", Ct)).Entry.TDefPage;
        LogicalTDefChain chain = await writer.Database.TableDefs.ReadTDefChainAsync(root, Ct);
        TDefImage image = Assert.IsType<TDefImage>(TDefCodec.Parse(writer.Database.Format, chain.Bytes));
        Assert.True(LongValueUsageMaps.TryRead(writer.Database.Format, chain.Bytes, out List<LongValueUsageMap> maps, out _));
        return maps.Select(map => Assert.Single(image.Columns, column => column.ColNum == map.ColumnNumber).Name).ToArray();
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public void Composer_OpaqueTrailerGrowthRespectsCompletePhysicalDefinitionBudget(DatabaseFormat format)
    {
        var profile = JetFormat.ForNewDatabase(format);
        var builder = new TDefPageBuilder(profile, null!);
        TableDef definition = TDefPageBuilder.BuildTableDefinition([new("Id", typeof(int))], profile);
        byte[] original = builder.BuildTDefPage(definition);
        int originalEnd = checked(Ri32(original, 8) + 8);
        int paddedEnd = profile.PageSize - 64;
        original.AsSpan(originalEnd, paddedEnd - originalEnd).Fill(0xE7);
        Wi32(original, 8, paddedEnd - 8);
        TDefImage image = Assert.IsType<TDefImage>(TDefCodec.Parse(profile, original));
        TableDef projected = TDefPageBuilder.BuildTableDefinition([new("Id", typeof(int)), new(new string('A', 64), typeof(int))], profile);
        byte[] replacement = builder.BuildTDefPage(projected);
        Assert.True(checked(Ri32(replacement, 8) + 8) < profile.PageSize);
        byte[] before = original.ToArray();

        Assert.Throws<JetLimitationException>(() => TDefRewriteComposer.Compose(profile, image, replacement, static name => name, maxLogicalBytes: profile.PageSize));
        Assert.Equal(before, original);
        int capacity = LogicalTDefChain.GetLogicalCapacity(profile.PageSize, profile.PageSize + 1);
        Assert.Throws<JetLimitationException>(() => TDefRewriteComposer.Compose(profile, image, replacement, static name => name, maxLogicalBytes: capacity - 1));
        byte[] composed = TDefRewriteComposer.Compose(profile, image, replacement, static name => name, maxLogicalBytes: capacity);
        Assert.Equal(capacity, composed.Length);
        int composedEnd = checked(Ri32(composed, 8) + 8);
        Assert.True(composedEnd > profile.PageSize);
        int trailerLength = paddedEnd - originalEnd + 10;
        Assert.Equal(original.AsSpan(originalEnd - 10, trailerLength).ToArray(), composed.AsSpan(composedEnd - trailerLength, trailerLength).ToArray());
    }

    private static async Task<Snapshot> StampMarkersAsync(MemoryStream stream)
    {
        stream.Position = 0;
        await using AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct);
        long root = (await writer.Services.Catalog.ResolveRequiredTableAsync("Original", Ct)).Entry.TDefPage;
        LogicalTDefChain chain = await writer.Database.TableDefs.ReadTDefChainAsync(root, Ct);
        byte[] bytes = chain.Bytes;
        bytes[1] = 0x6A;
        if (!writer.Database.Format.IsJet3)
        {
            bytes.AsSpan(24, 4).Fill(0xA7);
            bytes.AsSpan(32, 8).Fill(0xB8);
        }

        int descriptorStart = writer.Database.Format.TDef.BlockEnd + (Ri32(bytes, writer.Database.Format.TDef.NumRealIdx) * writer.Database.Format.TDef.RealIdxEntrySz);
        TDefImage image = Assert.IsType<TDefImage>(TDefCodec.Parse(writer.Database.Format, bytes));
        int payloadOrdinal = Assert.Single(image.Columns, column => column.Name == "Payload").ColNum;
        int payloadDescriptor = descriptorStart + (payloadOrdinal * writer.Database.Format.ColumnDescriptor.Size);
        if (writer.Database.Format.IsJet3)
        {
            Wu16(bytes, payloadDescriptor + writer.Database.Format.ColumnDescriptor.CodePageOff, 1251);
        }
        else
        {
            bytes[payloadDescriptor + writer.Database.Format.ColumnDescriptor.SortVersionOff] = 1;
        }

        int used = checked(Ri32(bytes, 8) + 8);
        Assert.Equal(ushort.MaxValue, Ru16(bytes, used - 10));
        bytes.AsSpan(used - 8, 8).Fill(0xC9);
        byte[] trailer = [0xD1, 0x82, 0x03, 0xF4, 0xA5, 0x76, 0x67, 0x58, 0x39, 0x2A, 0x1B];
        trailer.CopyTo(bytes, used);
        used += trailer.Length;
        await chain.WriteAsync(bytes, used, writer.Services.PageAllocator.AllocatePageAsync, writer.Pager.WritePageAsync, writer.Services.PageAllocator.DeallocatePageAsync, writer.Database.Format.WritesTDefFreeSpace, Ct);
        for (int index = 0; index < chain.PageNumbers.Count; index++)
        {
            long pageNumber = chain.PageNumbers[index];
            byte[] page = await writer.Database.Pages.ReadPageCopyAsync(pageNumber, Ct);
            if (index > 0)
            {
                page[1] = checked((byte)(0x70 + index));
                page[2] = 0xE3;
                page[3] = 0xF2;
            }

            if (index == chain.PageNumbers.Count - 1)
            {
                page.AsSpan(page.Length - 16).Fill(0xED);
            }

            await writer.Pager.WritePageAsync(pageNumber, page, Ct);
        }

        return await ReadSnapshotAsync(writer, root);
    }

    private static async Task<Snapshot> AssertMarkersAsync(MemoryStream stream, Snapshot before)
    {
        stream.Position = 0;
        await using AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct);
        long root = (await writer.Services.Catalog.ResolveRequiredTableAsync("Original", Ct)).Entry.TDefPage;
        Snapshot after = await ReadSnapshotAsync(writer, root);
        Assert.Equal(before.Root, after.Root);
        Assert.Equal(before.Header, after.Header);
        Assert.Equal(before.Trailer, after.Trailer);
        Assert.Equal(before.SortOrder, after.SortOrder);
        Assert.Equal(before.SortBytes, after.SortBytes);
        int retained = Math.Min(before.Pages.Length, after.Pages.Length);
        Assert.Equal(before.Pages.Take(retained), after.Pages.Take(retained));
        Assert.Equal(before.ContinuationHeaders.Take(retained - 1), after.ContinuationHeaders.Take(retained - 1));
        int paddingStart = Math.Max(before.UsedLength, after.UsedLength);
        int paddingEnd = Math.Min(before.Bytes.Length, after.Bytes.Length);
        if (paddingStart < paddingEnd)
        {
            Assert.Equal(before.Bytes.AsSpan(paddingStart, paddingEnd - paddingStart).ToArray(), after.Bytes.AsSpan(paddingStart, paddingEnd - paddingStart).ToArray());
        }

        return after;
    }

    private static async Task<Snapshot> ReadSnapshotAsync(AccessWriter writer, long root)
    {
        LogicalTDefChain chain = await writer.Database.TableDefs.ReadTDefChainAsync(root, Ct);
        byte[] bytes = chain.Bytes;
        int used = checked(Ri32(bytes, 8) + 8);
        TDefImage image = Assert.IsType<TDefImage>(TDefCodec.Parse(writer.Database.Format, bytes));
        var headers = new List<uint>();
        foreach (long page in chain.PageNumbers.Skip(1))
        {
            headers.Add(Ru32(await writer.Database.Pages.ReadPageCopyAsync(page, Ct), 0));
        }

        byte[] header = writer.Database.Format.IsJet3 ? [bytes[1]] : [bytes[1], .. bytes.AsSpan(24, 4).ToArray(), .. bytes.AsSpan(32, 8).ToArray()];
        ColumnInfo payload = Assert.Single(image.Columns, column => column.Name is "Payload" or "RenamedPayload");
        return new Snapshot(root, [.. chain.PageNumbers], header, bytes.AsSpan(used - 21, 21).ToArray(), payload.TextSortOrder, payload.RawDescriptor.Skip(writer.Database.Format.ColumnDescriptor.SortOrderOff).Take(4).ToArray(), [.. headers], used, bytes);
    }

    private sealed record Snapshot(long Root, long[] Pages, byte[] Header, byte[] Trailer, JetDatabaseWriter.Indexes.Collation.TextSortOrder SortOrder, byte[] SortBytes, uint[] ContinuationHeaders, int UsedLength, byte[] Bytes);
}
