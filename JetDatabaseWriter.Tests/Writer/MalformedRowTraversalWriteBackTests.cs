namespace JetDatabaseWriter.Tests.Writer;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Write-back traversal refuses live content that a lenient row scan cannot reach safely.</summary>
public sealed class MalformedRowTraversalWriteBackTests
{
    private const string TableName = "Items";

    public static TheoryData<DatabaseFormat, WriteMode, bool, string> DirectoryCases()
    {
        var cases = new TheoryData<DatabaseFormat, WriteMode, bool, string>();
        foreach (DatabaseFormat format in Enum.GetValues<DatabaseFormat>())
        {
            foreach (WriteMode mode in Enum.GetValues<WriteMode>())
            {
                foreach (bool schemaEdit in new[] { false, true })
                {
                    foreach (string damage in new[] { "zero", "header", "outside", "count", "duplicate" })
                    {
                        cases.Add(format, mode, schemaEdit, damage);
                    }
                }
            }
        }

        return cases;
    }

    public static TheoryData<DatabaseFormat, WriteMode, bool, OverflowRowLayout> OverflowCases()
    {
        var cases = new TheoryData<DatabaseFormat, WriteMode, bool, OverflowRowLayout>();
        foreach (DatabaseFormat format in Enum.GetValues<DatabaseFormat>())
        {
            foreach (WriteMode mode in Enum.GetValues<WriteMode>())
            {
                foreach (bool schemaEdit in new[] { false, true })
                {
                    foreach (OverflowRowLayout layout in new[]
                    {
                        OverflowRowLayout.ShortPointer,
                        OverflowRowLayout.PointerPastEndOfFile,
                        OverflowRowLayout.Cycle,
                    })
                    {
                        cases.Add(format, mode, schemaEdit, layout);
                    }
                }
            }
        }

        return cases;
    }

    public static TheoryData<DatabaseFormat, WriteMode, bool> ValidAliasCases()
    {
        var cases = new TheoryData<DatabaseFormat, WriteMode, bool>();
        foreach (DatabaseFormat format in Enum.GetValues<DatabaseFormat>())
        {
            foreach (WriteMode mode in Enum.GetValues<WriteMode>())
            {
                cases.Add(format, mode, false);
                cases.Add(format, mode, true);
            }
        }

        return cases;
    }

    [Theory]
    [MemberData(nameof(DirectoryCases))]
    public async Task InvalidLiveDirectory_RefusesMutationWithoutChangingFile(DatabaseFormat format, WriteMode mode, bool schemaEdit, string damage)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = await CreateSmallTableAsync(format, ct);
        RowLocation damaged = await ChangeDirectoryAsync(stream, damage, ct);

        await AssertMutationRefusedAsync(stream, mode, schemaEdit, damaged.PageNumber, damage == "count" ? null : damaged.RowIndex, ct);
    }

    [Theory]
    [MemberData(nameof(OverflowCases))]
    public async Task UnresolvedOverflow_RefusesMutationWithoutChangingFile(DatabaseFormat format, WriteMode mode, bool schemaEdit, OverflowRowLayout layout)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        byte[] image = await SyntheticOverflowRows.CreateTableAsync(format, TableName, 80, primaryKey: false, ct);
        SyntheticOverflowRow damaged = await SyntheticOverflowRows.MoveRowAsync(image, TableName, layout, ct);
        await using var stream = new MemoryStream();
        await stream.WriteAsync(image, ct);
        stream.Position = 0;

        // Reader omission remains deliberate; a writer must not adopt it.
        await using (AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, ct))
        {
            using DataTable rows = await reader.ReadTableAsync(TableName, cancellationToken: ct);
            Assert.DoesNotContain(rows.AsEnumerable(), row => (int)row["Id"] == 1);
            Assert.Contains(rows.AsEnumerable(), row => (int)row["Id"] == 2);
        }

        await AssertMutationRefusedAsync(stream, mode, schemaEdit, damaged.HeaderPage, damaged.HeaderRow, ct);
    }

    [Theory]
    [MemberData(nameof(ValidAliasCases))]
    public async Task DeletedOffsetAlias_AllowsMutationAndPreservesBothLiveRows(DatabaseFormat format, WriteMode mode, bool schemaEdit)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = await CreateSmallTableAsync(format, ct);
        _ = await ChangeDirectoryAsync(stream, "deleted-alias", ct);
        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, WriteModes.WriterOptions(mode), leaveOpen: true, ct))
        {
            await WriteModes.RunAsync(writer, mode, () => MutateAsync(writer, schemaEdit, ct), ct);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, ct);
        using DataTable rows = await reader.ReadTableAsync(TableName, cancellationToken: ct);
        Assert.Equal(2, rows.Rows.Count);
        Assert.Equal(schemaEdit ? "second" : "changed", Assert.Single(rows.AsEnumerable(), row => (int)row["Id"] == 2)["Name"]);
        Assert.Equal("first", Assert.Single(rows.AsEnumerable(), row => (int)row["Id"] == 1)["Name"]);
    }

    private static async Task<MemoryStream> CreateSmallTableAsync(DatabaseFormat format, CancellationToken ct)
    {
        var stream = new MemoryStream();
        try
        {
            await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, WriteModes.WriterOptions(WriteMode.Direct), leaveOpen: true, ct);
            await writer.CreateTableAsync(TableName, [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Name", typeof(string), 20)], ct);
            await writer.InsertRowsAsync(TableName, [[1, "first"], [2, "second"]], ct);
            return stream;
        }
        catch
        {
            await stream.DisposeAsync();
            throw;
        }
    }

    private static async Task<RowLocation> ChangeDirectoryAsync(MemoryStream stream, string damage, CancellationToken ct)
    {
        stream.Position = 0;
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(stream, cancellationToken: ct);
        CatalogEntry entry = Assert.IsType<CatalogEntry>(await harness.GetCatalogEntryAsync(TableName, ct));
        RowLocation[] rows = [.. await harness.Database.GetLiveRowLocationsAsync(entry.TDefPage, ct)];
        Assert.Equal(2, rows.Length);
        Assert.Equal(rows[0].PageNumber, rows[1].PageNumber);
        RowLocation damaged = rows[0];
        JetFormat format = harness.Database.Format;
        int pageStart = checked((int)(damaged.PageNumber * format.PageSize));
        byte[] image = stream.GetBuffer();
        int slotOffset = pageStart + format.DataPage.RowsStart + (damaged.RowIndex * sizeof(ushort));
        ushort offset = damage switch
        {
            "zero" => 0,
            "header" => checked((ushort)format.DataPage.RowsStart),
            "outside" => checked((ushort)format.PageSize),
            "duplicate" => checked((ushort)rows[1].RowStart),
            "count" or "deleted-alias" => checked((ushort)damaged.RowStart),
            _ => throw new ArgumentException("Unknown directory damage.", nameof(damage)),
        };
        BinaryPrimitives.WriteUInt16LittleEndian(image.AsSpan(slotOffset, sizeof(ushort)), offset);
        if (damage == "count")
        {
            BinaryPrimitives.WriteUInt16LittleEndian(image.AsSpan(pageStart + format.DataPage.NumRows, sizeof(ushort)), ushort.MaxValue);
        }
        else if (damage == "deleted-alias")
        {
            BinaryPrimitives.WriteUInt16LittleEndian(image.AsSpan(pageStart + format.DataPage.NumRows, sizeof(ushort)), 3);
            BinaryPrimitives.WriteUInt16LittleEndian(image.AsSpan(pageStart + format.DataPage.RowsStart + (2 * sizeof(ushort)), sizeof(ushort)), checked((ushort)(offset | 0x8000)));
        }

        return damaged;
    }

    private static async Task AssertMutationRefusedAsync(MemoryStream stream, WriteMode mode, bool schemaEdit, long pageNumber, int? rowIndex, CancellationToken ct)
    {
        byte[] before = stream.ToArray();
        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, WriteModes.WriterOptions(mode), leaveOpen: true, ct))
        {
            await WriteModes.RunAsync(
                writer,
                mode,
                async () =>
                {
                    JetCorruptDataException failure = await Assert.ThrowsAsync<JetCorruptDataException>(() => MutateAsync(writer, schemaEdit, ct));
                    Assert.Equal(JetErrorCode.MalformedValue, failure.ErrorCode);
                    Assert.Equal(TableName, failure.ErrorInfo.TableName);
                    Assert.Equal(pageNumber, failure.ErrorInfo.PageNumber);
                    if (rowIndex is int slot)
                    {
                        Assert.Contains($"slot {slot}", Assert.IsType<string>(failure.ErrorInfo.Reason), StringComparison.Ordinal);
                    }

                    Assert.Equal(before, stream.ToArray());
                },
                ct);
        }

        Assert.Equal(before, stream.ToArray());
    }

    private static async Task MutateAsync(AccessWriter writer, bool schemaEdit, CancellationToken ct)
    {
        if (schemaEdit)
        {
            await writer.AddColumnAsync(TableName, new ColumnDefinition("Extra", typeof(int)), ct);
        }
        else
        {
            Assert.Equal(1, await writer.UpdateRowsAsync(TableName, "Id", 2, new Dictionary<string, object?> { ["Name"] = "changed" }, ct));
        }
    }
}
