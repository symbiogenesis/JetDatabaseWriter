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

/// <summary>A writer snapshot must refuse undecodable live rows instead of omitting them from a rewrite.</summary>
public sealed class MalformedRowWriteBackTests
{
    private const string TableName = "Items";

    /// <summary>Gets reachable malformed-row cases for every mutation and write mode.</summary>
    public static TheoryData<DatabaseFormat, WriteMode, bool, bool> Cases()
    {
        var data = new TheoryData<DatabaseFormat, WriteMode, bool, bool>();
        foreach (DatabaseFormat format in new[] { DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb })
        {
            foreach (WriteMode mode in new[] { WriteMode.Direct, WriteMode.AutoCommit, WriteMode.ExplicitCommit })
            {
                foreach (bool schemaEdit in new[] { false, true })
                {
                    data.Add(format, mode, false, schemaEdit);
                    if (format != DatabaseFormat.Jet3Mdb)
                    {
                        data.Add(format, mode, true, schemaEdit);
                    }
                }
            }
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task WriteBack_InvalidLiveRow_RefusesWithoutChangingFile(DatabaseFormat databaseFormat, WriteMode mode, bool shortRow, bool schemaEdit)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, databaseFormat, WriteModes.WriterOptions(WriteMode.Direct), leaveOpen: true, ct))
        {
            await writer.CreateTableAsync(TableName, [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Note", typeof(string), 20)], ct);
            await writer.InsertRowsAsync(TableName, [[1, "keep"], [2, "malformed"]], ct);
        }

        RowLocation malformed = await DamageSecondRowAsync(stream, shortRow, ct);
        byte[] before = stream.ToArray();

        // A lenient public read still exposes the intact row. Writer snapshots
        // must not use that omission policy when they will write values back.
        stream.Position = 0;
        await using (AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, ct))
        {
            using DataTable table = await reader.ReadTableAsync(TableName, cancellationToken: ct);
            DataRow row = Assert.Single(table.AsEnumerable());
            Assert.Equal(1, row["Id"]);
            Assert.Equal("keep", row["Note"]);
        }

        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, WriteModes.WriterOptions(mode), leaveOpen: true, ct))
        {
            await WriteModes.RunAsync(
                writer,
                mode,
                async () =>
                {
                    JetCorruptDataException failure = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
                    {
                        if (schemaEdit)
                        {
                            await writer.AddColumnAsync(TableName, new ColumnDefinition("Extra", typeof(int)), ct);
                        }
                        else
                        {
                            await writer.UpdateRowsAsync(TableName, "Id", 1, new Dictionary<string, object?> { ["Note"] = "changed" }, ct);
                        }
                    });
                    Assert.Equal(JetErrorCode.MalformedValue, failure.ErrorCode);
                    Assert.Equal(TableName, failure.ErrorInfo.TableName);
                    Assert.Equal(malformed.DataPageNumber, failure.ErrorInfo.PageNumber);
                    Assert.Contains($"slot {malformed.DataRowIndex}", Assert.IsType<string>(failure.ErrorInfo.Reason), StringComparison.Ordinal);
                    Assert.Equal(before, stream.ToArray());
                },
                ct);
        }

        Assert.Equal(before, stream.ToArray());
    }

    private static async Task<RowLocation> DamageSecondRowAsync(MemoryStream stream, bool shortRow, CancellationToken ct)
    {
        stream.Position = 0;
        RowLocation malformed;
        long tdefPage;
        await using (ReaderHarness harness = await ReaderHarness.OpenAsync(stream, cancellationToken: ct))
        {
            CatalogEntry entry = Assert.IsType<CatalogEntry>(await harness.GetCatalogEntryAsync(TableName, ct));
            tdefPage = entry.TDefPage;
            RowLocation[] locations = [.. await harness.Database.GetLiveRowLocationsAsync(tdefPage, ct)];
            Assert.Equal(2, locations.Length);
            RowLocation intact = locations[0];
            malformed = locations[1];
            Assert.False(intact.IsOverflow);
            Assert.False(malformed.IsOverflow);
            Assert.Equal(intact.DataPageNumber, malformed.DataPageNumber);
            JetFormat format = harness.Database.Format;
            int pageStart = checked((int)(malformed.DataPageNumber * format.PageSize));
            byte[] file = stream.GetBuffer();
            if (shortRow)
            {
                Assert.Equal(2, format.RowFields.NumCols);
                int rowStart = intact.RowStart - 1;
                BinaryPrimitives.WriteUInt16LittleEndian(file.AsSpan(pageStart + format.DataPage.RowsStart + (malformed.DataRowIndex * sizeof(ushort)), sizeof(ushort)), checked((ushort)rowStart));
                file[pageStart + rowStart] = 0;
                malformed = malformed with { RowStart = rowStart, RowSize = 1 };
            }
            else
            {
                file.AsSpan(pageStart + malformed.RowStart, format.RowFields.NumCols).Clear();
            }
        }

        // Both defects remain live slots and reach the writer's row visitor.
        stream.Position = 0;
        await using (ReaderHarness harness = await ReaderHarness.OpenAsync(stream, cancellationToken: ct))
        {
            Assert.Contains(malformed, await harness.Database.GetLiveRowLocationsAsync(tdefPage, ct));
        }

        return malformed;
    }
}
