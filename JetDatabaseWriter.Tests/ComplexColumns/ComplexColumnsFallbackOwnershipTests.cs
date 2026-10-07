namespace JetDatabaseWriter.Tests.ComplexColumns;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Damaged complex metadata must never borrow cells from an unrelated table.</summary>
public sealed class ComplexColumnsFallbackOwnershipTests
{
    /// <summary>Checks damaged flat IDs, parent IDs and an unreadable complex catalog.</summary>
    /// <param name="damage">The catalog field to damage, or the catalog TDEF.</param>
    [Theory]
    [InlineData("FlatTableID")]
    [InlineData("ConceptualTableID")]
    [InlineData("TDEF")]
    [InlineData("Ambiguous")]
    [InlineData("ParentOnly")]
    [InlineData("WrongParent")]
    [InlineData("DescriptorDuplicate")]
    [InlineData("DescriptorOnly")]
    [InlineData("None")]
    public async Task Rows_DamagedCatalog_DoesNotReadAnotherParentsFiles(string damage)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await ComplexColumnTestSupport.CreateWriterAsync(stream))
        {
            foreach (string name in new[] { "Mail", "Docs" })
            {
                await writer.CreateTableAsync(
                    name,
                    [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true }],
                    ComplexColumnTestSupport.Ct);
                await writer.InsertRowAsync(name, [1, DBNull.Value], ComplexColumnTestSupport.Ct);
                await writer.AddAttachmentAsync(
                    name,
                    "Files",
                    new Dictionary<string, object?> { ["Id"] = 1 },
                    new AttachmentInput(name + ".txt", [1, 2, 3]),
                    ComplexColumnTestSupport.Ct);
            }
        }

        await DamageCatalogAsync(stream, damage);
        await using AccessReader reader = await ComplexColumnTestSupport.OpenReaderAsync(stream);
        using DataTable table = await reader.ReadTableAsync("Docs", cancellationToken: ComplexColumnTestSupport.Ct);
        object cell = Assert.Single(table.Rows.Cast<DataRow>())["Files"];
        if (damage is "None" or "DescriptorOnly")
        {
            Assert.Equal("Docs.txt", Assert.Single(ComplexCellValue.ReadAttachments(Assert.IsType<byte[]>(cell))).FileName);
        }
        else
        {
            Assert.IsType<DBNull>(cell);
        }
    }

    [Fact]
    public async Task Rows_MissingCatalog_DoesNotReadUserTableWithMatchingSuffix()
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await ComplexColumnTestSupport.CreateWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "X_Files",
                [new ColumnDefinition("Files", typeof(int)), new ColumnDefinition("FileName", typeof(string)), new ColumnDefinition("FileData", typeof(byte[]))],
                ComplexColumnTestSupport.Ct);
            await writer.InsertRowAsync("X_Files", [1, "spoof.txt", new byte[] { 1 }], ComplexColumnTestSupport.Ct);
            await writer.CreateTableAsync(
                "Docs",
                [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true }],
                ComplexColumnTestSupport.Ct);
            await writer.InsertRowAsync("Docs", [1, DBNull.Value], ComplexColumnTestSupport.Ct);
        }

        await DamageCatalogAsync(stream, "TDEF");
        await using AccessReader reader = await ComplexColumnTestSupport.OpenReaderAsync(stream);
        using DataTable table = await reader.ReadTableAsync("Docs", cancellationToken: ComplexColumnTestSupport.Ct);
        Assert.IsType<DBNull>(Assert.Single(table.Rows.Cast<DataRow>())["Files"]);
    }

    private static async Task DamageCatalogAsync(MemoryStream stream, string damage)
    {
        if (damage == "None")
        {
            return;
        }

        stream.Position = 0;
        await using WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: ComplexColumnTestSupport.Ct);
        long tdefPage = await harness.Services.CatalogRows.FindSystemTableTdefPageAsync("MSysComplexColumns", ComplexColumnTestSupport.Ct);
        if (damage == "TDEF")
        {
            byte[] page = await harness.Database.Pages.ReadPageCopyAsync(tdefPage, ComplexColumnTestSupport.Ct);
            page[0] = 0;
            await harness.Pager.WritePageAsync(tdefPage, page, ComplexColumnTestSupport.Ct);
            return;
        }

        ResolvedTable parent = await harness.Services.Catalog.ResolveRequiredTableAsync("Docs", ComplexColumnTestSupport.Ct);
        long parentPage = parent.Entry.TDefPage;
        long mailPage = damage == "WrongParent"
            ? (await harness.Services.Catalog.ResolveRequiredTableAsync("Mail", ComplexColumnTestSupport.Ct)).Entry.TDefPage
            : 0;
        int complexId = Assert.IsType<ColumnInfo>(parent.Definition.FindColumn("Files")).Misc;
        TableDef definition = Assert.IsType<TableDef>(await harness.Database.TableDefs.ReadTableDefAsync(tdefPage, ComplexColumnTestSupport.Ct));

        // Also clear ComplexID so the descriptor join cannot bypass the ownership fallback.
        string[] fields = damage switch
        {
            "DescriptorOnly" => ["ComplexID"],
            "ParentOnly" or "WrongParent" => ["ConceptualTableID"],
            "DescriptorDuplicate" => ["ComplexID", "ConceptualTableID"],
            "Ambiguous" => ["ComplexID", "ConceptualTableID"],
            _ => ["ComplexID", damage],
        };

        foreach (string field in fields)
        {
            ColumnInfo column = Assert.IsType<ColumnInfo>(definition.FindColumn(field));
            foreach (RowLocation location in await harness.Database.GetLiveRowLocationsAsync(tdefPage, ComplexColumnTestSupport.Ct))
            {
                byte[] page = await harness.Database.Pages.ReadPageCopyAsync(location.PageNumber, ComplexColumnTestSupport.Ct);
                Span<byte> value = page.AsSpan(location.RowStart + harness.Database.Format.RowFields.NumCols + column.FixedOff, 4);
                if ((damage is "Ambiguous" or "DescriptorDuplicate") && field == "ConceptualTableID")
                {
                    BinaryPrimitives.WriteInt32LittleEndian(value, checked((int)parentPage));
                }
                else if (damage == "DescriptorDuplicate")
                {
                    BinaryPrimitives.WriteInt32LittleEndian(value, complexId);
                }
                else if (damage == "WrongParent")
                {
                    BinaryPrimitives.WriteInt32LittleEndian(value, checked((int)mailPage));
                }
                else
                {
                    value.Clear();
                }

                await harness.Pager.WritePageAsync(location.PageNumber, page, ComplexColumnTestSupport.Ct);
            }
        }
    }
}
