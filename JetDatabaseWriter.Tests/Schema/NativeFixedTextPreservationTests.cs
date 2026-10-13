namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.ValueDecoding;
using JetDatabaseWriter.ValueDecoding.Models;
using Xunit;

/// <summary>Native fixed Text descriptors and raw slots survive unrelated edits.</summary>
public sealed class NativeFixedTextPreservationTests
{
    /// <summary>Gets native fixtures paired with every supported unrelated rewrite.</summary>
    public static TheoryData<string, string> Edits
    {
        get
        {
            var result = new TheoryData<string, string>();
            foreach (string file in new[] { "NativeFixedTextJet3Cp1252.mdb", "NativeFixedTextJet3Cp932.mdb", "NativeFixedTextJet4.mdb", "NativeFixedTextAce.accdb" })
            {
                foreach (string operation in new[] { "add", "drop", "rename-column", "update" })
                {
                    result.Add(file, operation);
                }
            }

            return result;
        }
    }

    [Theory]
    [MemberData(nameof(Edits))]
    public async Task NativeFixedText_UnrelatedEditsPreserveDescriptorAndStoredBytes(string fileName, string operation)
    {
        string path = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Databases", "FixedText", fileName);
        await using var stream = new MemoryStream();
        await stream.WriteAsync(await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken), TestContext.Current.CancellationToken);
        (ColumnInfo before, Dictionary<int, byte[]> beforeValues) = await ReadStorageAsync(stream, "FixedText", "Value");
        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            switch (operation)
            {
                case "add":
                    await writer.AddColumnAsync("FixedText", new ColumnDefinition("NewValue", typeof(int)), TestContext.Current.CancellationToken);
                    break;
                case "drop":
                    await writer.DropColumnAsync("FixedText", "Spare", TestContext.Current.CancellationToken);
                    break;
                case "rename-column":
                    await writer.RenameColumnAsync("FixedText", "Value", "Label", TestContext.Current.CancellationToken);
                    break;
                case "update":
                    Assert.Equal(beforeValues.Count, await writer.UpdateRowsAsync("FixedText", RowCriteria.All(), new RowValues { ["Spare"] = 8 }, TestContext.Current.CancellationToken));
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(operation), operation, "Unknown fixed Text test edit.");
            }
        }

        const string tableName = "FixedText";
        string columnName = operation == "rename-column" ? "Label" : "Value";
        (ColumnInfo after, Dictionary<int, byte[]> afterValues) = await ReadStorageAsync(stream, tableName, columnName);
        Assert.True(after.IsFixed);
        Assert.Equal(before.Size, after.Size);
        Assert.Equal(before.Flags, after.Flags);
        Assert.Equal(before.ExtraFlags, after.ExtraFlags);
        Assert.Equal(before.RawDescriptor, after.RawDescriptor);
        Assert.Equal(beforeValues.Count, afterValues.Count);
        foreach ((int id, byte[] value) in beforeValues)
        {
            Assert.Equal(value, afterValues[id]);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        using DataTable rows = await reader.ReadTableAsync(tableName, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal("A\0B   ", Assert.Single(rows.Select("Id = 4"))[columnName]);
        Assert.Equal(DBNull.Value, Assert.Single(rows.Select("Id = 5"))[columnName]);
        if (fileName is "NativeFixedTextJet4.mdb" or "NativeFixedTextAce.accdb")
        {
            Assert.Equal("\uFEFFA    ", Assert.Single(rows.Select("Id = 7"))[columnName]);
        }
    }

    [Theory]
    [InlineData("NativeFixedTextJet4.mdb")]
    [InlineData("NativeFixedTextAce.accdb")]
    public async Task NativeFixedText_ScalarMetadataReaderPreservesLeadingBom(string fileName)
    {
        string path = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Databases", "FixedText", fileName);
        await using var stream = new MemoryStream();
        await stream.WriteAsync(await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken), TestContext.Current.CancellationToken);
        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            // The catalog scalar reader assumes a variable-column trailer.
            // Keep the native fixed Text bytes while supplying that row shape.
            await writer.AddColumnAsync("FixedText", new ColumnDefinition("Tail", typeof(byte[])), TestContext.Current.CancellationToken);
            await writer.UpdateRowsAsync("FixedText", RowCriteria.All(), new RowValues { ["Tail"] = new byte[] { 1, 2, 3 } }, TestContext.Current.CancellationToken);
        }

        stream.Position = 0;
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(stream, cancellationToken: TestContext.Current.CancellationToken);
        CatalogEntry entry = Assert.IsType<CatalogEntry>(await harness.GetCatalogEntryAsync("FixedText", TestContext.Current.CancellationToken));
        TableDef definition = Assert.IsType<TableDef>(await harness.ReadTableDefAsync(entry.TDefPage, TestContext.Current.CancellationToken));
        ColumnInfo value = Assert.Single(definition.Columns, column => column.Name == "Value");
        ColumnInfo id = Assert.Single(definition.Columns, column => column.Name == "Id");
        Assert.True(definition.HasVarColumns);
        Assert.NotNull(DirectRowDecoderBuilder.TryBuildHybrid<FixedHybridRow>(["Value", "Id", "Spare", "Tail"], definition.Columns, definition.ClrTypes));
        bool found = false;
        for (long number = 1; number < harness.Database.Pages.PageCount; number++)
        {
            byte[] page = await harness.ReadPageCopyAsync(number, TestContext.Current.CancellationToken);
            JetFormat format = harness.Database.Format;
            if (page[0] != Constants.PageTypes.Data || JetTypeInfo.Ri32(page, format.DataPage.TDefOff) != entry.TDefPage)
            {
                continue;
            }

            foreach (RowBound bound in DataPageRows.EnumerateLiveRowBounds(format, page))
            {
                if (ScalarColumnReader.DecodeSimpleColumnValue(format, page, bound.RowStart, bound.RowSize, id) == "7")
                {
                    Assert.Equal("\uFEFFA    ", ScalarColumnReader.DecodeSimpleColumnValue(format, page, bound.RowStart, bound.RowSize, value));
                    found = true;
                }
            }
        }

        Assert.True(found);
        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        await foreach (FixedHybridRow row in reader.Rows<FixedHybridRow>("FixedText", cancellationToken: TestContext.Current.CancellationToken))
        {
            Assert.Equal(new byte[] { 1, 2, 3 }, row.Tail);
            if (row.Id == 7)
            {
                Assert.Equal("\uFEFFA    ", row.Value);
            }
        }
    }

    private static async Task<(ColumnInfo Column, Dictionary<int, byte[]> Values)> ReadStorageAsync(MemoryStream stream, string tableName, string columnName)
    {
        stream.Position = 0;
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(stream, cancellationToken: TestContext.Current.CancellationToken);
        CatalogEntry entry = Assert.IsType<CatalogEntry>(await harness.GetCatalogEntryAsync(tableName, TestContext.Current.CancellationToken));
        TableDef definition = Assert.IsType<TableDef>(await harness.ReadTableDefAsync(entry.TDefPage, TestContext.Current.CancellationToken));
        ColumnInfo value = Assert.Single(definition.Columns, column => column.Name == columnName);
        ColumnInfo id = Assert.Single(definition.Columns, column => column.Name == "Id");
        var values = new Dictionary<int, byte[]>();
        JetFormat format = harness.Database.Format;
        for (long number = 1; number < harness.Database.Pages.PageCount; number++)
        {
            byte[] page = await harness.ReadPageCopyAsync(number, TestContext.Current.CancellationToken);
            if (page[0] != Constants.PageTypes.Data || JetTypeInfo.Ri32(page, format.DataPage.TDefOff) != entry.TDefPage)
            {
                continue;
            }

            foreach (RowBound bound in DataPageRows.EnumerateLiveRowBounds(format, page))
            {
                Assert.False(bound.IsOverflowPointer);
                Assert.True(RowDecodePlan.TryParseRowLayout(format.RowFields, page, bound.RowStart, bound.RowSize, definition.HasVarColumns, out RowLayout layout));
                ColumnSlice idSlice = RowDecodePlan.ResolveColumnSlice(format.RowFields, page, bound.RowStart, bound.RowSize, layout, id);
                int rowId = JetTypeInfo.Ri32(page, bound.RowStart + idSlice.DataStart);
                values.Add(rowId, page.AsSpan(bound.RowStart + format.RowFields.NumCols + value.FixedOff, value.Size).ToArray());
            }
        }

        Assert.NotEmpty(values);
        return (value, values);
    }

#pragma warning disable CA1812 // Rows<T> creates this projection through the compiled hybrid decoder.
    private sealed class FixedHybridRow
    {
        public int Id { get; set; }

        public string? Value { get; set; }

        public byte[]? Tail { get; set; }
    }
#pragma warning restore CA1812
}
