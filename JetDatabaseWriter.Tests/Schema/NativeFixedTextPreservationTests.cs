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

    /// <summary>Gets malformed native fixed Text slots paired with every unrelated rewrite.</summary>
    public static TheoryData<string, string, bool> MalformedEdits
    {
        get
        {
            var result = new TheoryData<string, string, bool>();
            foreach (string file in new[] { "NativeFixedTextJet3Cp932.mdb", "NativeFixedTextJet4.mdb", "NativeFixedTextAce.accdb" })
            {
                foreach (string operation in new[] { "add", "drop", "rename-column", "update" })
                {
                    result.Add(file, operation, false);
                    result.Add(file, operation, true);
                }
            }

            return result;
        }
    }

    [Theory]
    [MemberData(nameof(MalformedEdits))]
    public async Task NativeFixedText_MalformedUnassignedSlotSurvivesEdits(string fileName, string operation, bool nullSlot)
    {
        string path = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Databases", "FixedText", fileName);
        await using var stream = new MemoryStream();
        await stream.WriteAsync(await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken), TestContext.Current.CancellationToken);
        await DamageFixedTextAsync(stream, nullSlot ? 5 : 1);
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

        (ColumnInfo after, Dictionary<int, byte[]> afterValues) = await ReadStorageAsync(stream, "FixedText", operation == "rename-column" ? "Label" : "Value");
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
        using DataTable rows = await reader.ReadTableAsync("FixedText", cancellationToken: TestContext.Current.CancellationToken);
        string columnName = operation == "rename-column" ? "Label" : "Value";
        Assert.Equal(DBNull.Value, Assert.Single(rows.Select("Id = 5"))[columnName]);
        if (operation == "update")
        {
            Assert.All(rows.Select(), row => Assert.Equal(8, row["Spare"]));
        }
    }

    [Theory]
    [InlineData("NativeFixedTextJet4.mdb", false)]
    [InlineData("NativeFixedTextJet4.mdb", true)]
    [InlineData("NativeFixedTextAce.accdb", false)]
    [InlineData("NativeFixedTextAce.accdb", true)]
    public async Task NativeFixedText_ExplicitEqualAssignmentReencodesSlot(string fileName, bool nullSlot)
    {
        string path = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Databases", "FixedText", fileName);
        await using var stream = new MemoryStream();
        await stream.WriteAsync(await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken), TestContext.Current.CancellationToken);
        int targetId = nullSlot ? 5 : 1;
        await DamageFixedTextAsync(stream, targetId);
        object assigned;
        stream.Position = 0;
        await using (AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            using DataTable rows = await reader.ReadTableAsync("FixedText", cancellationToken: TestContext.Current.CancellationToken);
            assigned = Assert.Single(rows.Select($"Id = {targetId}"))["Value"];
        }

        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            Assert.Equal(1, await writer.UpdateRowsAsync("FixedText", RowCriteria.Where("Id", targetId), new RowValues { ["Value"] = assigned }, TestContext.Current.CancellationToken));
        }

        (ColumnInfo column, Dictionary<int, byte[]> stored) = await ReadStorageAsync(stream, "FixedText", "Value");
        byte[] expected = nullSlot ? new byte[column.Size] : System.Text.Encoding.Unicode.GetBytes(Assert.IsType<string>(assigned));
        Assert.Equal(expected, stored[targetId]);
    }

    [Fact]
    public async Task NativeFixedText_ExplicitInvalidCodePageAssignmentRefusesWithoutMutation()
    {
        string path = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Databases", "FixedText", "NativeFixedTextJet3Cp932.mdb");
        await using var stream = new MemoryStream();
        await stream.WriteAsync(await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken), TestContext.Current.CancellationToken);
        await DamageFixedTextAsync(stream, 1);
        string assigned;
        stream.Position = 0;
        await using (AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            using DataTable rows = await reader.ReadTableAsync("FixedText", cancellationToken: TestContext.Current.CancellationToken);
            assigned = Assert.IsType<string>(Assert.Single(rows.Select("Id = 1"))["Value"]);
        }

        byte[] before = stream.ToArray();
        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await Assert.ThrowsAsync<JetDatabaseWriter.Exceptions.JetLimitationException>(async () =>
                await writer.UpdateRowsAsync("FixedText", RowCriteria.Where("Id", 1), new RowValues { ["Value"] = assigned }, TestContext.Current.CancellationToken));
            Assert.Equal(before, stream.ToArray());
            Assert.Equal(1, await writer.UpdateRowsAsync("FixedText", RowCriteria.Where("Id", 1), new RowValues { ["Value"] = "A" }, TestContext.Current.CancellationToken));
        }

        (_, Dictionary<int, byte[]> stored) = await ReadStorageAsync(stream, "FixedText", "Value");
        Assert.Equal("A     "u8.ToArray(), stored[1]);
    }

    [Theory]
    [InlineData("NativeFixedTextJet4.mdb", false, false)]
    [InlineData("NativeFixedTextJet4.mdb", true, false)]
    [InlineData("NativeFixedTextAce.accdb", false, false)]
    [InlineData("NativeFixedTextAce.accdb", true, false)]
    [InlineData("NativeFixedTextJet4.mdb", false, true)]
    [InlineData("NativeFixedTextJet4.mdb", true, true)]
    [InlineData("NativeFixedTextAce.accdb", false, true)]
    [InlineData("NativeFixedTextAce.accdb", true, true)]
    public async Task NativeFixedText_CascadeKeepsUnassignedMalformedSlot(string fileName, bool nullSlot, bool selfReference)
    {
        string path = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Databases", "FixedText", fileName);
        await using var stream = new MemoryStream();
        await stream.WriteAsync(await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken), TestContext.Current.CancellationToken);
        int targetId = nullSlot ? 5 : 1;
        await DamageFixedTextAsync(stream, targetId);
        (ColumnInfo before, Dictionary<int, byte[]> beforeValues) = await ReadStorageAsync(stream, "FixedText", "Value");
        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            string parent = selfReference ? "FixedText" : "Parents";
            if (selfReference)
            {
                Assert.Equal(beforeValues.Count, await writer.UpdateRowsAsync("FixedText", RowCriteria.All(), new RowValues { ["Spare"] = targetId }, TestContext.Current.CancellationToken));
            }
            else
            {
                await writer.CreateTableAsync(parent, [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }], TestContext.Current.CancellationToken);
                foreach (int id in beforeValues.Keys)
                {
                    await writer.InsertRowAsync(parent, [id], TestContext.Current.CancellationToken);
                }
            }

            await writer.CreateRelationshipAsync(new RelationshipDefinition("ParentFixedText", parent, "Id", "FixedText", selfReference ? "Spare" : "Id") { CascadeUpdates = true }, TestContext.Current.CancellationToken);
            Assert.Equal(1, await writer.UpdateRowsAsync(parent, RowCriteria.Where("Id", targetId), new RowValues { ["Id"] = 100 }, TestContext.Current.CancellationToken));
        }

        (ColumnInfo after, Dictionary<int, byte[]> afterValues) = await ReadStorageAsync(stream, "FixedText", "Value");
        Assert.Equal(before.RawDescriptor, after.RawDescriptor);
        Assert.Equal(beforeValues.Count, afterValues.Count);
        Assert.False(afterValues.ContainsKey(targetId));
        foreach ((int id, byte[] value) in beforeValues)
        {
            Assert.Equal(value, afterValues[id == targetId ? 100 : id]);
        }
    }

    private static async Task DamageFixedTextAsync(MemoryStream stream, int targetId)
    {
        stream.Position = 0;
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(stream, cancellationToken: TestContext.Current.CancellationToken);
        CatalogEntry entry = Assert.IsType<CatalogEntry>(await harness.GetCatalogEntryAsync("FixedText", TestContext.Current.CancellationToken));
        TableDef definition = Assert.IsType<TableDef>(await harness.ReadTableDefAsync(entry.TDefPage, TestContext.Current.CancellationToken));
        ColumnInfo value = Assert.Single(definition.Columns, column => column.Name == "Value");
        ColumnInfo id = Assert.Single(definition.Columns, column => column.Name == "Id");
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
                if (JetTypeInfo.Ri32(page, bound.RowStart + idSlice.DataStart) != targetId)
                {
                    continue;
                }

                long end = (number * format.PageSize) + bound.RowStart + format.RowFields.NumCols + value.FixedOff + value.Size;
                stream.Position = end - (format.IsJet3 ? 1 : 2);
                if (format.IsJet3)
                {
                    // A final CP932 lead byte has no trailing byte within the fixed slot.
                    stream.WriteByte(0x81);
                }
                else
                {
                    // An unpaired high surrogate is replaced by the public text decoder.
                    stream.WriteByte(0);
                    stream.WriteByte(0xD8);
                }

                return;
            }
        }

        Assert.Fail("The native fixed Text row to damage was not found.");
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
