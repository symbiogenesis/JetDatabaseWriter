namespace JetDatabaseWriter.Tests.Indexes.Collation;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Public collation choices select stored keys and survive schema edits.</summary>
public sealed class PublicTextCollationTests
{
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, TextCollation.General97)]
    [InlineData(DatabaseFormat.Jet4Mdb, TextCollation.GeneralLegacy)]
    [InlineData(DatabaseFormat.Jet4Mdb, TextCollation.General)]
    [InlineData(DatabaseFormat.AceAccdb, TextCollation.GeneralLegacy)]
    [InlineData(DatabaseFormat.AceAccdb, TextCollation.General)]
    public async Task ExplicitChoice_PreservesMetadataUniquenessAndSeeksThroughSchemaEdits(DatabaseFormat format, TextCollation collation)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, WriteModes.WriterOptions(WriteMode.Direct), leaveOpen: true, ct))
        {
            await writer.CreateTableAsync("Collated", [new ColumnDefinition("Code", typeof(string), 40) { IsPrimaryKey = true, TextCollation = collation }, new ColumnDefinition("Discard", typeof(int))], ct);
            await writer.InsertRowAsync("Collated", ["caf\u00e9", 9], ct);
            byte[] before = stream.ToArray();
            await Assert.ThrowsAsync<JetConstraintException>(async () => await writer.InsertRowAsync("Collated", ["CAF\u00c9", 10], ct));
            Assert.Equal(before, stream.ToArray());
            await writer.AddColumnAsync("Collated", new ColumnDefinition("Note", typeof(string)) { TextCollation = collation }, ct);
            Assert.Equal(1, await writer.UpdateRowsAsync("Collated", "Code", "caf\u00e9", new Dictionary<string, object?> { ["Note"] = "r\u00e9sum\u00e9" }, ct));
            await writer.DropColumnAsync("Collated", "Discard", ct);
            await writer.RenameColumnAsync("Collated", "Code", "Renamed", ct);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, ct);
        foreach (ColumnMetadata column in await reader.GetColumnMetadataAsync("Collated", ct))
        {
            Assert.Equal(collation, column.TextCollation);
            Assert.Equal((ushort)0x0409, column.TextSortOrderId);
            byte? version = collation switch
            {
                TextCollation.General97 => null,
                TextCollation.GeneralLegacy => 0,
                TextCollation.General => 1,
                _ => 1,
            };
            Assert.Equal(version, column.TextSortOrderVersion);
        }

        IndexMetadata index = Assert.Single(await reader.ListIndexesAsync("Collated", ct));
        var rows = new List<object[]>();
        await foreach (object[] row in reader.SeekRowsAsync("Collated", index.Name, ["CAF\u00c9"], ct))
        {
            rows.Add(row);
        }

        object[] found = Assert.Single(rows);
        Assert.Equal("caf\u00e9", found[0]);
        Assert.Equal("r\u00e9sum\u00e9", found[1]);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, TextCollation.General97)]
    [InlineData(DatabaseFormat.Jet4Mdb, TextCollation.GeneralLegacy)]
    [InlineData(DatabaseFormat.AceAccdb, TextCollation.GeneralLegacy)]
    public async Task NullChoice_UsesDatabaseDefault(DatabaseFormat format, TextCollation expected)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var definition = new ColumnDefinition("Text", typeof(string), 40);
        Assert.Null(definition.TextCollation);
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, WriteModes.WriterOptions(WriteMode.Direct), leaveOpen: true, ct))
        {
            await writer.CreateTableAsync("Defaults", [definition], ct);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, ct);
        Assert.Equal(expected, Assert.Single(await reader.GetColumnMetadataAsync("Defaults", ct)).TextCollation);
    }

    [Theory]
    [InlineData(TextCollation.GeneralLegacy)]
    [InlineData(TextCollation.General)]
    public async Task CalculatedTextChoice_UsesDeclaredTextResult(TextCollation collation)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, WriteModes.WriterOptions(WriteMode.Direct), leaveOpen: true, ct))
        {
            await writer.CreateTableAsync("Calculated", [new ColumnDefinition("Text", typeof(string), 40), new ColumnDefinition("Computed", typeof(string), 80) { IsCalculated = true, CalculationExpression = "[Text] & \"!\"", TextCollation = collation }], ct);
            await writer.InsertRowAsync("Calculated", new RowValues { ["Text"] = "caf\u00e9" }, ct);
            byte[] before = stream.ToArray();
            await Assert.ThrowsAsync<ArgumentException>(async () => await writer.AddColumnAsync("Calculated", new ColumnDefinition("Number", typeof(int)) { IsCalculated = true, CalculationExpression = "1", TextCollation = collation }, ct));
            Assert.Equal(before, stream.ToArray());
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, ct);
        ColumnMetadata computed = Assert.Single(await reader.GetColumnMetadataAsync("Calculated", ct), column => column.Name == "Computed");
        Assert.True(computed.IsCalculated);
        Assert.Equal(collation, computed.TextCollation);
        var rows = new List<object[]>();
        await foreach (object[] row in reader.Rows("Calculated", cancellationToken: ct))
        {
            rows.Add(row);
        }

        Assert.Equal("caf\u00e9!", Assert.Single(rows)[1]);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, TextCollation.GeneralLegacy)]
    [InlineData(DatabaseFormat.Jet3Mdb, TextCollation.General)]
    [InlineData(DatabaseFormat.Jet4Mdb, TextCollation.General97)]
    [InlineData(DatabaseFormat.AceAccdb, TextCollation.General97)]
    [InlineData(DatabaseFormat.Jet4Mdb, (TextCollation)99)]
    public async Task InvalidChoice_CreateAndAddRefuseBeforeMutation(DatabaseFormat format, TextCollation collation)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, WriteModes.WriterOptions(WriteMode.Direct), leaveOpen: true, ct);
        await writer.CreateTableAsync("Existing", [new ColumnDefinition("Id", typeof(int))], ct);
        byte[] before = stream.ToArray();
        await Assert.ThrowsAnyAsync<ArgumentException>(async () => await writer.CreateTableAsync("Refused", [new ColumnDefinition("Text", typeof(string), 40) { TextCollation = collation }], ct));
        Assert.Equal(before, stream.ToArray());
        await Assert.ThrowsAnyAsync<ArgumentException>(async () => await writer.AddColumnAsync("Existing", new ColumnDefinition("Text", typeof(string), 40) { TextCollation = collation }, ct));
        Assert.Equal(before, stream.ToArray());
    }

    [Fact]
    public async Task NonTextChoice_RefusesBeforeMutation()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.Jet4Mdb, WriteModes.WriterOptions(WriteMode.Direct), leaveOpen: true, ct);
        byte[] before = stream.ToArray();
        await Assert.ThrowsAsync<ArgumentException>(async () => await writer.CreateTableAsync("Refused", [new ColumnDefinition("Id", typeof(int)) { TextCollation = TextCollation.GeneralLegacy }], ct));
        Assert.Equal(before, stream.ToArray());
    }

    [Fact]
    public async Task UnknownStoredCollation_ReportsRawIdWithoutSupportedChoice()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.Jet4Mdb, WriteModes.WriterOptions(WriteMode.Direct), leaveOpen: true, ct))
        {
            await writer.CreateTableAsync("Unknown", [new ColumnDefinition("Text", typeof(string), 40), new ColumnDefinition("Id", typeof(int))], ct);
        }

        await CollationTestSupport.PatchSortOrderAsync(stream, "Unknown", "Text", 0x041D, ct);
        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, ct);
        IReadOnlyList<ColumnMetadata> columns = await reader.GetColumnMetadataAsync("Unknown", ct);
        ColumnMetadata text = Assert.Single(columns, column => column.Name == "Text");
        Assert.Null(text.TextCollation);
        Assert.Equal((ushort)0x041D, text.TextSortOrderId);
        Assert.Equal((byte)0, text.TextSortOrderVersion);
        ColumnMetadata scalar = Assert.Single(columns, column => column.Name == "Id");
        Assert.Null(scalar.TextCollation);
        Assert.Null(scalar.TextSortOrderId);
        Assert.Null(scalar.TextSortOrderVersion);
    }
}
