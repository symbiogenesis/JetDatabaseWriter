namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Unit tests for <see cref="PersistedPropertyProjector"/>, which carries a
/// table's persisted properties through a schema rewrite, and for the
/// table-level target helpers of <see cref="ColumnPropertyBlock"/> and
/// <see cref="ColumnPropertyBlockBuilder"/>.
/// </summary>
public sealed class PersistedPropertyProjectorTests
{
    private static readonly JetFormat Format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);

    private static readonly ColumnDefinition IdColumn = new("Id", typeof(int));

    private static readonly ColumnDefinition NameColumn = new("Name", typeof(string), maxLength: 20);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public void ProjectForRewrite_RenamedColumn_MovesTargetAndKeepsEntryBytes()
    {
        ColumnPropertyBlock original = AccessShapedBlock();

        ColumnPropertyBlock projected = PersistedPropertyProjector.ProjectForRewrite(
            original,
            [IdColumn, NameColumn],
            [IdColumn, NameColumn.WithName("Title")],
            name => string.Equals(name, "Name", StringComparison.OrdinalIgnoreCase) ? "Title" : name,
            Format);

        Assert.Equal(["Id", string.Empty, "Title"], projected.Targets.Select(t => t.Name));
        Assert.Equal(Lines(original.FindTarget("Name")!), Lines(projected.FindTarget("Title")!));
        Assert.Equal(Lines(original.FindTarget("Id")!), Lines(projected.FindTarget("Id")!));
        Assert.Equal(ColumnPropertyChunkType.PropertyBlockAlt1, projected.FindTarget("Title")!.ChunkType);
    }

    [Fact]
    public void ProjectForRewrite_DroppedColumn_RemovesOnlyItsTarget()
    {
        ColumnPropertyBlock original = AccessShapedBlock();

        ColumnPropertyBlock projected = PersistedPropertyProjector.ProjectForRewrite(
            original,
            [IdColumn, NameColumn],
            [IdColumn],
            name => string.Equals(name, "Name", StringComparison.OrdinalIgnoreCase) ? null : name,
            Format);

        Assert.Equal(["Id", string.Empty], projected.Targets.Select(t => t.Name));
        Assert.Equal(Lines(original.FindTarget("Id")!), Lines(projected.FindTarget("Id")!));
    }

    [Fact]
    public void ProjectForRewrite_AddedColumn_GetsTheWriterProperties()
    {
        ColumnDefinition added = new("Note", typeof(string), maxLength: 30) { Description = "a note" };

        ColumnPropertyBlock projected = PersistedPropertyProjector.ProjectForRewrite(
            AccessShapedBlock(),
            [IdColumn, NameColumn],
            [IdColumn, NameColumn, added],
            name => name,
            Format);

        Assert.Equal(["Id", string.Empty, "Name", "Note"], projected.Targets.Select(t => t.Name));
        ColumnPropertyTarget note = projected.FindTarget("Note")!;
        Assert.Equal("a note", note.GetTextValue(Constants.ColumnPropertyNames.Description, Format));
        Assert.False(note.GetBooleanValue(Constants.ColumnPropertyNames.AllowZeroLength));
        Assert.False(note.GetBooleanValue(Constants.ColumnPropertyNames.Required));
    }

    [Fact]
    public void ProjectForRewrite_TableTarget_KeptInPlaceWithoutNameMap()
    {
        ColumnPropertyBlock original = AccessShapedBlock();

        ColumnPropertyBlock projected = PersistedPropertyProjector.ProjectForRewrite(original, [IdColumn, NameColumn], [IdColumn, NameColumn], name => name, Format);

        ColumnPropertyTarget table = Assert.IsType<ColumnPropertyTarget>(projected.FindTableTarget());
        Assert.Equal(1, projected.Targets.ToList().IndexOf(table));
        Assert.Equal(ColumnPropertyChunkType.PropertyBlock, table.ChunkType);
        Assert.Equal([.. Lines(original.FindTableTarget()!).Where(l => !l.StartsWith("NameMap|", StringComparison.Ordinal))], Lines(table));
        Assert.Null(table.Find("NameMap"));
    }

    [Fact]
    public void ProjectForRewrite_OrphanTarget_IsKeptUnlessAColumnTakesItsName()
    {
        var builder = ColumnPropertyBlockBuilder.FromBlock(AccessShapedBlock());
        AddEntry(builder.GetOrAddTarget("Gone"), "Caption", ColumnType.MemoType, Encoding.Unicode.GetBytes("stale"));
        AddEntry(builder.GetOrAddTarget("Kept"), "Caption", ColumnType.MemoType, Encoding.Unicode.GetBytes("orphan"));
        ColumnPropertyBlock original = Parse(builder);

        ColumnPropertyBlock projected = PersistedPropertyProjector.ProjectForRewrite(
            original,
            [IdColumn, NameColumn],
            [IdColumn, NameColumn, new ColumnDefinition("Gone", typeof(int)) { Description = "new column" }],
            name => name,
            Format);

        Assert.Equal(Lines(original.FindTarget("Kept")!), Lines(projected.FindTarget("Kept")!));
        ColumnPropertyTarget gone = Assert.Single(projected.Targets, t => t.Name == "Gone");
        Assert.Null(gone.Find("Caption"));
        Assert.Equal("new column", gone.GetTextValue(Constants.ColumnPropertyNames.Description, Format));
    }

    [Fact]
    public void ProjectForRewrite_RenameOntoOrphanTargetName_ReplacesTheOrphan()
    {
        var builder = ColumnPropertyBlockBuilder.FromBlock(AccessShapedBlock());
        AddEntry(builder.GetOrAddTarget("Title"), "Caption", ColumnType.MemoType, Encoding.Unicode.GetBytes("stale"));
        ColumnPropertyBlock original = Parse(builder);

        ColumnPropertyBlock projected = PersistedPropertyProjector.ProjectForRewrite(
            original,
            [IdColumn, NameColumn],
            [IdColumn, NameColumn.WithName("Title")],
            name => string.Equals(name, "Name", StringComparison.OrdinalIgnoreCase) ? "Title" : name,
            Format);

        ColumnPropertyTarget title = Assert.Single(projected.Targets, t => t.Name == "Title");
        Assert.Equal(Lines(original.FindTarget("Name")!), Lines(title));
    }

    [Fact]
    public void ProjectForRewrite_ChangedModelledProperty_ReplacesOnlyThatEntryInPlace()
    {
        ColumnDefinition before = new("Qty", typeof(int)) { ValidationRuleExpression = "[Price]>0", ValidationText = "positive" };
        ColumnDefinition after = before with { ValidationRuleExpression = "[Cost]>0" };
        var builder = new ColumnPropertyBlockBuilder();
        ColumnPropertyTargetBuilder qty = builder.GetOrAddTarget("Qty");
        AddEntry(qty, "ColumnWidth", ColumnType.IntegerType, [0x10, 0x0E]);
        qty.Entries.Add(new ColumnPropertyEntryBuilder { Name = "ValidationRule", DataType = ColumnType.MemoType, DdlFlag = 1, Value = Encoding.Unicode.GetBytes("[Price]>0") });
        qty.Entries.Add(new ColumnPropertyEntryBuilder { Name = "ValidationText", DataType = ColumnType.TextType, DdlFlag = 1, Value = Encoding.Unicode.GetBytes("positive") });
        ColumnPropertyBlock original = Parse(builder);

        ColumnPropertyBlock projected = PersistedPropertyProjector.ProjectForRewrite(original, [before], [after], name => name, Format);

        Assert.Equal(
            [
                "ColumnWidth|3|0|100E",
                $"ValidationRule|12|1|{Convert.ToHexString(Encoding.Unicode.GetBytes("[Cost]>0"))}",
                $"ValidationText|10|1|{Convert.ToHexString(Encoding.Unicode.GetBytes("positive"))}",
            ],
            Lines(projected.FindTarget("Qty")!));
    }

    [Fact]
    public void ProjectForRewrite_UnknownChunks_AreKept()
    {
        var builder = ColumnPropertyBlockBuilder.FromBlock(AccessShapedBlock());
        builder.UnknownChunks.Add(new ColumnPropertyUnknownChunk(0xABCD, [0xDE, 0xAD, 0xBE, 0xEF]));

        ColumnPropertyBlock projected = PersistedPropertyProjector.ProjectForRewrite(Parse(builder), [IdColumn, NameColumn], [IdColumn], name => name == "Name" ? null : name, Format);

        ColumnPropertyUnknownChunk chunk = Assert.Single(projected.UnknownChunks);
        Assert.Equal((ushort)0xABCD, chunk.ChunkType);
        Assert.Equal([0xDE, 0xAD, 0xBE, 0xEF], chunk.Payload);
    }

    [Fact]
    public void ProjectForRewrite_NoStoredProperties_GivesOnlyTheAddedColumnsProperties()
    {
        ColumnPropertyBlock projected = PersistedPropertyProjector.ProjectForRewrite(
            null,
            [IdColumn, NameColumn],
            [IdColumn, NameColumn, new ColumnDefinition("Note", typeof(string), maxLength: 10)],
            name => name,
            Format);

        Assert.Equal(["Note"], projected.Targets.Select(t => t.Name));
    }

    [Fact]
    public void ProjectForRewrite_NothingLeft_ReturnsAnEmptyBlockThatWritesNoBlob()
    {
        var builder = new ColumnPropertyBlockBuilder();
        AddEntry(builder.GetOrAddTarget("Name"), "Caption", ColumnType.MemoType, Encoding.Unicode.GetBytes("c"));

        ColumnPropertyBlock projected = PersistedPropertyProjector.ProjectForRewrite(Parse(builder), [IdColumn, NameColumn], [IdColumn], name => name == "Name" ? null : name, Format);

        Assert.Empty(projected.Targets);
        Assert.Null(projected.ToBytes(Format));
    }

    [Fact]
    public void FindTableTarget_ReturnsTheEmptyNamedTarget_NeverOneNamedAfterTheTable()
    {
        var builder = new ColumnPropertyBlockBuilder();
        AddEntry(builder.GetOrAddTarget("Orders"), "Description", ColumnType.TextType, Encoding.Unicode.GetBytes("a column named Orders"));
        ColumnPropertyBlock withoutTableTarget = Parse(builder);
        Assert.Null(withoutTableTarget.FindTableTarget());

        AddEntry(builder.GetOrAddTableTarget(), "Description", ColumnType.TextType, Encoding.Unicode.GetBytes("the table"));
        ColumnPropertyTarget table = Assert.IsType<ColumnPropertyTarget>(Parse(builder).FindTableTarget());
        Assert.Equal("the table", table.GetTextValue(Constants.ColumnPropertyNames.Description, Format));
    }

    [Fact]
    public void GetOrAddTableTarget_AddsAnEmptyNamedPropertyBlockFirst_AndReturnsItAfterwards()
    {
        var builder = new ColumnPropertyBlockBuilder();
        _ = builder.GetOrAddTarget("A");

        ColumnPropertyTargetBuilder table = builder.GetOrAddTableTarget();

        Assert.Same(table, builder.Targets[0]);
        Assert.Equal(string.Empty, table.Name);
        Assert.Equal(ColumnPropertyChunkType.PropertyBlock, table.ChunkType);
        Assert.Same(table, builder.GetOrAddTableTarget());
        Assert.Equal(2, builder.Targets.Count);
    }

    /// <summary>
    /// Access writes the table-level target as an empty-named property block of
    /// chunk type 0x00 wherever it puts it: 12th of 12 in NorthwindTraders'
    /// OrderDetails, first in Jackcess's testV1997 Table1, but last of four in
    /// the Jet4 compIndexTestV2000 Table1 and third of 18 in calcFieldTestV2010's
    /// Table1. No position can be assumed.
    /// </summary>
    /// <param name="fixture">The fixture path.</param>
    /// <param name="table">The table.</param>
    /// <param name="index">The table-level target's position.</param>
    /// <param name="property">A table-level property it holds.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [InlineData("NorthwindTraders", "OrderDetails", 11, "SubdatasheetName")]
    [InlineData("TestV1997", "Table1", 0, "Orientation")]
    [InlineData("CompIndexTestV2000", "Table1", 3, "Orientation")]
    [InlineData("CalcFieldTestV2010", "Table1", 2, "Orientation")]
    public async Task FindTableTarget_AccessAuthored_ReturnsEmptyNamedBlockWhereverItIs(string fixture, string table, int index, string property)
    {
        string path = fixture switch
        {
            "NorthwindTraders" => TestDatabases.NorthwindTraders,
            "TestV1997" => TestDatabases.TestV1997,
            "CompIndexTestV2000" => TestDatabases.CompIndexTestV2000,
            _ => TestDatabases.CalcFieldTestV2010,
        };
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(path, cancellationToken: Ct);
        CatalogEntry entry = Assert.IsType<CatalogEntry>(await harness.GetCatalogEntryAsync(table, Ct));
        ColumnPropertyBlock block = Assert.IsType<ColumnPropertyBlock>(await harness.Services.Catalog.ReadLvPropForTableAsync(entry.TDefPage, Ct));

        ColumnPropertyTarget target = Assert.IsType<ColumnPropertyTarget>(block.FindTableTarget());

        Assert.Equal(index, block.Targets.ToList().IndexOf(target));
        Assert.Equal(string.Empty, target.Name);
        Assert.Equal(ColumnPropertyChunkType.PropertyBlock, target.ChunkType);
        Assert.NotNull(target.Find(property));
        Assert.Single(block.Targets, t => t.Name.Length == 0);
    }

    /// <summary>
    /// A blob in the shape Access writes: column targets of chunk type 0x01 with
    /// properties the writer does not model, and the table-level target in the
    /// middle with a Description, a GUID and a NameMap.
    /// </summary>
    private static ColumnPropertyBlock AccessShapedBlock()
    {
        var builder = new ColumnPropertyBlockBuilder();
        ColumnPropertyTargetBuilder id = builder.GetOrAddTarget("Id");
        AddEntry(id, "ColumnWidth", ColumnType.IntegerType, [0x10, 0x0E]);
        AddEntry(id, "GUID", ColumnType.BinaryType, Guid.Parse("00112233-4455-6677-8899-aabbccddeeff").ToByteArray());

        var table = new ColumnPropertyTargetBuilder { Name = string.Empty, ChunkType = ColumnPropertyChunkType.PropertyBlock };
        AddEntry(table, "Description", ColumnType.TextType, Encoding.Unicode.GetBytes("the table"));
        AddEntry(table, "NameMap", ColumnType.OleType, [1, 2, 3, 4]);
        AddEntry(table, "Filter", ColumnType.MemoType, Encoding.Unicode.GetBytes("[Name] Like \"a*\""));
        builder.Targets.Add(table);

        ColumnPropertyTargetBuilder name = builder.GetOrAddTarget("Name");
        AddEntry(name, "Caption", ColumnType.MemoType, Encoding.Unicode.GetBytes("Full name"));
        name.Entries.Add(new ColumnPropertyEntryBuilder { Name = "AllowZeroLength", DataType = ColumnType.BooleanType, DdlFlag = 1, Value = [0xFF] });
        name.Entries.Add(new ColumnPropertyEntryBuilder { Name = "Required", DataType = ColumnType.BooleanType, DdlFlag = 0, Value = [0x00] });
        return Parse(builder);
    }

    private static void AddEntry(ColumnPropertyTargetBuilder target, string name, ColumnType type, byte[] value)
        => target.Entries.Add(new ColumnPropertyEntryBuilder { Name = name, DataType = type, DdlFlag = 0, Value = value });

    private static ColumnPropertyBlock Parse(ColumnPropertyBlockBuilder builder)
        => Assert.IsType<ColumnPropertyBlock>(ColumnPropertyBlock.Parse(builder.ToBytes(Format), Format));

    private static List<string> Lines(ColumnPropertyTarget target)
        => [.. target.Entries.Select(e => $"{e.Name}|{(int)e.DataType}|{e.DdlFlag}|{Convert.ToHexString(e.Value)}")];
}
