namespace JetDatabaseWriter.Tests.Internal;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Core;
using JetDatabaseWriter.Internal;
using JetDatabaseWriter.Internal.Builders;
using JetDatabaseWriter.Internal.Helpers;
using JetDatabaseWriter.Internal.Models;
using JetDatabaseWriter.Models;
using Xunit;

#pragma warning disable SA1202 // Tests intentionally interleave public test methods with private fixture helpers.

public sealed class PersistedColumnPropertiesTests
{
    // ── JetExpressionConverter.ToJetExpression ─────────────────────────

    [Fact]
    public void ToJetExpression_Null_And_DBNull_Return_Null()
    {
        Assert.Null(JetExpressionConverter.ToJetExpression(null));
        Assert.Null(JetExpressionConverter.ToJetExpression(DBNull.Value));
    }

    [Theory]
    [InlineData(0, "0")]
    [InlineData(42, "42")]
    [InlineData(-1, "-1")]
    [InlineData((short)7, "7")]
    [InlineData((byte)5, "5")]
    [InlineData(9999999999, "9999999999")]
    public void ToJetExpression_Integers_RoundTripInvariant(object value, string expected)
    {
        Assert.Equal(expected, JetExpressionConverter.ToJetExpression(value));
    }

    [Theory]
    [InlineData(true, "True")]
    [InlineData(false, "False")]
    public void ToJetExpression_Booleans(bool value, string expected)
    {
        Assert.Equal(expected, JetExpressionConverter.ToJetExpression(value));
    }

    [Fact]
    public void ToJetExpression_Strings_QuoteAndEscape()
    {
        Assert.Equal("\"hi\"", JetExpressionConverter.ToJetExpression("hi"));
        Assert.Equal("\"a \"\"quoted\"\" b\"", JetExpressionConverter.ToJetExpression("a \"quoted\" b"));
    }

    [Fact]
    public void ToJetExpression_DateTime_HashWrappedInvariant()
    {
        var dt = new DateTime(2026, 4, 24, 13, 5, 30);
        Assert.Equal("#2026-04-24 13:05:30#", JetExpressionConverter.ToJetExpression(dt));
    }

    [Fact]
    public void ToJetExpression_Guid_BraceWrapped()
    {
        var g = new Guid("12345678-1234-1234-1234-1234567890AB");
        Assert.Equal("{guid 12345678-1234-1234-1234-1234567890ab}", JetExpressionConverter.ToJetExpression(g));
    }

    [Fact]
    public void ToJetExpression_ByteArray_Throws()
    {
        Assert.Throws<NotSupportedException>(() => JetExpressionConverter.ToJetExpression(new byte[] { 1, 2, 3 }));
    }

    // ── ColumnPropertyBlockBuilder round-trip via parser ──────────────

    [Fact]
    public void Builder_RoundTrips_Via_Parser_SingleColumn()
    {
        var b = new ColumnPropertyBlockBuilder();
        var t = b.GetOrAddTarget("Qty");
        t.AddText(ColumnPropertyNames.DefaultValue, "0", DatabaseFormat.Jet4Mdb);
        t.AddText(ColumnPropertyNames.ValidationRule, ">=0", DatabaseFormat.Jet4Mdb);
        t.AddText(ColumnPropertyNames.ValidationText, "must be non-negative", DatabaseFormat.Jet4Mdb);
        t.AddText(ColumnPropertyNames.Description, "Quantity ordered", DatabaseFormat.Jet4Mdb);

        byte[]? blob = b.ToBytes(DatabaseFormat.Jet4Mdb);

        ColumnPropertyBlock parsed = ColumnPropertyBlock.Parse(blob, DatabaseFormat.Jet4Mdb)!;
        ColumnPropertyTarget tgt = parsed.FindTarget("Qty")!;
        Assert.Equal("0", tgt.GetTextValue(ColumnPropertyNames.DefaultValue, DatabaseFormat.Jet4Mdb));
        Assert.Equal(">=0", tgt.GetTextValue(ColumnPropertyNames.ValidationRule, DatabaseFormat.Jet4Mdb));
        Assert.Equal("must be non-negative", tgt.GetTextValue(ColumnPropertyNames.ValidationText, DatabaseFormat.Jet4Mdb));
        Assert.Equal("Quantity ordered", tgt.GetTextValue(ColumnPropertyNames.Description, DatabaseFormat.Jet4Mdb));
    }

    [Fact]
    public void Builder_Empty_Returns_Null_Blob()
    {
        Assert.Null(new ColumnPropertyBlockBuilder().ToBytes(DatabaseFormat.Jet4Mdb));
    }

    [Fact]
    public void Builder_FromBlock_PreservesUnknownChunks()
    {
        var srcBuilder = new ColumnPropertyBlockBuilder();
        srcBuilder.GetOrAddTarget("A").AddText(ColumnPropertyNames.Description, "alpha", DatabaseFormat.Jet4Mdb);
        srcBuilder.UnknownChunks.Add(new ColumnPropertyUnknownChunk(0xABCD, [0xDE, 0xAD, 0xBE, 0xEF]));

        byte[] blob = srcBuilder.ToBytes(DatabaseFormat.Jet4Mdb)!;
        ColumnPropertyBlock parsed = ColumnPropertyBlock.Parse(blob, DatabaseFormat.Jet4Mdb)!;

        ColumnPropertyBlockBuilder copy = ColumnPropertyBlockBuilder.FromBlock(parsed);
        byte[] reblob = copy.ToBytes(DatabaseFormat.Jet4Mdb)!;
        ColumnPropertyBlock reparsed = ColumnPropertyBlock.Parse(reblob, DatabaseFormat.Jet4Mdb)!;

        Assert.Single(reparsed.UnknownChunks);
        Assert.Equal((ushort)0xABCD, reparsed.UnknownChunks[0].ChunkType);
        Assert.Equal(new byte[] { 0xDE, 0xAD, 0xBE, 0xEF }, reparsed.UnknownChunks[0].Payload);
        Assert.Equal("alpha", reparsed.FindTarget("A")!.GetTextValue(ColumnPropertyNames.Description, DatabaseFormat.Jet4Mdb));
    }

    [Fact]
    public void BuildLvPropBlob_NoPersistedProps_Returns_Null()
    {
        var cols = new List<ColumnDefinition>
        {
            new("X", typeof(int)),
            new("Y", typeof(string), maxLength: 50),
        };

        Assert.Null(JetExpressionConverter.BuildLvPropBlob(cols, DatabaseFormat.Jet4Mdb));
    }

    [Fact]
    public void BuildLvPropBlob_DefaultValueClr_ConvertedToExpression()
    {
        var cols = new List<ColumnDefinition>
        {
            new("X", typeof(int)) { DefaultValue = 42 },
        };

        byte[] blob = JetExpressionConverter.BuildLvPropBlob(cols, DatabaseFormat.Jet4Mdb)!;
        ColumnPropertyBlock parsed = ColumnPropertyBlock.Parse(blob, DatabaseFormat.Jet4Mdb)!;
        Assert.Equal("42", parsed.FindTarget("X")!.GetTextValue(ColumnPropertyNames.DefaultValue, DatabaseFormat.Jet4Mdb));
    }

    [Fact]
    public void BuildLvPropBlob_ExpressionWinsOver_ClrDefault()
    {
        var cols = new List<ColumnDefinition>
        {
            new("X", typeof(int))
            {
                DefaultValue = 1,
                DefaultValueExpression = "=Now()",
            },
        };

        byte[] blob = JetExpressionConverter.BuildLvPropBlob(cols, DatabaseFormat.Jet4Mdb)!;
        ColumnPropertyBlock parsed = ColumnPropertyBlock.Parse(blob, DatabaseFormat.Jet4Mdb)!;
        Assert.Equal("=Now()", parsed.FindTarget("X")!.GetTextValue(ColumnPropertyNames.DefaultValue, DatabaseFormat.Jet4Mdb));
    }

    // ── End-to-end: CreateTableAsync persists; AccessReader reads back ─

    // Sample fixture sized to fit within the 256-byte inline OLE limit (write-side
    // LVAL chain support is not yet implemented). With UTF-16LE encoding the four
    // long strings on one column plus a description on a second column total ~190
    // bytes for the LvProp blob.
    private static List<ColumnDefinition> SampleColumnsWithProperties() => new()
    {
        new("Qty", typeof(int))
        {
            DefaultValueExpression = "0",
            ValidationRuleExpression = ">=0",
            ValidationText = "too low",
            Description = "qty",
        },
        new("N", typeof(string), maxLength: 50)
        {
            Description = "name",
        },
    };

    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CreateTableAsync_PersistsLvProp_Visible_Via_GetColumnMetadataAsync(DatabaseFormat format)
    {
        var ms = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            ms, format, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("Items", SampleColumnsWithProperties(), TestContext.Current.CancellationToken);
        }

        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(
            ms,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        List<ColumnMetadata> meta = await reader.GetColumnMetadataAsync("Items", TestContext.Current.CancellationToken);

        ColumnMetadata qty = meta.Single(m => m.Name == "Qty");
        Assert.Equal("0", qty.DefaultValueExpression);
        Assert.Equal(">=0", qty.ValidationRuleExpression);
        Assert.Equal("too low", qty.ValidationText);
        Assert.Equal("qty", qty.Description);

        ColumnMetadata name = meta.Single(m => m.Name == "N");
        Assert.Null(name.DefaultValueExpression);
        Assert.Equal("name", name.Description);
    }

    // ──  round-trip preservation through schema mutations ──────

    [Fact]
    public async Task AddColumnAsync_PreservesExistingLvProp()
    {
        var ms = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            ms, DatabaseFormat.AceAccdb, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("T", SampleColumnsWithProperties(), TestContext.Current.CancellationToken);
            await writer.AddColumnAsync("T", new ColumnDefinition("New", typeof(int)), TestContext.Current.CancellationToken);
        }

        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(
            ms,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        List<ColumnMetadata> meta = await reader.GetColumnMetadataAsync("T", TestContext.Current.CancellationToken);
        Assert.Equal(3, meta.Count);

        ColumnMetadata qty = meta.Single(m => m.Name == "Qty");
        Assert.Equal("0", qty.DefaultValueExpression);
        Assert.Equal("too low", qty.ValidationText);
        Assert.Equal("qty", qty.Description);

        Assert.Null(meta.Single(m => m.Name == "New").DefaultValueExpression);
    }

    [Fact]
    public async Task DropColumnAsync_RemovesDroppedColumnLvProp_PreservesOthers()
    {
        var ms = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            ms, DatabaseFormat.AceAccdb, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("T", SampleColumnsWithProperties(), TestContext.Current.CancellationToken);
            await writer.DropColumnAsync("T", "N", TestContext.Current.CancellationToken);
        }

        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(
            ms,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        List<ColumnMetadata> meta = await reader.GetColumnMetadataAsync("T", TestContext.Current.CancellationToken);

        Assert.Single(meta);
        Assert.Equal("Qty", meta[0].Name);
        Assert.Equal("0", meta[0].DefaultValueExpression);
        Assert.Equal("qty", meta[0].Description);
    }

    [Fact]
    public async Task RenameColumnAsync_RetargetsLvProp_To_NewColumnName()
    {
        var ms = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            ms, DatabaseFormat.AceAccdb, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("T", SampleColumnsWithProperties(), TestContext.Current.CancellationToken);
            await writer.RenameColumnAsync("T", "Qty", "Quantity", TestContext.Current.CancellationToken);
        }

        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(
            ms,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        List<ColumnMetadata> meta = await reader.GetColumnMetadataAsync("T", TestContext.Current.CancellationToken);

        ColumnMetadata renamed = meta.Single(m => m.Name == "Quantity");
        Assert.Equal("0", renamed.DefaultValueExpression);
        Assert.Equal(">=0", renamed.ValidationRuleExpression);
        Assert.Equal("too low", renamed.ValidationText);
        Assert.Equal("qty", renamed.Description);

        // Old target name should no longer be present.
        Assert.DoesNotContain(meta, m => m.Name == "Qty");
    }

    [Fact]
    public async Task CreateTableAsync_NoPersistedProps_DoesNotEmitLvProp()
    {
        var ms = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            ms, DatabaseFormat.AceAccdb, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "Plain",
                new List<ColumnDefinition> { new("X", typeof(int)) },
                TestContext.Current.CancellationToken);
        }

        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(
            ms,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        ColumnMetadata col = (await reader.GetColumnMetadataAsync("Plain", TestContext.Current.CancellationToken)).Single();
        Assert.Null(col.DefaultValueExpression);
        Assert.Null(col.ValidationRuleExpression);
        Assert.Null(col.ValidationText);
        Assert.Null(col.Description);
    }
}
