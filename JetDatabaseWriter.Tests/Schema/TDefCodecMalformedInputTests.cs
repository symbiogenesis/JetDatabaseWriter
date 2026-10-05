namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using Xunit;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>Pins tolerant column parsing and immutable descriptor ownership on malformed definitions.</summary>
public sealed class TDefCodecMalformedInputTests
{
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public void Parse_TruncatedHeaderOrDescriptors_ReturnsNull(DatabaseFormat kind)
    {
        var format = JetFormat.ForNewDatabase(kind);
        Assert.Null(TDefCodec.Parse(format, null));
        Assert.Null(TDefCodec.Parse(format, new byte[format.TDef.BlockEnd - 1]));
        byte[] td = new byte[format.TDef.BlockEnd];
        Wu16(td, format.TDef.NumCols, 1);
        Assert.Null(TDefCodec.Parse(format, td));
        Wu16(td, format.TDef.NumCols, Constants.TableDefinition.MaxColumns + 1);
        Assert.Null(TDefCodec.Parse(format, td));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, -1)]
    [InlineData(DatabaseFormat.Jet3Mdb, int.MaxValue)]
    [InlineData(DatabaseFormat.Jet4Mdb, -1)]
    [InlineData(DatabaseFormat.Jet4Mdb, int.MaxValue)]
    [InlineData(DatabaseFormat.AceAccdb, -1)]
    [InlineData(DatabaseFormat.AceAccdb, int.MaxValue)]
    public void Parse_InvalidRealIndexCount_ClampsForColumnsAndRetainsDeclaredCount(DatabaseFormat kind, int count)
    {
        var format = JetFormat.ForNewDatabase(kind);
        byte[] td = OneColumn(format);
        Wi32(td, format.TDef.NumRealIdx, count);
        TDefImage? image = TDefCodec.Parse(format, td);
        Assert.NotNull(image);
        Assert.Equal(count, image.Header.RealIndexCount);
        Assert.True(image.Issues.HasFlag(TDefParseIssues.InvalidRealIndexCount));
        var legacy = new LegacyTDefParsers(format);
        Assert.Equal(legacy.Parse(td)!.Columns[0], image.Columns[0] with { RawDescriptor = [] });
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public void Parse_TruncatedName_KeepsColumnAndReportsIssue(DatabaseFormat kind)
    {
        var format = JetFormat.ForNewDatabase(kind);
        byte[] td = OneColumn(format);
        int name = format.TDef.BlockEnd + format.ColumnDescriptor.Size;
        td[name] = 255;
        TDefImage? image = TDefCodec.Parse(format, td);
        Assert.NotNull(image);
        Assert.Equal(string.Empty, Assert.Single(image.Columns).Name);
        Assert.True(image.Issues.HasFlag(TDefParseIssues.TruncatedColumnNames));
        Assert.Null(image.IndexSection);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public void Parse_SourceAndReturnedCopiesCannotMutateImage(DatabaseFormat kind)
    {
        var format = JetFormat.ForNewDatabase(kind);
        byte[] td = OneColumn(format);
        TDefImage? image = TDefCodec.Parse(format, td);
        Assert.NotNull(image);
        byte[] original = image.CopyBytes();
        ColumnInfo column = Assert.Single(image.Columns);
        byte type = column.RawDescriptor[format.ColumnDescriptor.TypeOff];
        td[format.TDef.BlockEnd + format.ColumnDescriptor.TypeOff] = 255;
        byte[] copy = image.CopyBytes();
        copy[format.TDef.BlockEnd] = 255;
        Assert.Equal(original, image.CopyBytes());
        Assert.Equal(type, column.RawDescriptor[format.ColumnDescriptor.TypeOff]);
        Assert.Throws<NotSupportedException>(() => ((IList<byte>)column.RawDescriptor)[0] = 255);
        Assert.Same(column.RawDescriptor, column.WithCalculatedResultType(ColumnType.LongIntegerType).RawDescriptor);
        Assert.Throws<NotSupportedException>(() => ((IList<ColumnInfo>)image.Columns).Clear());
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, -1, 0)]
    [InlineData(DatabaseFormat.Jet3Mdb, int.MaxValue, 0)]
    [InlineData(DatabaseFormat.Jet4Mdb, -1, 0)]
    [InlineData(DatabaseFormat.Jet4Mdb, int.MaxValue, 0)]
    [InlineData(DatabaseFormat.AceAccdb, -1, 0)]
    [InlineData(DatabaseFormat.AceAccdb, int.MaxValue, 0)]
    [InlineData(DatabaseFormat.Jet3Mdb, 1, -1)]
    [InlineData(DatabaseFormat.Jet4Mdb, 1, int.MaxValue)]
    [InlineData(DatabaseFormat.AceAccdb, 1, int.MaxValue)]
    public void ReadMetadata_MalformedIndexCounts_PreservesLegacyEmptyBail(DatabaseFormat kind, int logicalCount, int realCount)
    {
        var format = JetFormat.ForNewDatabase(kind);
        byte[] td = OneColumn(format);
        Wi32(td, format.TDef.NumIdx, logicalCount);
        Wi32(td, format.TDef.NumRealIdx, realCount);
        TDefImage? image = TDefCodec.Parse(format, td);
        Assert.NotNull(image);
        Assert.Equal(logicalCount, image.Header.LogicalIndexCount);
        Assert.Equivalent(LegacyIndexCatalogReader.ReadMetadata(format, td, image.Columns), IndexCatalogReader.ReadMetadata(format, td, image.Columns), strict: true);
        Assert.Empty(IndexCatalogReader.ReadMetadata(format, td, image.Columns));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public void Parse_TruncatedIndexSection_KeepsColumnsAndMetadataBails(DatabaseFormat kind)
    {
        var format = JetFormat.ForNewDatabase(kind);
        byte[] td = OneColumn(format);
        Wi32(td, format.TDef.NumIdx, 1);
        TDefImage? image = TDefCodec.Parse(format, td);
        Assert.NotNull(image);
        Assert.Single(image.Columns);
        Assert.True(image.Issues.HasFlag(TDefParseIssues.TruncatedIndexDescriptors));
        Assert.Null(image.IndexSection);
        Assert.Empty(IndexCatalogReader.ReadMetadata(format, td, image.Columns));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public void ReadMetadata_ShortHeaderWithZeroIndexCount_PreservesLegacyEmptyBail(DatabaseFormat kind)
    {
        var format = JetFormat.ForNewDatabase(kind);
        byte[] td = new byte[format.TDef.NumRealIdx + sizeof(int)];
        Assert.Empty(LegacyIndexCatalogReader.ReadMetadata(format, td, []));
        Assert.Empty(IndexCatalogReader.ReadMetadata(format, td, []));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public void ReadColumns_MalformedIndexData_IsIndependentOfIndexParsing(DatabaseFormat kind)
    {
        var format = JetFormat.ForNewDatabase(kind);
        byte[] td = OneColumn(format);
        Wi32(td, format.TDef.NumIdx, int.MaxValue);
        List<ColumnInfo>? columns = TDefCodec.ReadColumns(format, td, out TDefHeader header, out _, out TDefParseIssues issues, out _);
        Assert.NotNull(columns);
        Assert.Equal(int.MaxValue, header.LogicalIndexCount);
        Assert.Equal(TDefParseIssues.None, issues);
        Assert.Equal(new LegacyTDefParsers(format).Parse(td)!.Columns[0], Assert.Single(columns) with { RawDescriptor = [] });
        td[format.TDef.BlockEnd] = 255;
        Assert.NotEqual((byte)255, columns[0].RawDescriptor[0]);
    }

    private static byte[] OneColumn(JetFormat format)
    {
        byte[] name = format.EncodeTDefNameRecord("C");
        byte[] td = new byte[format.TDef.BlockEnd + format.ColumnDescriptor.Size + name.Length];
        td[0] = Constants.PageTypes.TableDefinition;
        Wu16(td, format.TDef.NumCols, 1);
        td[format.TDef.BlockEnd + format.ColumnDescriptor.TypeOff] = (byte)ColumnType.LongIntegerType;
        name.CopyTo(td, format.TDef.BlockEnd + format.ColumnDescriptor.Size);
        return td;
    }
}
