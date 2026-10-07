namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using Xunit;

/// <summary>Pins independent schema projections and calculated-column metadata.</summary>
public sealed class TableSchemaResolutionTests
{
    [Fact]
    public void Definition_OwnsColumnsAndReadOnlyMetadata()
    {
        ColumnInfo[] source = [new ColumnInfo { Name = "Id", Type = ColumnType.LongIntegerType, Flags = 1 }];
        var definition = new TableDef { Columns = source };
        source[0] = new ColumnInfo { Name = "Replaced", Type = ColumnType.MemoType };
        Assert.Equal("Id", Assert.Single(definition.Columns).Name);
        Assert.Equal(typeof(int), Assert.Single(definition.ClrTypes));
        Assert.False(definition.HasVarColumns);
        Assert.Throws<NotSupportedException>(() => ((IList<Type>)definition.ClrTypes)[0] = typeof(string));
        Assert.Throws<NotSupportedException>(() => ((IList<ColumnInfo>)definition.Columns)[0] = source[0]);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public void DefinitionProjection_DoesNotChangeTheImage(DatabaseFormat kind)
    {
        var format = JetFormat.ForNewDatabase(kind);
        TDefImage image = TDefCodec.Parse(format, new byte[format.PageSize])!;
        var schema = new TableSchema(image, properties: null, propertiesLoaded: false);
        TableDef first = schema.CreateDefinition();
        Assert.Throws<NotSupportedException>(() => ((IList<ColumnInfo>)first.Columns).Add(new ColumnInfo { Name = "Injected", Type = ColumnType.LongIntegerType }));
        TableDef second = schema.CreateDefinition();
        Assert.Same(first, second);
        Assert.Empty(second.Columns);
        Assert.Empty(image.Columns);
        Assert.False(schema.PropertiesLoaded);
    }

    [Theory]
    [InlineData(ColumnType.ByteType)]
    [InlineData(ColumnType.LongIntegerType)]
    public void CalculatedResultType_ProjectsPersistedTypeAndKeepsFallback(ColumnType resultType)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.AceAccdb);
        byte[] bytes = new byte[format.PageSize];
        JetTypeInfo.Wu16(bytes, format.TDef.NumCols, 1);
        int descriptor = format.TDef.BlockEnd;
        bytes[descriptor + format.ColumnDescriptor.TypeOff] = (byte)ColumnType.MemoType;
        bytes[descriptor + 16] = 0xC0;
        int name = descriptor + format.ColumnDescriptor.Size;
        JetTypeInfo.Wu16(bytes, name, 2);
        bytes[name + 2] = (byte)'C';
        TDefImage image = TDefCodec.Parse(format, bytes)!;
        Assert.Equal(ColumnType.MemoType, TableSchema.CreateDefinition(image, null).Columns[0].Type);
        Assert.Equal(default, TableSchema.CreateDefinition(image, null).Columns[0].CalculatedResultType);
        var builder = new ColumnPropertyBlockBuilder();
        builder.GetOrAddTarget("C").AddByte(Constants.ColumnPropertyNames.ResultType, (byte)resultType);
        var properties = ColumnPropertyBlock.Parse(builder.ToBytes(JetFormat.ForNewDatabase(DatabaseFormat.AceAccdb)), JetFormat.ForNewDatabase(DatabaseFormat.AceAccdb));
        TableDef definition = TableSchema.CreateDefinition(image, properties);
        Assert.Equal(resultType, definition.Columns[0].CalculatedResultType);
        Assert.Equal(JetTypeInfo.ResolveClrType(definition.Columns[0]), definition.ClrTypes[0]);
        Assert.Equal(default, image.Columns[0].CalculatedResultType);
    }
}
