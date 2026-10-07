namespace JetDatabaseWriter.Tests.Schema;

using System;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using Xunit;
using static JetDatabaseWriter.Enums.ColumnType;

public sealed class TDefPageBuilderTests
{
    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public void UnindexedTable_DeclaresNativeTerminatorAndReservedBytes(DatabaseFormat databaseFormat)
    {
        var format = JetFormat.ForNewDatabase(databaseFormat);
        TableDef tableDef = TDefPageBuilder.BuildTableDefinition(
            [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Data", typeof(byte[]), 4)],
            format);
        var builder = new TDefPageBuilder(format, null!);
        byte[] page = builder.BuildTDefPage(tableDef);
        int end = BitConverter.ToInt32(page, 8) + 8;
        Assert.Equal(ushort.MaxValue, BitConverter.ToUInt16(page, end - 10));
        Assert.Equal(new byte[8], page.AsSpan(end - 8, 8).ToArray());
        Assert.Equal(format.PageSize - end, BitConverter.ToUInt16(page, 2));
    }

    [Fact]
    public void BuildTableDefinition_DateTimeExtended_UsesFixedDeclaredSize()
    {
        TableDef tableDef = TDefPageBuilder.BuildTableDefinition(
            [new ColumnDefinition("ExtendedAt", typeof(DateTime)) { IsDateTimeExtended = true }],
            JetFormat.ForNewDatabase(DatabaseFormat.AceAccdb));

        ColumnInfo column = Assert.Single(tableDef.Columns);
        Assert.Equal(DateTimeExtendedType, column.Type);
        Assert.Equal(42, column.Size);
        Assert.Equal(0, column.FixedOff);
        Assert.True(column.IsFixed);
    }
}
