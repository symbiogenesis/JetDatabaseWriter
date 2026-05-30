namespace JetDatabaseWriter.Tests.Schema;

using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Models;
using Xunit;
using static JetDatabaseWriter.Enums.ColumnType;

public sealed class TDefPageBuilderTests
{
    [Fact]
    public void BuildTableDefinition_DateTimeExtended_UsesFixedDeclaredSize()
    {
        TableDef tableDef = AccessWriter.BuildTableDefinition(
            [new ColumnDefinition("ExtendedAt", typeof(string)) { ColumnTypeOverride = DateTimeExtendedType }],
            DatabaseFormat.AceAccdb);

        ColumnInfo column = Assert.Single(tableDef.Columns);
        Assert.Equal(DateTimeExtendedType, column.Type);
        Assert.Equal(42, column.Size);
        Assert.Equal(0, column.FixedOff);
        Assert.True(column.IsFixed);
    }
}
