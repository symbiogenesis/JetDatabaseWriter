namespace JetDatabaseWriter.Tests.Relationships;

using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Relationships;
using JetDatabaseWriter.Schema.Models;
using Xunit;

public sealed class RelationshipColumnPolicyTests
{
    [Fact]
    public void PersistedCalculatedKey_RefusesWithoutCachingSuccessfulPreflight()
    {
        var definition = new TableDef
        {
            Columns =
            [
                new ColumnInfo { Name = "Id", Type = ColumnType.LongIntegerType },
                new ColumnInfo { Name = "Computed", Type = ColumnType.LongIntegerType, ExtraFlags = Constants.CalculatedColumn.ExtFlagMask },
            ],
        };
        var invalid = new FkContext([new FkRelationship("FK", "Parent", ["Computed"], "Child", ["Id"], true, true)]);
        JetOperationException error = Assert.Throws<JetOperationException>(() =>
            RelationshipEnforcer.ValidateKeyDescriptors("Parent", definition, invalid));
        Assert.Equal("Computed", error.ErrorInfo.ColumnName);
        Assert.Empty(invalid.ValidatedKeyTables);

        var valid = new FkContext([new FkRelationship("FK", "Parent", ["Id"], "Child", ["Id"], true, true)]);
        RelationshipEnforcer.ValidateKeyDescriptors("Parent", definition, valid);
        Assert.Contains("Parent", valid.ValidatedKeyTables);
    }
}
