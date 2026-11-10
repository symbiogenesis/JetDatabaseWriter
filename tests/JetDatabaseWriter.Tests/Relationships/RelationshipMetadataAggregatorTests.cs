namespace JetDatabaseWriter.Tests.Relationships;

using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Relationships;
using JetDatabaseWriter.Schema.Models;
using Xunit;

public sealed class RelationshipMetadataAggregatorTests
{
    private static readonly string[] CatalogColumns =
        ["szRelationship", "szObject", "szReferencedObject", "szColumn", "szReferencedColumn", "icolumn", "grbit"];

    [Theory]
    [InlineData(typeof(byte), 2)]
    [InlineData(typeof(short), 0x1102)]
    [InlineData(typeof(int), 0x1102)]
    [InlineData(typeof(long), 0x1102)]
    public void CatalogAdapters_PreserveNumericWidthsCompositeOrderAndFlags(Type numericType, int flags)
    {
        object firstOrdinal = Convert.ChangeType(0, numericType, CultureInfo.InvariantCulture);
        object secondOrdinal = Convert.ChangeType(1, numericType, CultureInfo.InvariantCulture);
        object catalogFlags = Convert.ChangeType(flags, numericType, CultureInfo.InvariantCulture);
        object[][] rows =
        [
            ["FK_Children_Parents", "Children", "Parents", "RefB", "KeyB", secondOrdinal, firstOrdinal],
            ["fk_children_parents", "Children", "Parents", "RefA", "KeyA", firstOrdinal, catalogFlags],
        ];

        foreach (IReadOnlyList<RelationshipMetadata> result in ReadBoth(CatalogColumns, rows))
        {
            RelationshipMetadata relationship = Assert.Single(result);
            Assert.Equal("FK_Children_Parents", relationship.Name);
            Assert.Equal("Parents", relationship.PrimaryTable);
            Assert.Equal("Children", relationship.ForeignTable);
            Assert.Equal(["KeyA", "KeyB"], relationship.PrimaryColumns);
            Assert.Equal(["RefA", "RefB"], relationship.ForeignColumns);
            Assert.False(relationship.EnforcesReferentialIntegrity);
            Assert.Equal(flags != 2, relationship.CascadeUpdates);
            Assert.Equal(flags != 2, relationship.CascadeDeletes);
        }
    }

    [Theory]
    [InlineData(3)]
    [InlineData(4)]
    public void CatalogAdapters_OmitWholeGroupsWithMissingKeyColumns(int malformedColumn)
    {
        object[] malformed = ["Broken", "Children", "Parents", "RefB", "KeyB", 1, 0];
        malformed[malformedColumn] = DBNull.Value;
        object[][] rows =
        [
            ["Broken", "Children", "Parents", "RefA", "KeyA", 0, 0],
            [string.Empty, "Children", "Parents", "RefA", "KeyA", 0, 0],
            malformed,
            ["Valid", "Children", "Parents", "RefA", "KeyA", 0, 0],
        ];

        foreach (IReadOnlyList<RelationshipMetadata> result in ReadBoth(CatalogColumns, rows))
        {
            Assert.Equal("Valid", Assert.Single(result).Name);
        }
    }

    [Theory]
    [InlineData("szRelationship")]
    [InlineData("szObject")]
    [InlineData("szReferencedObject")]
    [InlineData("szColumn")]
    [InlineData("szReferencedColumn")]
    public void CatalogAdapters_MissingRequiredColumnsReturnNoRelationships(string missingColumn)
    {
        string[] columns = [.. CatalogColumns.Where(column => column != missingColumn)];
        object[] fullRow = ["Valid", "Children", "Parents", "RefA", "KeyA", 0, 0];
        object[][] rows = [[.. columns.Select(column => fullRow[Array.IndexOf(CatalogColumns, column)])]];
        foreach (IReadOnlyList<RelationshipMetadata> result in ReadBoth(columns, rows))
        {
            Assert.Empty(result);
        }
    }

    [Fact]
    public void CatalogAdapters_OptionalColumnsUseStoredOrderAndDefaultFlags()
    {
        string[] columns = [.. CatalogColumns.Take(5).Select(static name => name.ToUpperInvariant())];
        object[][] rows =
        [
            ["Second", "Children", "Parents", "RefB", "KeyB"],
            ["First", "Children", "Parents", "RefA", "KeyA"],
            ["second", "Children", "Parents", "RefA", "KeyA"],
        ];

        foreach (IReadOnlyList<RelationshipMetadata> result in ReadBoth(columns, rows))
        {
            Assert.Equal(["Second", "First"], result.Select(static relationship => relationship.Name));
            Assert.Equal(["KeyB", "KeyA"], result[0].PrimaryColumns);
            Assert.Equal(["RefB", "RefA"], result[0].ForeignColumns);
            Assert.All(result, static relationship =>
            {
                Assert.True(relationship.EnforcesReferentialIntegrity);
                Assert.False(relationship.CascadeUpdates);
                Assert.False(relationship.CascadeDeletes);
            });
        }
    }

    private static IReadOnlyList<RelationshipMetadata>[] ReadBoth(string[] columns, object[][] rows)
    {
        using DataTable table = new() { Locale = CultureInfo.InvariantCulture };
        foreach (string column in columns)
        {
            table.Columns.Add(column, typeof(object));
        }

        foreach (object[] row in rows)
        {
            table.Rows.Add(row);
        }

        TableDef definition = new()
        {
            Columns = [.. columns.Select(static name => new ColumnInfo { Name = name })],
        };
        return [RelationshipMetadataAggregator.Aggregate(table), RelationshipMetadataAggregator.Aggregate(definition, rows)];
    }
}
