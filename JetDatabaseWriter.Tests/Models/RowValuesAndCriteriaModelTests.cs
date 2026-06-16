namespace JetDatabaseWriter.Tests.Models;

using System;
using System.Linq;
using JetDatabaseWriter.Models;
using Xunit;

/// <summary>
/// Unit tests for the named-column / predicate model types (<see cref="RowValues"/>,
/// <see cref="RowCriteria"/>, <see cref="ColumnPredicate"/>) that do not require a
/// database.
/// </summary>
public sealed class RowValuesAndCriteriaModelTests
{
    [Fact]
    public void RowValues_IsCaseInsensitiveOnColumnNames()
    {
        var row = new RowValues { ["Name"] = "Alice" };

        Assert.True(row.Contains("name"));
        Assert.True(row.TryGetValue("NAME", out object? value));
        Assert.Equal("Alice", value);
        Assert.Equal("Alice", row["nAmE"]);
    }

    [Fact]
    public void RowValues_AddDuplicateColumnThrows()
    {
        var row = new RowValues { ["Id"] = 1 };

        Assert.Throws<ArgumentException>(() => row.Add("id", 2));
    }

    [Fact]
    public void RowValues_SetReturnsSelfForChaining()
    {
        RowValues row = RowValues.Create().Set("Id", 1).Set("Name", "Bob");

        Assert.Equal(2, row.Count);
        Assert.Equal(1, row["Id"]);
        Assert.Equal("Bob", row["Name"]);
    }

    [Fact]
    public void RowValues_IndexerSetReplacesExistingValue()
    {
        var row = new RowValues { ["Score"] = 1m };
        row["score"] = 2m;

        Assert.Equal(1, row.Count);
        Assert.Equal(2m, row["Score"]);
    }

    [Fact]
    public void RowValues_EmptyColumnNameThrows()
    {
        var row = new RowValues();

        Assert.Throws<ArgumentException>(() => row[string.Empty] = 1);
    }

    [Fact]
    public void RowValues_EnumeratesAssignedPairs()
    {
        var row = new RowValues { ["Id"] = 1, ["Name"] = "X" };

        var pairs = row.ToDictionary(p => p.Key, p => p.Value, StringComparer.OrdinalIgnoreCase);

        Assert.Equal(2, pairs.Count);
        Assert.Equal(1, pairs["Id"]);
        Assert.Equal("X", pairs["Name"]);
    }

    [Fact]
    public void RowCriteria_WhereAndBuildsConjunction()
    {
        RowCriteria criteria = RowCriteria.Where("Region", "West")
            .And(ColumnPredicate.GreaterThan("Score", 80m))
            .And("Active", true);

        Assert.Equal(3, criteria.Count);
        Assert.Equal(ColumnPredicateOperator.Equal, criteria.Predicates[0].Operator);
        Assert.Equal(ColumnPredicateOperator.GreaterThan, criteria.Predicates[1].Operator);
        Assert.Equal("Active", criteria.Predicates[2].ColumnName);
    }

    [Fact]
    public void RowCriteria_AllIsEmpty() => Assert.Empty(RowCriteria.All());

    [Fact]
    public void RowCriteria_CollectionInitializerAddsPredicates()
    {
        var criteria = new RowCriteria
        {
            ColumnPredicate.EqualTo("A", 1),
            ColumnPredicate.LessThan("B", 5),
        };

        Assert.Equal(2, criteria.Count);
    }

    [Fact]
    public void ColumnPredicate_OrderedFactoriesRejectNullOperand()
    {
        Assert.Throws<ArgumentNullException>(() => ColumnPredicate.GreaterThan("A", null!));
        Assert.Throws<ArgumentNullException>(() => ColumnPredicate.Between("A", null!, 1));
        Assert.Throws<ArgumentNullException>(() => ColumnPredicate.Between("A", 1, null!));
    }

    [Fact]
    public void ColumnPredicate_InCopiesOperands()
    {
        object?[] source = [1, 2, 3];
        var predicate = ColumnPredicate.In("Id", source);
        source[0] = 999;

        Assert.NotNull(predicate.Operands);
        Assert.Equal([1, 2, 3], predicate.Operands);
    }

    [Fact]
    public void ColumnPredicate_BetweenStoresBothBounds()
    {
        var predicate = ColumnPredicate.Between("Score", 10m, 20m);

        Assert.Equal(ColumnPredicateOperator.Between, predicate.Operator);
        Assert.Equal(10m, predicate.Operand);
        Assert.Equal(20m, predicate.UpperOperand);
    }
}
