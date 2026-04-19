namespace JetDatabaseReader.Tests;

using System;
using System.Collections.Generic;
using Xunit;

#pragma warning disable CA1812 // Test POCOs are instantiated via reflection by RowMapper
#pragma warning disable SA1201 // Nested test POCOs before test methods is standard xUnit convention

public class RowMapperTests
{
    // ── Test POCOs ────────────────────────────────────────────────────

    private sealed class SimpleProduct
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;

        public decimal Price { get; set; }
    }

    private sealed class NullableProduct
    {
        public int? Id { get; set; }

        public string? Name { get; set; }

        public DateTime? CreatedDate { get; set; }
    }

    private sealed class ReadOnlyPoco
    {
        public int Id { get; set; }

        public string Computed => $"Item-{Id}";
    }

    private sealed class TypeMismatchPoco
    {
        public long Id { get; set; }

        public double Price { get; set; }
    }

    private sealed class EmptyPoco
    {
        // Intentionally left empty for testing.
        public override string ToString() => "EmptyPoco";
    }

    // ── BuildIndex ────────────────────────────────────────────────────

    [Fact]
    public void BuildIndex_MatchingHeaders_ReturnsNonNullEntries()
    {
        var headers = new List<string> { "Id", "Name", "Price" };

        var index = RowMapper<SimpleProduct>.BuildIndex(headers);

        Assert.Equal(3, index.Length);
        Assert.NotNull(index[0]);
        Assert.NotNull(index[1]);
        Assert.NotNull(index[2]);
    }

    [Fact]
    public void BuildIndex_CaseInsensitive_MatchesRegardlessOfCase()
    {
        var headers = new List<string> { "ID", "nAmE", "PRICE" };

        var index = RowMapper<SimpleProduct>.BuildIndex(headers);

        Assert.Equal("Id", index[0]!.Name);
        Assert.Equal("Name", index[1]!.Name);
        Assert.Equal("Price", index[2]!.Name);
    }

    [Fact]
    public void BuildIndex_UnmatchedHeaders_ReturnsNullForThose()
    {
        var headers = new List<string> { "Id", "UnknownColumn", "Price" };

        var index = RowMapper<SimpleProduct>.BuildIndex(headers);

        Assert.NotNull(index[0]);
        Assert.Null(index[1]);
        Assert.NotNull(index[2]);
    }

    [Fact]
    public void BuildIndex_EmptyHeaders_ReturnsEmptyArray()
    {
        var headers = new List<string>();

        var index = RowMapper<SimpleProduct>.BuildIndex(headers);

        Assert.Empty(index);
    }

    [Fact]
    public void BuildIndex_ReadOnlyProperty_IsNotIncluded()
    {
        var headers = new List<string> { "Id", "Computed" };

        var index = RowMapper<ReadOnlyPoco>.BuildIndex(headers);

        Assert.NotNull(index[0]);
        Assert.Null(index[1]);
    }

    // ── Map — basic ───────────────────────────────────────────────────

    [Fact]
    public void Map_AllColumnsMatch_SetsAllProperties()
    {
        var headers = new List<string> { "Id", "Name", "Price" };
        var index = RowMapper<SimpleProduct>.BuildIndex(headers);
        var row = new object[] { 42, "Widget", 9.99m };

        SimpleProduct result = RowMapper<SimpleProduct>.Map(row, index);

        Assert.Equal(42, result.Id);
        Assert.Equal("Widget", result.Name);
        Assert.Equal(9.99m, result.Price);
    }

    [Fact]
    public void Map_UnmatchedColumn_IsIgnored()
    {
        var headers = new List<string> { "Id", "UnknownColumn", "Name" };
        var index = RowMapper<SimpleProduct>.BuildIndex(headers);
        var row = new object[] { 1, "extra-value", "Gadget" };

        SimpleProduct result = RowMapper<SimpleProduct>.Map(row, index);

        Assert.Equal(1, result.Id);
        Assert.Equal("Gadget", result.Name);
        Assert.Equal(0m, result.Price);
    }

    // ── Map — nulls ──────────────────────────────────────────────────

    [Fact]
    public void Map_NullValue_LeavesPropertyAtDefault()
    {
        var headers = new List<string> { "Id", "Name" };
        var index = RowMapper<SimpleProduct>.BuildIndex(headers);
        var row = new object[] { 1, null! };

        SimpleProduct result = RowMapper<SimpleProduct>.Map(row, index);

        Assert.Equal(string.Empty, result.Name);
    }

    [Fact]
    public void Map_DBNullValue_LeavesPropertyAtDefault()
    {
        var headers = new List<string> { "Id", "Name" };
        var index = RowMapper<SimpleProduct>.BuildIndex(headers);
        var row = new object[] { 1, DBNull.Value };

        SimpleProduct result = RowMapper<SimpleProduct>.Map(row, index);

        Assert.Equal(string.Empty, result.Name);
    }

    // ── Map — nullable properties ────────────────────────────────────

    [Fact]
    public void Map_NullableProperty_WithValue_SetsProperty()
    {
        var headers = new List<string> { "Id", "Name", "CreatedDate" };
        var index = RowMapper<NullableProduct>.BuildIndex(headers);
        var date = new DateTime(2025, 6, 15);
        var row = new object[] { 7, "Test", date };

        NullableProduct result = RowMapper<NullableProduct>.Map(row, index);

        Assert.Equal(7, result.Id);
        Assert.Equal("Test", result.Name);
        Assert.Equal(date, result.CreatedDate);
    }

    [Fact]
    public void Map_NullableProperty_WithNull_StaysNull()
    {
        var headers = new List<string> { "Id", "CreatedDate" };
        var index = RowMapper<NullableProduct>.BuildIndex(headers);
        var row = new object[] { 1, DBNull.Value };

        NullableProduct result = RowMapper<NullableProduct>.Map(row, index);

        Assert.Equal(1, result.Id);
        Assert.Null(result.CreatedDate);
    }

    // ── Map — type coercion ──────────────────────────────────────────

    [Fact]
    public void Map_IntToLong_ConvertsSuccessfully()
    {
        var headers = new List<string> { "Id", "Price" };
        var index = RowMapper<TypeMismatchPoco>.BuildIndex(headers);
        var row = new object[] { 42, 19.99m };

        TypeMismatchPoco result = RowMapper<TypeMismatchPoco>.Map(row, index);

        Assert.Equal(42L, result.Id);
        Assert.Equal(19.99, result.Price);
    }

    // ── Map — row/index length mismatches ────────────────────────────

    [Fact]
    public void Map_RowShorterThanIndex_MapsAvailableValues()
    {
        var headers = new List<string> { "Id", "Name", "Price" };
        var index = RowMapper<SimpleProduct>.BuildIndex(headers);
        var row = new object[] { 5 }; // only one value

        SimpleProduct result = RowMapper<SimpleProduct>.Map(row, index);

        Assert.Equal(5, result.Id);
        Assert.Equal(string.Empty, result.Name);
    }

    [Fact]
    public void Map_RowLongerThanIndex_IgnoresExtraValues()
    {
        var headers = new List<string> { "Id" };
        var index = RowMapper<SimpleProduct>.BuildIndex(headers);
        var row = new object[] { 5, "extra1", "extra2" };

        SimpleProduct result = RowMapper<SimpleProduct>.Map(row, index);

        Assert.Equal(5, result.Id);
        Assert.Equal(string.Empty, result.Name);
    }

    // ── Map — empty POCO ─────────────────────────────────────────────

    [Fact]
    public void Map_EmptyPoco_ReturnsNewInstance()
    {
        var headers = new List<string> { "Id", "Name" };
        var index = RowMapper<EmptyPoco>.BuildIndex(headers);
        var row = new object[] { 1, "test" };

        EmptyPoco result = RowMapper<EmptyPoco>.Map(row, index);

        Assert.NotNull(result);
    }

    // ── Map — multiple rows share same index ─────────────────────────

    [Fact]
    public void Map_MultipleRows_ProducesIndependentInstances()
    {
        var headers = new List<string> { "Id", "Name", "Price" };
        var index = RowMapper<SimpleProduct>.BuildIndex(headers);
        var row1 = new object[] { 1, "Alpha", 10m };
        var row2 = new object[] { 2, "Beta", 20m };

        SimpleProduct result1 = RowMapper<SimpleProduct>.Map(row1, index);
        SimpleProduct result2 = RowMapper<SimpleProduct>.Map(row2, index);

        Assert.Equal(1, result1.Id);
        Assert.Equal("Alpha", result1.Name);
        Assert.Equal(2, result2.Id);
        Assert.Equal("Beta", result2.Name);
        Assert.NotSame(result1, result2);
    }

    // ── Map — value already correct type (fast path) ─────────────────

    [Fact]
    public void Map_ValueAlreadyCorrectType_SkipsConversion()
    {
        var headers = new List<string> { "Id", "Name", "Price" };
        var index = RowMapper<SimpleProduct>.BuildIndex(headers);
        var row = new object[] { 99, "Direct", 5.5m };

        SimpleProduct result = RowMapper<SimpleProduct>.Map(row, index);

        Assert.Equal(99, result.Id);
        Assert.Equal("Direct", result.Name);
        Assert.Equal(5.5m, result.Price);
    }

    // ── ToRow — reverse mapping ──────────────────────────────────────

    [Fact]
    public void ToRow_AllPropertiesMatch_ReturnsCorrectValues()
    {
        var headers = new List<string> { "Id", "Name", "Price" };
        var index = RowMapper<SimpleProduct>.BuildIndex(headers);
        var product = new SimpleProduct { Id = 7, Name = "Bolt", Price = 1.25m };

        object[] row = RowMapper<SimpleProduct>.ToRow(product, index);

        Assert.Equal(3, row.Length);
        Assert.Equal(7, row[0]);
        Assert.Equal("Bolt", row[1]);
        Assert.Equal(1.25m, row[2]);
    }

    [Fact]
    public void ToRow_UnmatchedColumn_ProducesDBNull()
    {
        var headers = new List<string> { "Id", "Unknown" };
        var index = RowMapper<SimpleProduct>.BuildIndex(headers);
        var product = new SimpleProduct { Id = 3 };

        object[] row = RowMapper<SimpleProduct>.ToRow(product, index);

        Assert.Equal(3, row[0]);
        Assert.Equal(DBNull.Value, row[1]);
    }

    [Fact]
    public void ToRow_NullPropertyValue_ProducesDBNull()
    {
        var headers = new List<string> { "Id", "Name" };
        var index = RowMapper<NullableProduct>.BuildIndex(headers);
        var product = new NullableProduct { Id = 1, Name = null };

        object[] row = RowMapper<NullableProduct>.ToRow(product, index);

        Assert.Equal(1, row[0]);
        Assert.Equal(DBNull.Value, row[1]);
    }

    [Fact]
    public void ToRow_RoundTrips_WithMap()
    {
        var headers = new List<string> { "Id", "Name", "Price" };
        var index = RowMapper<SimpleProduct>.BuildIndex(headers);
        var original = new SimpleProduct { Id = 42, Name = "Gadget", Price = 19.99m };

        object[] row = RowMapper<SimpleProduct>.ToRow(original, index);
        SimpleProduct roundTripped = RowMapper<SimpleProduct>.Map(row, index);

        Assert.Equal(original.Id, roundTripped.Id);
        Assert.Equal(original.Name, roundTripped.Name);
        Assert.Equal(original.Price, roundTripped.Price);
    }
}
