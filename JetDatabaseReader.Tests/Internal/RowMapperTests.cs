namespace JetDatabaseReader.Tests;

using System;
using System.Collections.Generic;
using FluentAssertions;
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

        index.Should().HaveCount(3);
        index[0].Should().NotBeNull();
        index[1].Should().NotBeNull();
        index[2].Should().NotBeNull();
    }

    [Fact]
    public void BuildIndex_CaseInsensitive_MatchesRegardlessOfCase()
    {
        var headers = new List<string> { "ID", "nAmE", "PRICE" };

        var index = RowMapper<SimpleProduct>.BuildIndex(headers);

        index[0]!.Name.Should().Be("Id");
        index[1]!.Name.Should().Be("Name");
        index[2]!.Name.Should().Be("Price");
    }

    [Fact]
    public void BuildIndex_UnmatchedHeaders_ReturnsNullForThose()
    {
        var headers = new List<string> { "Id", "UnknownColumn", "Price" };

        var index = RowMapper<SimpleProduct>.BuildIndex(headers);

        index[0].Should().NotBeNull();
        index[1].Should().BeNull();
        index[2].Should().NotBeNull();
    }

    [Fact]
    public void BuildIndex_EmptyHeaders_ReturnsEmptyArray()
    {
        var headers = new List<string>();

        var index = RowMapper<SimpleProduct>.BuildIndex(headers);

        index.Should().BeEmpty();
    }

    [Fact]
    public void BuildIndex_ReadOnlyProperty_IsNotIncluded()
    {
        var headers = new List<string> { "Id", "Computed" };

        var index = RowMapper<ReadOnlyPoco>.BuildIndex(headers);

        index[0].Should().NotBeNull();
        index[1].Should().BeNull("read-only properties should not be mapped");
    }

    // ── Map — basic ───────────────────────────────────────────────────

    [Fact]
    public void Map_AllColumnsMatch_SetsAllProperties()
    {
        var headers = new List<string> { "Id", "Name", "Price" };
        var index = RowMapper<SimpleProduct>.BuildIndex(headers);
        var row = new object[] { 42, "Widget", 9.99m };

        SimpleProduct result = RowMapper<SimpleProduct>.Map(row, index);

        result.Id.Should().Be(42);
        result.Name.Should().Be("Widget");
        result.Price.Should().Be(9.99m);
    }

    [Fact]
    public void Map_UnmatchedColumn_IsIgnored()
    {
        var headers = new List<string> { "Id", "UnknownColumn", "Name" };
        var index = RowMapper<SimpleProduct>.BuildIndex(headers);
        var row = new object[] { 1, "extra-value", "Gadget" };

        SimpleProduct result = RowMapper<SimpleProduct>.Map(row, index);

        result.Id.Should().Be(1);
        result.Name.Should().Be("Gadget");
        result.Price.Should().Be(0m, "unmatched property stays at default");
    }

    // ── Map — nulls ──────────────────────────────────────────────────

    [Fact]
    public void Map_NullValue_LeavesPropertyAtDefault()
    {
        var headers = new List<string> { "Id", "Name" };
        var index = RowMapper<SimpleProduct>.BuildIndex(headers);
        var row = new object[] { 1, null! };

        SimpleProduct result = RowMapper<SimpleProduct>.Map(row, index);

        result.Name.Should().Be(string.Empty, "null should leave default intact");
    }

    [Fact]
    public void Map_DBNullValue_LeavesPropertyAtDefault()
    {
        var headers = new List<string> { "Id", "Name" };
        var index = RowMapper<SimpleProduct>.BuildIndex(headers);
        var row = new object[] { 1, DBNull.Value };

        SimpleProduct result = RowMapper<SimpleProduct>.Map(row, index);

        result.Name.Should().Be(string.Empty);
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

        result.Id.Should().Be(7);
        result.Name.Should().Be("Test");
        result.CreatedDate.Should().Be(date);
    }

    [Fact]
    public void Map_NullableProperty_WithNull_StaysNull()
    {
        var headers = new List<string> { "Id", "CreatedDate" };
        var index = RowMapper<NullableProduct>.BuildIndex(headers);
        var row = new object[] { 1, DBNull.Value };

        NullableProduct result = RowMapper<NullableProduct>.Map(row, index);

        result.Id.Should().Be(1);
        result.CreatedDate.Should().BeNull();
    }

    // ── Map — type coercion ──────────────────────────────────────────

    [Fact]
    public void Map_IntToLong_ConvertsSuccessfully()
    {
        var headers = new List<string> { "Id", "Price" };
        var index = RowMapper<TypeMismatchPoco>.BuildIndex(headers);
        var row = new object[] { 42, 19.99m };

        TypeMismatchPoco result = RowMapper<TypeMismatchPoco>.Map(row, index);

        result.Id.Should().Be(42L);
        result.Price.Should().Be(19.99);
    }

    // ── Map — row/index length mismatches ────────────────────────────

    [Fact]
    public void Map_RowShorterThanIndex_MapsAvailableValues()
    {
        var headers = new List<string> { "Id", "Name", "Price" };
        var index = RowMapper<SimpleProduct>.BuildIndex(headers);
        var row = new object[] { 5 }; // only one value

        SimpleProduct result = RowMapper<SimpleProduct>.Map(row, index);

        result.Id.Should().Be(5);
        result.Name.Should().Be(string.Empty, "unmapped due to short row");
    }

    [Fact]
    public void Map_RowLongerThanIndex_IgnoresExtraValues()
    {
        var headers = new List<string> { "Id" };
        var index = RowMapper<SimpleProduct>.BuildIndex(headers);
        var row = new object[] { 5, "extra1", "extra2" };

        SimpleProduct result = RowMapper<SimpleProduct>.Map(row, index);

        result.Id.Should().Be(5);
        result.Name.Should().Be(string.Empty);
    }

    // ── Map — empty POCO ─────────────────────────────────────────────

    [Fact]
    public void Map_EmptyPoco_ReturnsNewInstance()
    {
        var headers = new List<string> { "Id", "Name" };
        var index = RowMapper<EmptyPoco>.BuildIndex(headers);
        var row = new object[] { 1, "test" };

        EmptyPoco result = RowMapper<EmptyPoco>.Map(row, index);

        result.Should().NotBeNull();
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

        result1.Id.Should().Be(1);
        result1.Name.Should().Be("Alpha");
        result2.Id.Should().Be(2);
        result2.Name.Should().Be("Beta");
        result1.Should().NotBeSameAs(result2);
    }

    // ── Map — value already correct type (fast path) ─────────────────

    [Fact]
    public void Map_ValueAlreadyCorrectType_SkipsConversion()
    {
        var headers = new List<string> { "Id", "Name", "Price" };
        var index = RowMapper<SimpleProduct>.BuildIndex(headers);
        var row = new object[] { 99, "Direct", 5.5m };

        SimpleProduct result = RowMapper<SimpleProduct>.Map(row, index);

        result.Id.Should().Be(99);
        result.Name.Should().Be("Direct");
        result.Price.Should().Be(5.5m);
    }

    // ── ToRow — reverse mapping ──────────────────────────────────────

    [Fact]
    public void ToRow_AllPropertiesMatch_ReturnsCorrectValues()
    {
        var headers = new List<string> { "Id", "Name", "Price" };
        var index = RowMapper<SimpleProduct>.BuildIndex(headers);
        var product = new SimpleProduct { Id = 7, Name = "Bolt", Price = 1.25m };

        object[] row = RowMapper<SimpleProduct>.ToRow(product, index);

        row.Should().HaveCount(3);
        row[0].Should().Be(7);
        row[1].Should().Be("Bolt");
        row[2].Should().Be(1.25m);
    }

    [Fact]
    public void ToRow_UnmatchedColumn_ProducesDBNull()
    {
        var headers = new List<string> { "Id", "Unknown" };
        var index = RowMapper<SimpleProduct>.BuildIndex(headers);
        var product = new SimpleProduct { Id = 3 };

        object[] row = RowMapper<SimpleProduct>.ToRow(product, index);

        row[0].Should().Be(3);
        row[1].Should().Be(DBNull.Value);
    }

    [Fact]
    public void ToRow_NullPropertyValue_ProducesDBNull()
    {
        var headers = new List<string> { "Id", "Name" };
        var index = RowMapper<NullableProduct>.BuildIndex(headers);
        var product = new NullableProduct { Id = 1, Name = null };

        object[] row = RowMapper<NullableProduct>.ToRow(product, index);

        row[0].Should().Be(1);
        row[1].Should().Be(DBNull.Value);
    }

    [Fact]
    public void ToRow_RoundTrips_WithMap()
    {
        var headers = new List<string> { "Id", "Name", "Price" };
        var index = RowMapper<SimpleProduct>.BuildIndex(headers);
        var original = new SimpleProduct { Id = 42, Name = "Gadget", Price = 19.99m };

        object[] row = RowMapper<SimpleProduct>.ToRow(original, index);
        SimpleProduct roundTripped = RowMapper<SimpleProduct>.Map(row, index);

        roundTripped.Id.Should().Be(original.Id);
        roundTripped.Name.Should().Be(original.Name);
        roundTripped.Price.Should().Be(original.Price);
    }
}
