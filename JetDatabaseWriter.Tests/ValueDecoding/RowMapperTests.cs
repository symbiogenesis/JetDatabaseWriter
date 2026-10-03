namespace JetDatabaseWriter.Tests.ValueDecoding;

using System;
using System.Collections.Generic;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.ValueDecoding;
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

        public string Computed => $"Item-{this.Id}";
    }

    private sealed class TypeMismatchPoco
    {
        public long Id { get; set; }

        public double Price { get; set; }
    }

    private sealed class EmptyPoco
    {
        /// <summary>
        /// Intentionally left empty for testing.
        /// </summary>
        /// <returns>A string representation of the empty POCO, for debugging purposes.</returns>
        public override string ToString() => "EmptyPoco";
    }

    // ── TryGetAccessor ────────────────────────────────────────────────

    [Fact]
    public void TryGetAccessor_MatchingHeaders_ReturnsNonNullEntries()
    {
        Assert.NotNull(RowMapper<SimpleProduct>.TryGetAccessor("Id"));
        Assert.NotNull(RowMapper<SimpleProduct>.TryGetAccessor("Name"));
        Assert.NotNull(RowMapper<SimpleProduct>.TryGetAccessor("Price"));
    }

    [Fact]
    public void TryGetAccessor_CaseInsensitive_MatchesRegardlessOfCase()
    {
        Assert.Equal("Id", RowMapper<SimpleProduct>.TryGetAccessor("ID")!.Property.Name);
        Assert.Equal("Name", RowMapper<SimpleProduct>.TryGetAccessor("nAmE")!.Property.Name);
        Assert.Equal("Price", RowMapper<SimpleProduct>.TryGetAccessor("PRICE")!.Property.Name);
    }

    [Fact]
    public void TryGetAccessor_UnmatchedHeaders_ReturnsNullForThose()
    {
        Assert.NotNull(RowMapper<SimpleProduct>.TryGetAccessor("Id"));
        Assert.Null(RowMapper<SimpleProduct>.TryGetAccessor("UnknownColumn"));
        Assert.NotNull(RowMapper<SimpleProduct>.TryGetAccessor("Price"));
    }

    [Fact]
    public void TryGetAccessor_ReadOnlyProperty_IsNotIncluded()
    {
        Assert.NotNull(RowMapper<ReadOnlyPoco>.TryGetAccessor("Id"));
        Assert.Null(RowMapper<ReadOnlyPoco>.TryGetAccessor("Computed"));
    }

    // ── Build — basic ─────────────────────────────────────────────────

    [Fact]
    public void Build_AllColumnsMatch_SetsAllProperties()
    {
        var headers = new List<string> { "Id", "Name", "Price" };
        object[] row = [42, "Widget", 9.99m];

        SimpleProduct result = RowMapper<SimpleProduct>.Build(headers)(row);

        Assert.Equal(42, result.Id);
        Assert.Equal("Widget", result.Name);
        Assert.Equal(9.99m, result.Price);
    }

    [Fact]
    public void Build_UnmatchedColumn_IsIgnored()
    {
        var headers = new List<string> { "Id", "UnknownColumn", "Name" };
        object[] row = [1, "extra-value", "Gadget"];

        SimpleProduct result = RowMapper<SimpleProduct>.Build(headers)(row);

        Assert.Equal(1, result.Id);
        Assert.Equal("Gadget", result.Name);
        Assert.Equal(0m, result.Price);
    }

    // ── Build — nulls ────────────────────────────────────────────────

    [Fact]
    public void Build_NullValue_LeavesPropertyAtDefault()
    {
        var headers = new List<string> { "Id", "Name" };
        object[] row = [1, null!];

        SimpleProduct result = RowMapper<SimpleProduct>.Build(headers)(row);

        Assert.Equal(string.Empty, result.Name);
    }

    [Fact]
    public void Build_DBNullValue_LeavesPropertyAtDefault()
    {
        var headers = new List<string> { "Id", "Name" };
        object[] row = [1, DBNull.Value];

        SimpleProduct result = RowMapper<SimpleProduct>.Build(headers)(row);

        Assert.Equal(string.Empty, result.Name);
    }

    // ── Build — nullable properties ──────────────────────────────────

    [Fact]
    public void Build_NullableProperty_WithValue_SetsProperty()
    {
        var headers = new List<string> { "Id", "Name", "CreatedDate" };
        var date = new DateTime(2025, 6, 15);
        object[] row = [7, "Test", date];

        NullableProduct result = RowMapper<NullableProduct>.Build(headers)(row);

        Assert.Equal(7, result.Id);
        Assert.Equal("Test", result.Name);
        Assert.Equal(date, result.CreatedDate);
    }

    [Fact]
    public void Build_NullableProperty_WithNull_StaysNull()
    {
        var headers = new List<string> { "Id", "CreatedDate" };
        object[] row = [1, DBNull.Value];

        NullableProduct result = RowMapper<NullableProduct>.Build(headers)(row);

        Assert.Equal(1, result.Id);
        Assert.Null(result.CreatedDate);
    }

    // ── Build — type coercion ────────────────────────────────────────

    [Fact]
    public void Build_IntToLong_ConvertsSuccessfully()
    {
        var headers = new List<string> { "Id", "Price" };
        object[] row = [42, 19.99m];

        TypeMismatchPoco result = RowMapper<TypeMismatchPoco>.Build(headers)(row);

        Assert.Equal(42L, result.Id);
        Assert.Equal(19.99, result.Price);
    }

    // ── Build — row/header length mismatches ─────────────────────────

    [Fact]
    public void Build_RowShorterThanIndex_MapsAvailableValues()
    {
        var headers = new List<string> { "Id", "Name", "Price" };
        object[] row = [5]; // only one value

        SimpleProduct result = RowMapper<SimpleProduct>.Build(headers)(row);

        Assert.Equal(5, result.Id);
        Assert.Equal(string.Empty, result.Name);
    }

    [Fact]
    public void Build_RowLongerThanIndex_IgnoresExtraValues()
    {
        var headers = new List<string> { "Id" };
        object[] row = [5, "extra1", "extra2"];

        SimpleProduct result = RowMapper<SimpleProduct>.Build(headers)(row);

        Assert.Equal(5, result.Id);
        Assert.Equal(string.Empty, result.Name);
    }

    // ── Build — empty POCO ───────────────────────────────────────────

    [Fact]
    public void Build_EmptyPoco_ReturnsNewInstance()
    {
        var headers = new List<string> { "Id", "Name" };
        object[] row = [1, "test"];

        EmptyPoco result = RowMapper<EmptyPoco>.Build(headers)(row);

        Assert.NotNull(result);
    }

    // ── Build — multiple rows share one delegate ─────────────────────

    [Fact]
    public void Build_MultipleRows_ProducesIndependentInstances()
    {
        var headers = new List<string> { "Id", "Name", "Price" };
        Func<object?[], SimpleProduct> map = RowMapper<SimpleProduct>.Build(headers);
        object[] row1 = [1, "Alpha", 10m];
        object[] row2 = [2, "Beta", 20m];

        SimpleProduct result1 = map(row1);
        SimpleProduct result2 = map(row2);

        Assert.Equal(1, result1.Id);
        Assert.Equal("Alpha", result1.Name);
        Assert.Equal(2, result2.Id);
        Assert.Equal("Beta", result2.Name);
        Assert.NotSame(result1, result2);
    }

    // ── Build — value already correct type (fast path) ───────────────

    [Fact]
    public void Build_ValueAlreadyCorrectType_SkipsConversion()
    {
        var headers = new List<string> { "Id", "Name", "Price" };
        object[] row = [99, "Direct", 5.5m];

        SimpleProduct result = RowMapper<SimpleProduct>.Build(headers)(row);

        Assert.Equal(99, result.Id);
        Assert.Equal("Direct", result.Name);
        Assert.Equal(5.5m, result.Price);
    }

    // ── ToRow(TableDef) — reverse mapping ────────────────────────────

    private static TableDef MakeTableDef(params string[] columnNames)
    {
        var td = new TableDef();
        foreach (string name in columnNames)
        {
            td.Columns.Add(new ColumnInfo { Name = name });
        }

        return td;
    }

    [Fact]
    public void ToRow_AllPropertiesMatch_ReturnsCorrectValues()
    {
        TableDef td = MakeTableDef("Id", "Name", "Price");
        var product = new SimpleProduct { Id = 7, Name = "Bolt", Price = 1.25m };

        object[] row = RowMapper<SimpleProduct>.ToRow(td, product);

        Assert.Equal(3, row.Length);
        Assert.Equal(7, row[0]);
        Assert.Equal("Bolt", row[1]);
        Assert.Equal(1.25m, row[2]);
    }

    [Fact]
    public void ToRow_UnmatchedColumn_ProducesDBNull()
    {
        TableDef td = MakeTableDef("Id", "Unknown");
        var product = new SimpleProduct { Id = 3 };

        object[] row = RowMapper<SimpleProduct>.ToRow(td, product);

        Assert.Equal(3, row[0]);
        Assert.Equal(DBNull.Value, row[1]);
    }

    [Fact]
    public void ToRow_NullPropertyValue_ProducesDBNull()
    {
        TableDef td = MakeTableDef("Id", "Name");
        var product = new NullableProduct { Id = 1, Name = null };

        object[] row = RowMapper<NullableProduct>.ToRow(td, product);

        Assert.Equal(1, row[0]);
        Assert.Equal(DBNull.Value, row[1]);
    }

    [Fact]
    public void ToRow_NullableValueTypeWithNoValue_ProducesDBNull()
    {
        TableDef td = MakeTableDef("Id", "Name", "CreatedDate");
        var product = new NullableProduct { Id = null, Name = null, CreatedDate = null };

        object[] row = RowMapper<NullableProduct>.ToRow(td, product);

        Assert.Equal(DBNull.Value, row[0]);
        Assert.Equal(DBNull.Value, row[1]);
        Assert.Equal(DBNull.Value, row[2]);
    }

    [Fact]
    public void ToRow_NonNullableValueType_BoxedAsIs()
    {
        TableDef td = MakeTableDef("Id", "Name", "Price");
        var product = new SimpleProduct { Id = 0, Name = "Zero", Price = 0m };

        object[] row = RowMapper<SimpleProduct>.ToRow(td, product);

        // Default values for non-nullable value types must NOT be coerced to DBNull.
        Assert.Equal(0, row[0]);
        Assert.Equal(0m, row[2]);
    }

    [Fact]
    public void ToRow_EmptyPoco_AllDBNull()
    {
        TableDef td = MakeTableDef("Col1", "Col2");
        var item = new EmptyPoco();

        object[] row = RowMapper<EmptyPoco>.ToRow(td, item);

        Assert.Equal(2, row.Length);
        Assert.Equal(DBNull.Value, row[0]);
        Assert.Equal(DBNull.Value, row[1]);
    }

    [Fact]
    public void ToRow_RoundTrips_WithBuild()
    {
        TableDef td = MakeTableDef("Id", "Name", "Price");
        var headers = new List<string> { "Id", "Name", "Price" };
        var original = new SimpleProduct { Id = 42, Name = "Gadget", Price = 19.99m };

        object[] row = RowMapper<SimpleProduct>.ToRow(td, original);
        SimpleProduct roundTripped = RowMapper<SimpleProduct>.Build(headers)(row);

        Assert.Equal(original.Id, roundTripped.Id);
        Assert.Equal(original.Name, roundTripped.Name);
        Assert.Equal(original.Price, roundTripped.Price);
    }

    [Fact]
    public void NullableProduct_ToRow_ThenBuild_RoundTrips()
    {
        TableDef td = MakeTableDef("Id", "Name", "CreatedDate");
        var headers = new List<string> { "Id", "Name", "CreatedDate" };
        var original = new NullableProduct
        {
            Id = 99,
            Name = "RoundTrip",
            CreatedDate = new DateTime(2025, 12, 25),
        };

        object[] row = RowMapper<NullableProduct>.ToRow(td, original);
        NullableProduct result = RowMapper<NullableProduct>.Build(headers)(row);

        Assert.Equal(original.Id, result.Id);
        Assert.Equal(original.Name, result.Name);
        Assert.Equal(original.CreatedDate, result.CreatedDate);
    }

    [Fact]
    public void NullableProduct_WithAllNulls_ToRow_ThenBuild_StaysNull()
    {
        TableDef td = MakeTableDef("Id", "Name", "CreatedDate");
        var headers = new List<string> { "Id", "Name", "CreatedDate" };
        var original = new NullableProduct { Id = null, Name = null, CreatedDate = null };

        object[] row = RowMapper<NullableProduct>.ToRow(td, original);
        NullableProduct result = RowMapper<NullableProduct>.Build(headers)(row);

        Assert.Null(result.Id);
        Assert.Null(result.Name);
        Assert.Null(result.CreatedDate);
    }

    [Fact]
    public void ToRow_CaseInsensitiveColumnMatch()
    {
        // Column names differ in case from property names; matching is case-insensitive.
        TableDef td = MakeTableDef("ID", "nAmE", "PRICE");
        var product = new SimpleProduct { Id = 5, Name = "Case", Price = 2.5m };

        object[] row = RowMapper<SimpleProduct>.ToRow(td, product);

        Assert.Equal(5, row[0]);
        Assert.Equal("Case", row[1]);
        Assert.Equal(2.5m, row[2]);
    }

    [Fact]
    public void ToRow_SameTableDefInstance_ReusesCompiledDelegate()
    {
        // The per-TableDef ConditionalWeakTable cache means repeated calls with
        // the same TableDef instance must produce identical output without re-
        // compiling. Verified indirectly by asserting consistent results across
        // many invocations; a regression that recompiled per call would still
        // pass functionally, but exercising the cached path here keeps it covered.
        TableDef td = MakeTableDef("Id", "Name", "Price");
        var product = new SimpleProduct { Id = 11, Name = "Cached", Price = 9.99m };

        for (int i = 0; i < 5; i++)
        {
            object[] row = RowMapper<SimpleProduct>.ToRow(td, product);
            Assert.Equal(11, row[0]);
            Assert.Equal("Cached", row[1]);
            Assert.Equal(9.99m, row[2]);
        }
    }

    // ── Build — all DBNull row ───────────────────────────────────────

    [Fact]
    public void Build_AllDBNullValues_LeavesAllPropertiesAtDefault()
    {
        var headers = new List<string> { "Id", "Name", "Price" };
        object[] row = [DBNull.Value, DBNull.Value, DBNull.Value];

        SimpleProduct result = RowMapper<SimpleProduct>.Build(headers)(row);

        Assert.Equal(0, result.Id);
        Assert.Equal(string.Empty, result.Name);
        Assert.Equal(0m, result.Price);
    }

    [Fact]
    public void Build_AllNullValues_LeavesAllPropertiesAtDefault()
    {
        var headers = new List<string> { "Id", "Name", "Price" };
        object[] row = [null!, null!, null!];

        SimpleProduct result = RowMapper<SimpleProduct>.Build(headers)(row);

        Assert.Equal(0, result.Id);
        Assert.Equal(string.Empty, result.Name);
        Assert.Equal(0m, result.Price);
    }

    // ── Build — inconvertible type ───────────────────────────────────

    [Fact]
    public void Build_InconvertibleType_ThrowsOnConversion()
    {
        var headers = new List<string> { "Id", "Price" };
        Func<object?[], TypeMismatchPoco> map = RowMapper<TypeMismatchPoco>.Build(headers);

        // "not-a-number" cannot be converted to long
        object[] row = ["not-a-number", 1.0m];

        Assert.ThrowsAny<Exception>(() => map(row));
    }

    // ── Build — empty row ────────────────────────────────────────────

    [Fact]
    public void Build_EmptyRow_ReturnsDefaultInstance()
    {
        var headers = new List<string> { "Id", "Name", "Price" };
        object[] row = [];

        SimpleProduct result = RowMapper<SimpleProduct>.Build(headers)(row);

        Assert.Equal(0, result.Id);
        Assert.Equal(string.Empty, result.Name);
        Assert.Equal(0m, result.Price);
    }

    // ── TryGetAccessor — duplicate headers ───────────────────────────

    [Fact]
    public void TryGetAccessor_DuplicateHeaders_AllGetAccessors()
    {
        var headers = new List<string> { "Id", "Id", "Name" };

        Assert.All(headers, header => Assert.NotNull(RowMapper<SimpleProduct>.TryGetAccessor(header)));
    }

    // ── Build — type coercion: string to int via ChangeType ──────────

    [Fact]
    public void Build_StringToInt_ConvertsSuccessfully()
    {
        var headers = new List<string> { "Id", "Name", "Price" };
        object[] row = ["123", "Test", "45.67"];

        SimpleProduct result = RowMapper<SimpleProduct>.Build(headers)(row);

        Assert.Equal(123, result.Id);
        Assert.Equal("Test", result.Name);
        Assert.Equal(45.67m, result.Price);
    }
}
