namespace JetDatabaseWriter.Tests.Queries;

using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Queries;
using JetDatabaseWriter.ValueDecoding;
using Xunit;

/// <summary>Include materialization keeps row order, defaults and user exceptions.</summary>
public sealed class RuntimeRowMapperTests
{
    [Fact]
    public void Build_NullsAndMissingCells_KeepDefaultsAndSetterOrder()
    {
        string[] headers = ["Label", "Ignored", "Number", "Small", "Optional"];
        Func<object?[], object> mapper = RuntimeRowMapper.Build(typeof(ObservedRow), headers);
        ObservedRow mapped = Assert.IsType<ObservedRow>(mapper(["mapped", 99, 12L, null, DBNull.Value]));
        Assert.Equal(["constructed", "Label", "Number"], mapped.Assignments);
        Assert.Equal("mapped", mapped.Label);
        Assert.Equal(12, mapped.Number);
        Assert.Equal(9, mapped.Small);
        Assert.Equal(15, mapped.Optional);
        Assert.Equal(10, mapped.Ignored);

        ObservedRow shortRow = Assert.IsType<ObservedRow>(mapper([DBNull.Value]));
        Assert.Equal(["constructed"], shortRow.Assignments);
        Assert.Equal("default", shortRow.Label);
        Assert.Equal(7, shortRow.Number);
    }

    [Fact]
    public void Build_Overflow_ReportsColumnAndProperty()
    {
        InvalidCastException exception = Assert.Throws<InvalidCastException>(
            () => RuntimeRowMapper.Build(typeof(ObservedRow), ["Small"])([300]));
        Assert.Contains("'Small'", exception.Message, StringComparison.Ordinal);
        Assert.Contains("'ObservedRow.Small'", exception.Message, StringComparison.Ordinal);
        Assert.IsType<OverflowException>(exception.InnerException);
    }

    [Fact]
    public void Build_ThrowingSetter_PropagatesTheOriginalException()
    {
        InvalidOperationException exception = Assert.Throws<InvalidOperationException>(
            () => RuntimeRowMapper.Build(typeof(ThrowingRow), ["Value"])([1]));
        Assert.Equal("setter failed", exception.Message);
    }

    [Fact]
    public void Build_ConflictingMappings_ThrowsWithoutAReflectionWrapper() =>
        Assert.Throws<JetOperationException>(
            () => RuntimeRowMapper.Build(typeof(ConflictingRow), ["Value"]));

    [Fact]
    public void Build_ReusesTheGenericMappersBoundedShapeCache()
    {
        Func<object?[], object> original = RuntimeRowMapper.Build(typeof(CachedRow), ["Id"]);
        Assert.Same(RowMapper<CachedRow>.Build(["Id"]), original);
        Assert.Same(original, RuntimeRowMapper.Build(typeof(CachedRow), ["Id"]));
        for (int i = 0; i < 64; i++)
        {
            _ = RuntimeRowMapper.Build(typeof(CachedRow), ["Id", "Unmapped" + i]);
        }

        Assert.NotSame(original, RuntimeRowMapper.Build(typeof(CachedRow), ["Id"]));
        Assert.Equal(5, Assert.IsType<CachedRow>(original([5])).Id);
    }

#pragma warning disable CA1812 // The runtime mapper constructs these test entities through compiled expressions.
    private sealed class ObservedRow
    {
        [NotMapped]
        public List<string> Assignments { get; } = ["constructed"];

        public int Number
        {
            get;
            set
            {
                this.Assignments.Add(nameof(this.Number));
                field = value;
            }
        } = 7;

        public string Label
        {
            get;
            set
            {
                this.Assignments.Add(nameof(this.Label));
                field = value;
            }
        } = "default";

        public byte Small { get; set; } = 9;

        public int? Optional { get; set; } = 15;

        [NotMapped]
        public int Ignored { get; set; } = 10;
    }

    private sealed class ThrowingRow
    {
        public int Value
        {
            get;
            set
            {
                field = value;
                throw new InvalidOperationException("setter failed");
            }
        }
    }

    private sealed class ConflictingRow
    {
        [Column("Value")]
        public int First { get; set; }

        [Column("Value")]
        public int Second { get; set; }
    }

    private sealed class CachedRow
    {
        public int Id { get; set; }
    }
#pragma warning restore CA1812
}
