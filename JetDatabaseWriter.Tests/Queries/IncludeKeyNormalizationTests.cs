namespace JetDatabaseWriter.Tests.Queries;

using System;
using JetDatabaseWriter.Indexes.Collation;
using JetDatabaseWriter.Queries;
using Xunit;

/// <summary>
/// Unit tests for the join-key normalization used by relationship-inferred eager loading
/// (<c>Include</c>). The normalizer collapses equal values to one canonical key regardless of
/// CLR type so the two sides of a relationship match by value. These tests pin the behavior of
/// the formerly arbitrary <c>"o" + value.ToString()</c> fallback: reachable scalar key types
/// (<c>char</c>, <c>byte[]</c>) now normalize deterministically, and any other CLR type is left
/// unmatchable (a <see langword="null"/> key) instead of relying on an arbitrary string.
/// </summary>
public sealed class IncludeKeyNormalizationTests
{
    private static readonly TextSortOrder SortOrder = new(0x0409, 0, true);

    [Fact]
    public void Char_NormalizesIntoStringKeySpace_MatchingSingleCharacterText()
    {
        // A char POCO key (mapped from a one-character Text column) must match the string the
        // related table decodes that column to, but must not collide with its numeric code point.
        Assert.Equal(IncludeLoader.Normalize("A", SortOrder), IncludeLoader.Normalize('A', SortOrder));
        Assert.NotEqual(IncludeLoader.Normalize('A', SortOrder), IncludeLoader.Normalize(65, SortOrder));
    }

    [Fact]
    public void ByteArray_EqualContent_DistinctInstances_ProduceEqualKeys()
    {
        byte[] first = [1, 2, 3, 4];
        byte[] second = [1, 2, 3, 4];

        Assert.NotNull(IncludeLoader.Normalize(first, SortOrder));
        Assert.Equal(IncludeLoader.Normalize(first, SortOrder), IncludeLoader.Normalize(second, SortOrder));
    }

    [Fact]
    public void ByteArray_DistinctContent_ProduceDistinctKeys_NotCollapsedToTypeName()
    {
        // Regression: the old fallback turned every byte[] into "oSystem.Byte[]", so unrelated
        // binary keys all matched each other. Distinct content must now yield distinct keys.
        byte[] one = [0];
        byte[] two = [0, 0];
        byte[] different = [9, 8, 7];

        Assert.NotEqual(IncludeLoader.Normalize(one, SortOrder), IncludeLoader.Normalize(two, SortOrder));
        Assert.NotEqual(IncludeLoader.Normalize(one, SortOrder), IncludeLoader.Normalize(different, SortOrder));
    }

    [Fact]
    public void UnsupportedKeyType_NormalizesToNull()
    {
        // No supported scalar pattern matches, so the key is unmatchable rather than an
        // arbitrary ToString() that could collapse distinct instances together.
        Assert.Null(IncludeLoader.Normalize(TimeSpan.FromMinutes(5), SortOrder));
        Assert.Null(IncludeLoader.Normalize(new object(), SortOrder));
    }

    [Fact]
    public void NumericTypes_CollapseByValue_AcrossClrTypes()
    {
        string? canonical = IncludeLoader.Normalize(5, SortOrder);

        Assert.NotNull(canonical);
        Assert.Equal(canonical, IncludeLoader.Normalize((byte)5, SortOrder));
        Assert.Equal(canonical, IncludeLoader.Normalize(5L, SortOrder));
        Assert.Equal(canonical, IncludeLoader.Normalize(5UL, SortOrder));
        Assert.Equal(canonical, IncludeLoader.Normalize(5f, SortOrder));
        Assert.Equal(canonical, IncludeLoader.Normalize(5d, SortOrder));
        Assert.Equal(canonical, IncludeLoader.Normalize(5m, SortOrder));
    }

    [Fact]
    public void StringDigits_DoNotMatchNumericValue() => Assert.NotEqual(IncludeLoader.Normalize(5, SortOrder), IncludeLoader.Normalize("5", SortOrder));

    [Fact]
    public void NullAndDbNull_NormalizeToNull()
    {
        Assert.Null(IncludeLoader.Normalize(null, SortOrder));
        Assert.Null(IncludeLoader.Normalize(DBNull.Value, SortOrder));
    }
}
