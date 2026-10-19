namespace JetDatabaseWriter.Tests.Indexes;

using System;
using JetDatabaseWriter.Indexes;
using Xunit;

/// <summary>Unsigned lexicographic ordering of encoded index keys.</summary>
public sealed class IndexKeyComparisonTests
{
    [Theory]
    [InlineData(new byte[] { }, new byte[] { }, 0)]
    [InlineData(new byte[] { }, new byte[] { 0 }, -1)]
    [InlineData(new byte[] { 0 }, new byte[] { }, 1)]
    [InlineData(new byte[] { 1, 2 }, new byte[] { 1, 2 }, 0)]
    [InlineData(new byte[] { 1, 2 }, new byte[] { 1, 2, 0 }, -1)]
    [InlineData(new byte[] { 1, 2, 0 }, new byte[] { 1, 2 }, 1)]
    [InlineData(new byte[] { 127 }, new byte[] { 128 }, -1)]
    [InlineData(new byte[] { 255 }, new byte[] { 0 }, 1)]
    public void CompareKeyBytes_UsesUnsignedLexicographicOrder(byte[] left, byte[] right, int expected)
        => Assert.Equal(expected, Math.Sign(IndexPageCodec.CompareKeyBytes(left, right)));
}
