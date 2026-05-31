namespace JetDatabaseWriter.Tests.Infrastructure;

using JetDatabaseWriter.Infrastructure;
using Xunit;

public sealed class BinaryBufferTests
{
    [Fact]
    public void CopySlice_CopiesRequestedRange()
    {
        byte[] copied = BinaryBuffer.CopySlice([0, 1, 2, 3, 4], 1, 3);

        Assert.Equal([1, 2, 3], copied);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void CopySlice_ReturnsEmptyForNonPositiveLength(int length)
    {
        byte[] copied = BinaryBuffer.CopySlice([0, 1, 2], 1, length);

        Assert.Empty(copied);
    }

    [Fact]
    public void CopyTail_CopiesFromStartToEnd()
    {
        byte[] copied = BinaryBuffer.CopyTail([0, 1, 2, 3], 2);

        Assert.Equal([2, 3], copied);
    }
}
