namespace JetDatabaseWriter.Tests.Infrastructure;

using JetDatabaseWriter.Infrastructure;
using Xunit;

public sealed class BoxCacheTests
{
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Bool_ReturnsBoxedBool_WithCorrectValue(bool value)
    {
        object boxed = BoxCache.Bool(value);

        Assert.Equal(typeof(bool), boxed.GetType());
        Assert.Equal(value, (bool)boxed);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Bool_ReturnsSameInstance_ForSameValue(bool value)
    {
        Assert.Same(BoxCache.Bool(value), BoxCache.Bool(value));
    }

    [Fact]
    public void Bool_TrueAndFalse_AreDistinctInstances()
    {
        Assert.NotSame(BoxCache.Bool(true), BoxCache.Bool(false));
    }

    [Fact]
    public void Byte_ReturnsBoxedByte_AndCachedInstance_ForEveryValue()
    {
        for (int i = 0; i <= byte.MaxValue; i++)
        {
            byte value = (byte)i;
            object boxed = BoxCache.Byte(value);

            Assert.Equal(typeof(byte), boxed.GetType());
            Assert.Equal(value, (byte)boxed);
            Assert.Same(boxed, BoxCache.Byte(value));
        }
    }

    [Theory]
    [InlineData((short)-1)]
    [InlineData((short)0)]
    [InlineData((short)1)]
    [InlineData((short)256)]
    public void Int16_WithinCachedRange_ReturnsBoxedShort_AndCachedInstance(short value)
    {
        object boxed = BoxCache.Int16(value);

        Assert.Equal(typeof(short), boxed.GetType());
        Assert.Equal(value, (short)boxed);
        Assert.Same(boxed, BoxCache.Int16(value));
    }

    [Theory]
    [InlineData((short)-2)]
    [InlineData((short)257)]
    [InlineData(short.MinValue)]
    [InlineData(short.MaxValue)]
    public void Int16_OutsideCachedRange_ReturnsBoxedShort_WithCorrectValue(short value)
    {
        object boxed = BoxCache.Int16(value);

        Assert.Equal(typeof(short), boxed.GetType());
        Assert.Equal(value, (short)boxed);
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(256)]
    public void Int32_WithinCachedRange_ReturnsBoxedInt_AndCachedInstance(int value)
    {
        object boxed = BoxCache.Int32(value);

        Assert.Equal(typeof(int), boxed.GetType());
        Assert.Equal(value, (int)boxed);
        Assert.Same(boxed, BoxCache.Int32(value));
    }

    [Theory]
    [InlineData(-2)]
    [InlineData(257)]
    [InlineData(int.MinValue)]
    [InlineData(int.MaxValue)]
    public void Int32_OutsideCachedRange_ReturnsBoxedInt_WithCorrectValue(int value)
    {
        object boxed = BoxCache.Int32(value);

        Assert.Equal(typeof(int), boxed.GetType());
        Assert.Equal(value, (int)boxed);
    }

    [Fact]
    public void Int16_AndInt32_CachedRangeBoundaries_AreInterned()
    {
        // Boundary values either side of the cached window.
        Assert.Same(BoxCache.Int16(-1), BoxCache.Int16(-1));
        Assert.Same(BoxCache.Int16(256), BoxCache.Int16(256));
        Assert.Same(BoxCache.Int32(-1), BoxCache.Int32(-1));
        Assert.Same(BoxCache.Int32(256), BoxCache.Int32(256));
    }
}
