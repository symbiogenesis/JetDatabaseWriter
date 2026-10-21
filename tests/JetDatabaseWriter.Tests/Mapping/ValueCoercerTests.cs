namespace JetDatabaseWriter.Tests.Mapping;

using System;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Reflection;
using JetDatabaseWriter.Mapping;
using JetDatabaseWriter.Models;
using Xunit;

/// <summary>
/// Tests for <see cref="ValueCoercer"/>, the one conversion every typed read applies when a
/// decoded value is not already of the mapped property's type. One test per rule, in the
/// order the coercer applies them: pass-through, Hyperlink and text, enums, Guid, then
/// <see cref="Convert.ChangeType(object, Type, IFormatProvider)"/>; and the failure that
/// names the column and the property.
/// </summary>
public sealed class ValueCoercerTests
{
    private const string Column = "Source Column";

    private static readonly PropertyInfo Property = typeof(Target).GetProperty(nameof(Target.Value))!;

    /// <summary>An enum over <see cref="int"/>.</summary>
    [SuppressMessage("Design", "CA1515:Consider making public types internal", Justification = "Theory parameters of public xUnit test methods must be public.")]
    public enum Shade
    {
        /// <summary>No shade.</summary>
        None = 0,

        /// <summary>Red.</summary>
        Red = 1,

        /// <summary>Green.</summary>
        Green = 2,

        /// <summary>Blue.</summary>
        Blue = 3,
    }

    /// <summary>An enum over <see cref="byte"/>, so an <see cref="int"/> can overflow it.</summary>
    [SuppressMessage("Design", "CA1028:Enum storage should be Int32", Justification = "The test needs an underlying type narrower than the column's.")]
    internal enum ByteShade : byte
    {
        /// <summary>No shade.</summary>
        None = 0,

        /// <summary>Red.</summary>
        Red = 1,

        /// <summary>Green.</summary>
        Green = 2,
    }

    [Fact]
    public void Coerce_ValueTheTargetCanHold_PassesThroughUnchanged()
    {
        object boxed = 42;
        byte[] bytes = [1, 2, 3];

        Assert.Same(boxed, ValueCoercer.Coerce(boxed, typeof(int), Column, Property));
        Assert.Same(bytes, ValueCoercer.Coerce(bytes, typeof(object), Column, Property));
        Assert.Same(bytes, ValueCoercer.Coerce(bytes, typeof(byte[]), Column, Property));
        Assert.Equal("text", ValueCoercer.Coerce("text", typeof(string), Column, Property));
    }

    [Fact]
    public void Coerce_HyperlinkAndText_ConvertBothWays()
    {
        var link = (Hyperlink?)ValueCoercer.Coerce("Example#https://example.com#", typeof(Hyperlink), Column, Property);
        Assert.NotNull(link);
        Assert.Equal("Example", link.DisplayText);
        Assert.Equal("https://example.com", link.Address);

        Assert.Equal(link.ToString(), ValueCoercer.Coerce(link, typeof(string), Column, Property));

        // An empty string parses to no hyperlink, which skips the assignment.
        Assert.Null(ValueCoercer.Coerce(string.Empty, typeof(Hyperlink), Column, Property));
    }

    [Theory]
    [InlineData(2)]
    [InlineData((short)2)]
    [InlineData((byte)2)]
    [InlineData(2L)]
    [InlineData((sbyte)2)]
    [InlineData((ushort)2)]
    [InlineData(2U)]
    [InlineData(2UL)]
    public void Coerce_IntegralToEnum_MapsByValue(object value)
    {
        Assert.Equal(Shade.Green, ValueCoercer.Coerce(value, typeof(Shade), Column, Property));
        Assert.Equal(ByteShade.Green, ValueCoercer.Coerce(value, typeof(ByteShade), Column, Property));
    }

    [Fact]
    public void Coerce_IntegralToNullableEnum_MapsByValue() =>
        Assert.Equal(Shade.Blue, ValueCoercer.Coerce(3, typeof(Shade?), Column, Property));

    [Theory]
    [InlineData("Green", Shade.Green)]
    [InlineData("blue", Shade.Blue)]
    [InlineData("RED", Shade.Red)]
    [InlineData("3", Shade.Blue)]
    public void Coerce_TextToEnum_ParsesNameIgnoringCase(string value, Shade expected) =>
        Assert.Equal(expected, ValueCoercer.Coerce(value, typeof(Shade), Column, Property));

    [Fact]
    public void Coerce_IntegralOutsideTheEnumsNames_StillMaps()
    {
        // Enum.IsDefined is not checked, as a C# cast does not check it.
        Assert.Equal((Shade)99, ValueCoercer.Coerce(99, typeof(Shade), Column, Property));
        Assert.Equal((ByteShade)200, ValueCoercer.Coerce(200, typeof(ByteShade), Column, Property));
    }

    [Fact]
    public void Coerce_IntegralOverflowingTheEnumsUnderlyingType_Throws()
    {
        InvalidCastException ex = Assert.Throws<InvalidCastException>(
            () => ValueCoercer.Coerce(300, typeof(ByteShade), Column, Property));

        Assert.IsType<OverflowException>(ex.InnerException);
        AssertNamesColumnAndProperty(ex, typeof(int), typeof(ByteShade));
    }

    [Fact]
    public void Coerce_UnknownEnumName_Throws()
    {
        InvalidCastException ex = Assert.Throws<InvalidCastException>(
            () => ValueCoercer.Coerce("Purple", typeof(Shade), Column, Property));

        Assert.IsType<ArgumentException>(ex.InnerException, exactMatch: false);
        AssertNamesColumnAndProperty(ex, typeof(string), typeof(Shade));
    }

    [Theory]
    [InlineData("{6F9619FF-8B86-D011-B42D-00C04FC964FF}")]
    [InlineData("6f9619ff-8b86-d011-b42d-00c04fc964ff")]
    [InlineData("6f9619ff8b86d011b42d00c04fc964ff")]
    public void Coerce_TextToGuid_Parses(string value)
    {
        var expected = new Guid("6f9619ff-8b86-d011-b42d-00c04fc964ff");

        Assert.Equal(expected, ValueCoercer.Coerce(value, typeof(Guid), Column, Property));
        Assert.Equal(expected, ValueCoercer.Coerce(value, typeof(Guid?), Column, Property));
    }

    [Fact]
    public void Coerce_SixteenBytesToGuid_UsesTheGuidByteLayout()
    {
        var expected = new Guid("7c9e6679-7425-40de-944b-e07fc1f90ae7");

        Assert.Equal(expected, ValueCoercer.Coerce(expected.ToByteArray(), typeof(Guid), Column, Property));
    }

    [Fact]
    public void Coerce_BytesOfAnotherLengthToGuid_Throws()
    {
        InvalidCastException ex = Assert.Throws<InvalidCastException>(
            () => ValueCoercer.Coerce(new byte[15], typeof(Guid), Column, Property));

        AssertNamesColumnAndProperty(ex, typeof(byte[]), typeof(Guid));
    }

    [Fact]
    public void Coerce_MalformedGuidText_Throws()
    {
        InvalidCastException ex = Assert.Throws<InvalidCastException>(
            () => ValueCoercer.Coerce("not-a-guid", typeof(Guid), Column, Property));

        Assert.IsType<FormatException>(ex.InnerException);
        AssertNamesColumnAndProperty(ex, typeof(string), typeof(Guid));
    }

    [Fact]
    public void Coerce_OtherValues_ConvertWithTheInvariantCulture()
    {
        CultureInfo original = CultureInfo.CurrentCulture;
        try
        {
            // A culture whose decimal separator is a comma must not change the result.
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("de-DE");

            Assert.Equal(42L, ValueCoercer.Coerce(42, typeof(long), Column, Property));
            Assert.Equal(123, ValueCoercer.Coerce("123", typeof(int), Column, Property));
            Assert.Equal(45.67m, ValueCoercer.Coerce("45.67", typeof(decimal), Column, Property));
            Assert.Equal(19.5, ValueCoercer.Coerce(19.5m, typeof(double), Column, Property));
            Assert.Equal("2.5", ValueCoercer.Coerce(2.5, typeof(string), Column, Property));
            Assert.Equal((short)7, ValueCoercer.Coerce(7L, typeof(short?), Column, Property));
        }
        finally
        {
            CultureInfo.CurrentCulture = original;
        }
    }

    [Fact]
    public void Coerce_FractionToIntegralOrNarrowerFloatingTarget_Rounds()
    {
        // Convert.ChangeType rounds a fraction to the nearest integer, halves to even, as
        // Access's CInt does, and rounds a double to the nearest float. Only a value outside
        // the target's range throws.
        Assert.Equal(4, ValueCoercer.Coerce(3.7, typeof(int), Column, Property));
        Assert.Equal(2, ValueCoercer.Coerce(2.5, typeof(int), Column, Property));
        Assert.Equal(4, ValueCoercer.Coerce(3.5, typeof(int), Column, Property));
        Assert.Equal(12, ValueCoercer.Coerce(12.34m, typeof(int), Column, Property));
        Assert.Equal((byte)255, ValueCoercer.Coerce(255.4, typeof(byte), Column, Property));
        Assert.Equal(0.1f, ValueCoercer.Coerce(0.1, typeof(float), Column, Property));

        InvalidCastException ex = Assert.Throws<InvalidCastException>(
            () => ValueCoercer.Coerce(255.5, typeof(byte), Column, Property));
        Assert.IsType<OverflowException>(ex.InnerException);
    }

    [Fact]
    public void Coerce_UnparseableText_ThrowsNamingColumnPropertyAndTypes()
    {
        InvalidCastException ex = Assert.Throws<InvalidCastException>(
            () => ValueCoercer.Coerce("not-a-number", typeof(int), Column, Property));

        Assert.IsType<FormatException>(ex.InnerException);
        AssertNamesColumnAndProperty(ex, typeof(string), typeof(int));
    }

    [Fact]
    public void Coerce_ValueOverflowingTheTarget_ThrowsNamingColumnPropertyAndTypes()
    {
        InvalidCastException ex = Assert.Throws<InvalidCastException>(
            () => ValueCoercer.Coerce(300, typeof(byte), Column, Property));

        Assert.IsType<OverflowException>(ex.InnerException);
        AssertNamesColumnAndProperty(ex, typeof(int), typeof(byte));
    }

    [Fact]
    public void Coerce_ValueWithNoConversion_ThrowsNamingColumnPropertyAndTypes()
    {
        InvalidCastException ex = Assert.Throws<InvalidCastException>(
            () => ValueCoercer.Coerce(new DateTime(2024, 1, 2), typeof(Guid), Column, Property));

        Assert.IsType<InvalidCastException>(ex.InnerException);
        AssertNamesColumnAndProperty(ex, typeof(DateTime), typeof(Guid));
    }

    private static void AssertNamesColumnAndProperty(InvalidCastException ex, Type valueType, Type targetType)
    {
        Assert.Contains($"'{Column}'", ex.Message, StringComparison.Ordinal);
        Assert.Contains($"'{nameof(Target)}.{nameof(Target.Value)}'", ex.Message, StringComparison.Ordinal);
        Assert.Contains(valueType.ToString(), ex.Message, StringComparison.Ordinal);
        Assert.Contains(targetType.ToString(), ex.Message, StringComparison.Ordinal);
        Assert.NotNull(ex.InnerException);
    }

    /// <summary>Supplies the property the failure messages name.</summary>
#pragma warning disable CA1812 // Only its PropertyInfo is used.
    private sealed class Target
    {
        public object? Value { get; set; }
    }
#pragma warning restore CA1812
}
