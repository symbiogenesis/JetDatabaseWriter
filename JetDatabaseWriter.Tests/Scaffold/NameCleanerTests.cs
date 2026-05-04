namespace JetDatabaseWriter.Tests.Scaffold;

using JetDatabaseWriter.Scaffold;
using Xunit;

public class NameCleanerTests
{
    [Theory]
    [InlineData("Customers", "Customers")]
    [InlineData("order_details", "OrderDetails")]
    [InlineData("my table", "MyTable")]
    [InlineData("some-hyphenated-name", "SomeHyphenatedName")]
    [InlineData("dotted.name", "DottedName")]
    public void ToClassName_Converts_To_PascalCase(string input, string expected)
    {
        Assert.Equal(expected, NameCleaner.ToClassName(input));
    }

    [Theory]
    [InlineData("first_name", "FirstName")]
    [InlineData("Last Name", "LastName")]
    [InlineData("email-address", "EmailAddress")]
    [InlineData("company.name", "CompanyName")]
    public void ToPropertyName_Converts_To_PascalCase(string input, string expected)
    {
        Assert.Equal(expected, NameCleaner.ToPropertyName(input));
    }

    [Theory]
    [InlineData("123_table", "_123Table")]
    [InlineData("4column", "_4column")]
    public void ToClassName_Prefixes_Underscore_When_Starts_With_Digit(string input, string expected)
    {
        Assert.Equal(expected, NameCleaner.ToClassName(input));
    }

    [Theory]
    [InlineData("class", "Class")]
    [InlineData("int", "Int")]
    [InlineData("string", "String")]
    [InlineData("return", "Return")]
    public void ToClassName_PascalCases_Keywords(string input, string expected)
    {
        // PascalCase capitalizes the first letter, so keywords become non-keywords
        Assert.Equal(expected, NameCleaner.ToClassName(input));
    }

    [Theory]
    [InlineData("class", "Class")]
    [InlineData("int", "Int")]
    [InlineData("return", "Return")]
    public void ToPropertyName_PascalCases_Keywords(string input, string expected)
    {
        Assert.Equal(expected, NameCleaner.ToPropertyName(input));
    }

    [Fact]
    public void ToClassName_Strips_Invalid_Characters()
    {
        Assert.Equal("Hello123", NameCleaner.ToClassName("Hello!@#$%123"));
    }

    [Fact]
    public void ToClassName_Collapses_Multiple_Underscores()
    {
        Assert.Equal("AbCd", NameCleaner.ToClassName("ab___cd"));
    }

    [Theory]
    [InlineData("")]
    [InlineData("!!!")]
    [InlineData("@#$")]
    public void ToClassName_Returns_Unknown_For_Unsanitizable_Input(string input)
    {
        Assert.Equal("Unknown", NameCleaner.ToClassName(input));
    }

    [Fact]
    public void ToPropertyName_And_ToClassName_Are_Consistent()
    {
        string input = "some_column_name";
        Assert.Equal(NameCleaner.ToClassName(input), NameCleaner.ToPropertyName(input));
    }

    [Fact]
    public void ToClassName_Preserves_Unicode_Letters()
    {
        Assert.Equal("CaféTable", NameCleaner.ToClassName("café_table"));
    }

    [Fact]
    public void ToClassName_Handles_Mixed_Separators()
    {
        Assert.Equal("ABCDef", NameCleaner.ToClassName("a_b-c.def"));
    }

    [Theory]
    [InlineData("_table", "Table")]
    [InlineData("__double", "Double")]
    [InlineData("-leading-dash", "LeadingDash")]
    public void ToClassName_Leading_Separators_Do_Not_Produce_Extra_Capitals(string input, string expected)
    {
        Assert.Equal(expected, NameCleaner.ToClassName(input));
    }

    [Theory]
    [InlineData("table_", "Table")]
    [InlineData("name-", "Name")]
    [InlineData("end.", "End")]
    public void ToClassName_Trailing_Separators_Are_Ignored(string input, string expected)
    {
        Assert.Equal(expected, NameCleaner.ToClassName(input));
    }

    [Theory]
    [InlineData("a", "A")]
    [InlineData("Z", "Z")]
    [InlineData("5", "_5")]
    public void ToClassName_Single_Character_Input(string input, string expected)
    {
        Assert.Equal(expected, NameCleaner.ToClassName(input));
    }

    [Fact]
    public void ToClassName_Long_Input_Uses_Heap_Allocation()
    {
        // Names >= 128 chars exercise the heap-allocated array path
        string longName = new string('a', 130);
        string result = NameCleaner.ToClassName(longName);
        Assert.Equal("A" + new string('a', 129), result);
    }

    [Theory]
    [InlineData("123", "_123")]
    [InlineData("0abc", "_0abc")]
    public void ToPropertyName_Prefixes_Underscore_When_Starts_With_Digit(string input, string expected)
    {
        Assert.Equal(expected, NameCleaner.ToPropertyName(input));
    }

    [Theory]
    [InlineData("")]
    [InlineData("...")]
    [InlineData("_ _ _")]
    public void ToPropertyName_Returns_Unknown_For_Unsanitizable_Input(string input)
    {
        Assert.Equal("Unknown", NameCleaner.ToPropertyName(input));
    }

    [Fact]
    public void SanitizeToPascalCase_Uppercases_After_Each_Separator_Type()
    {
        Assert.Equal("ABCD", NameCleaner.SanitizeToPascalCase("a b-c.d"));
    }

    [Fact]
    public void SanitizeToPascalCase_Preserves_Existing_Casing_Mid_Word()
    {
        // Only the first char after a boundary is capitalized; others are left as-is
        Assert.Equal("MyHTTPClient", NameCleaner.SanitizeToPascalCase("my_hTTPClient"));
    }
}
