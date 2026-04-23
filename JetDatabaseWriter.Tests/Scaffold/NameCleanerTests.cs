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
}
