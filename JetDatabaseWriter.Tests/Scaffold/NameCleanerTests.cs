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
    [InlineData("first_name", "FirstName")]
    [InlineData("Last Name", "LastName")]
    [InlineData("email-address", "EmailAddress")]
    [InlineData("company.name", "CompanyName")]
    [InlineData("some_column_name", "SomeColumnName")]
    [InlineData("café_table", "CaféTable")]
    [InlineData("my_hTTPClient", "MyHTTPClient")]
    public void Names_ConvertToPascalCase_PreservingWordCasing(string input, string expected)
    {
        Assert.Equal(expected, NameCleaner.ToClassName(input));
        Assert.Equal(expected, NameCleaner.ToPropertyName(input));
    }

    [Theory]
    [InlineData("123_table", "_123Table")]
    [InlineData("4column", "_4column")]
    [InlineData("123", "_123")]
    [InlineData("0abc", "_0abc")]
    public void Names_PrefixUnderscoreWhenStartingWithDigit(string input, string expected)
    {
        Assert.Equal(expected, NameCleaner.ToClassName(input));
        Assert.Equal(expected, NameCleaner.ToPropertyName(input));
    }

    [Theory]
    [InlineData("class", "Class")]
    [InlineData("int", "Int")]
    [InlineData("string", "String")]
    [InlineData("return", "Return")]
    public void Names_PascalCaseKeywords(string input, string expected)
    {
        Assert.Equal(expected, NameCleaner.ToClassName(input));
        Assert.Equal(expected, NameCleaner.ToPropertyName(input));
    }

    [Theory]
    [InlineData("Hello!@#$%123", "Hello123")]
    [InlineData("ab___cd", "AbCd")]
    [InlineData("a_b-c.def", "ABCDef")]
    [InlineData("a b-c.d", "ABCD")]
    [InlineData("_table", "Table")]
    [InlineData("__double", "Double")]
    [InlineData("-leading-dash", "LeadingDash")]
    [InlineData("table_", "Table")]
    [InlineData("name-", "Name")]
    [InlineData("end.", "End")]
    public void Names_StripInvalidCharactersAndNormalizeSeparators(string input, string expected)
    {
        Assert.Equal(expected, NameCleaner.ToClassName(input));
        Assert.Equal(expected, NameCleaner.ToPropertyName(input));
    }

    [Theory]
    [InlineData("")]
    [InlineData("!!!")]
    [InlineData("@#$")]
    [InlineData("...")]
    [InlineData("_ _ _")]
    public void Names_ReturnUnknownForUnsanitizableInput(string input)
    {
        Assert.Equal("Unknown", NameCleaner.ToClassName(input));
        Assert.Equal("Unknown", NameCleaner.ToPropertyName(input));
    }

    [Theory]
    [InlineData("a", "A")]
    [InlineData("Z", "Z")]
    [InlineData("5", "_5")]
    public void Names_HandleSingleCharacterInput(string input, string expected)
    {
        Assert.Equal(expected, NameCleaner.ToClassName(input));
        Assert.Equal(expected, NameCleaner.ToPropertyName(input));
    }

    [Fact]
    public void Names_LongInput_UsesHeapAllocation()
    {
        // Names >= 128 chars exercise the heap-allocated array path.
        string input = new('a', 130);
        string expected = "A" + new string('a', 129);

        Assert.Equal(expected, NameCleaner.ToClassName(input));
        Assert.Equal(expected, NameCleaner.ToPropertyName(input));
    }
}
