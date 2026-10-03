namespace JetDatabaseWriter.Tests.Schema;

using System;
using JetDatabaseWriter.Schema;
using Xunit;

/// <summary>
/// The Access object-name rules in <see cref="AccessObjectName"/>: 1 to 64
/// characters, not only white space, no leading space, and none of
/// <c>. ! ` [ ]</c> or a control character (U+0000 to U+001F and U+007F).
/// </summary>
public sealed class AccessObjectNameTests
{
    /// <summary>Gets names Access allows, including the edges of each rule.</summary>
    /// <returns>The names.</returns>
    public static TheoryData<string> ValidNames() =>
    [
        "Orders",
        "Order Details",
        "=Eq",
        "a'b",
        "a\"b",
        "~tmp_1",
        "#Col",
        "Café",
        "顧客",
        "x ",
        "Trail ",
        " x",
        "a\u0080b",
        "😀",
        new string('x', 64),
    ];

    /// <summary>Gets names Access does not allow, with a fragment of the reason each one gives.</summary>
    /// <returns>The names and reasons.</returns>
    public static TheoryData<string, string> InvalidNames() => new()
    {
        { string.Empty, "it is empty" },
        { " ", "only white space" },
        { "   ", "only white space" },
        { "\t", "only white space" },
        { "　", "only white space" },
        { " Lead", "starts with a space" },
        { "a.b", "contains '.'" },
        { "a!b", "contains '!'" },
        { "a`b", "contains '`'" },
        { "a[b", "contains '['" },
        { "a]b", "contains ']'" },
        { "a\tb", "control character U+0009" },
        { "a\u0001b", "control character U+0001" },
        { "a\nb", "control character U+000A" },
        { "\u0000", "control character U+0000" },
        { "a\u001Fb", "control character U+001F" },
        { "a\u007Fb", "control character U+007F" },
        { new string('x', 65), "65 characters long; the limit is 64" },
        { " " + new string('x', 70), "starts with a space" },
    };

    [Theory]
    [MemberData(nameof(ValidNames))]
    public void FindViolation_ValidNames_ReturnsNull(string name) =>
        Assert.Null(AccessObjectName.FindViolation(name));

    [Theory]
    [MemberData(nameof(InvalidNames))]
    public void FindViolation_InvalidNames_ReturnsReason(string name, string reason) =>
        Assert.Contains(reason, AccessObjectName.FindViolation(name), StringComparison.Ordinal);

    [Fact]
    public void ThrowIfInvalid_Null_ThrowsArgumentNullExceptionWithParamName()
    {
        ArgumentNullException ex = Assert.Throws<ArgumentNullException>(() => AccessObjectName.ThrowIfInvalid(null, "tableName", "table"));
        Assert.Equal("tableName", ex.ParamName);
    }

    [Fact]
    public void ThrowIfInvalid_Empty_ThrowsArgumentException()
    {
        ArgumentException ex = Assert.Throws<ArgumentException>(() => AccessObjectName.ThrowIfInvalid(string.Empty, "tableName", "table"));
        Assert.Equal("tableName", ex.ParamName);
    }

    [Fact]
    public void ThrowIfInvalid_ValidName_DoesNotThrow() =>
        AccessObjectName.ThrowIfInvalid("Order Details", "tableName", "table");

    [Fact]
    public void ThrowIfInvalid_Violation_MessageNamesRuleAndEscapesControlChars()
    {
        ArgumentException ex = Assert.Throws<ArgumentException>(() => AccessObjectName.ThrowIfInvalid("a\u0001b", "newColumnName", "column"));

        Assert.Equal("newColumnName", ex.ParamName);
        Assert.Contains("The column name 'a\\u0001b' is not valid: it contains the control character U+0001.", ex.Message, StringComparison.Ordinal);
        Assert.Contains("1 to 64 characters", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("\u0001", ex.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public void ThrowIfInvalidMember_MissingName_ThrowsArgumentExceptionNamingPosition(string? name)
    {
        ArgumentException ex = Assert.Throws<ArgumentException>(() => AccessObjectName.ThrowIfInvalidMember(name, "columns", "column", 2));

        Assert.Equal("columns", ex.ParamName);
        Assert.Equal("The column at position 2 has no name. (Parameter 'columns')", ex.Message);
    }

    [Fact]
    public void ThrowIfInvalidMember_Violation_MessageNamesKindAndPosition()
    {
        ArgumentException ex = Assert.Throws<ArgumentException>(() => AccessObjectName.ThrowIfInvalidMember("ix.Name", "indexes", "index", 0));

        Assert.Equal("indexes", ex.ParamName);
        Assert.StartsWith("The index name 'ix.Name' at position 0 is not valid: it contains '.'.", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ThrowIfInvalidMember_SingleDefinition_OmitsPosition()
    {
        ArgumentException missing = Assert.Throws<ArgumentException>(() => AccessObjectName.ThrowIfInvalidMember(null, "column", "column"));
        Assert.StartsWith("The column has no name.", missing.Message, StringComparison.Ordinal);

        ArgumentException invalid = Assert.Throws<ArgumentException>(() => AccessObjectName.ThrowIfInvalidMember(" x", "column", "column"));
        Assert.StartsWith("The column name ' x' is not valid: it starts with a space.", invalid.Message, StringComparison.Ordinal);
    }
}
