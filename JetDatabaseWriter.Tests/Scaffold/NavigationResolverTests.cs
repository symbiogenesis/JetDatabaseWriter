namespace JetDatabaseWriter.Tests.Scaffold;

using System.Collections.Generic;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Scaffold;
using Xunit;

/// <summary>
/// Unit tests for <see cref="NavigationResolver"/> and the navigation-naming helpers:
/// confirms the parent reference / child collection pairing, FK-derived reference
/// names, pluralization rules, and that relationships touching un-scaffolded tables
/// are ignored.
/// </summary>
public sealed class NavigationResolverTests
{
    [Fact]
    public void Resolve_SingleRelationship_AddsChildCollectionAndParentReference()
    {
        var tables = new List<string> { "Customers", "Orders" };
        var relationships = new List<RelationshipMetadata>
        {
            new()
            {
                Name = "FK_Orders_Customers",
                PrimaryTable = "Customers",
                PrimaryColumns = ["Id"],
                ForeignTable = "Orders",
                ForeignColumns = ["CustomerId"],
            },
        };

        Dictionary<string, List<ScaffoldNavigation>> result = NavigationResolver.Resolve(tables, relationships);

        ScaffoldNavigation parentNav = Assert.Single(result["Customers"]);
        Assert.True(parentNav.IsCollection);
        Assert.Equal("Orders", parentNav.TargetClassName);
        Assert.Equal("Orders", parentNav.PreferredName);

        ScaffoldNavigation childNav = Assert.Single(result["Orders"]);
        Assert.False(childNav.IsCollection);
        Assert.Equal("Customers", childNav.TargetClassName);
        Assert.Equal("Customer", childNav.PreferredName);
    }

    [Fact]
    public void Resolve_SkipsRelationshipsTouchingUnscaffoldedTables()
    {
        var tables = new List<string> { "Orders" };
        var relationships = new List<RelationshipMetadata>
        {
            new()
            {
                Name = "FK_Orders_Customers",
                PrimaryTable = "Customers",
                PrimaryColumns = ["Id"],
                ForeignTable = "Orders",
                ForeignColumns = ["CustomerId"],
            },
        };

        Assert.Empty(NavigationResolver.Resolve(tables, relationships));
    }

    [Fact]
    public void Resolve_PluralizesSingularChildClassForCollection()
    {
        var tables = new List<string> { "Customer", "Order" };
        var relationships = new List<RelationshipMetadata>
        {
            new()
            {
                Name = "FK",
                PrimaryTable = "Customer",
                PrimaryColumns = ["Id"],
                ForeignTable = "Order",
                ForeignColumns = ["CustomerId"],
            },
        };

        Dictionary<string, List<ScaffoldNavigation>> result = NavigationResolver.Resolve(tables, relationships);

        ScaffoldNavigation parentNav = Assert.Single(result["Customer"]);
        Assert.Equal("Orders", parentNav.PreferredName);
    }

    [Fact]
    public void Resolve_MultiColumnForeignKey_UsesParentClassNameForReference()
    {
        var tables = new List<string> { "Parent", "Child" };
        var relationships = new List<RelationshipMetadata>
        {
            new()
            {
                Name = "FK",
                PrimaryTable = "Parent",
                PrimaryColumns = ["KeyA", "KeyB"],
                ForeignTable = "Child",
                ForeignColumns = ["RefA", "RefB"],
            },
        };

        Dictionary<string, List<ScaffoldNavigation>> result = NavigationResolver.Resolve(tables, relationships);

        ScaffoldNavigation childNav = Assert.Single(result["Child"]);
        Assert.Equal("Parent", childNav.PreferredName);
    }

    [Theory]
    [InlineData("Order", "Orders")]
    [InlineData("Category", "Categories")]
    [InlineData("Address", "Addresses")]
    [InlineData("Box", "Boxes")]
    [InlineData("Dish", "Dishes")]
    [InlineData("Branch", "Branches")]
    [InlineData("Company", "Companies")]
    public void Pluralize_AppliesEnglishRules(string singular, string plural) =>
        Assert.Equal(plural, NameCleaner.Pluralize(singular));
}
