namespace JetDatabaseWriter.Tests.Scaffold;

using System;
using System.Collections.Generic;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Scaffold;
using Xunit;

/// <summary>
/// Tests that <see cref="EntityEmitter"/> emits reference and collection navigation
/// properties alongside the column properties, including the required
/// <c>System.Collections.Generic</c> using and collection initializer.
/// </summary>
public sealed class EntityEmitterNavigationTests
{
    private static List<ColumnMetadata> Columns(params (string Name, Type Type)[] cols)
    {
        var list = new List<ColumnMetadata>(cols.Length);
        foreach ((string name, Type type) in cols)
        {
            list.Add(new ColumnMetadata { Name = name, ClrType = type, IsNullable = false, TypeName = "Long Integer", Size = ColumnSize.FromBytes(4) });
        }

        return list;
    }

    [Fact]
    public void Emit_ReferenceNavigation_EmitsNullableParentProperty()
    {
        var navigations = new List<ScaffoldNavigation> { new(IsCollection: false, TargetClassName: "Customers", PreferredName: "Customer") };

        string result = EntityEmitter.Emit("Orders", Columns(("Id", typeof(int)), ("CustomerId", typeof(int))), navigations, "NS", useRecords: false, nullable: true);

        Assert.Contains("public Customers? Customer { get; set; }", result, StringComparison.Ordinal);
        Assert.Contains("public int CustomerId { get; set; }", result, StringComparison.Ordinal);
    }

    [Fact]
    public void Emit_CollectionNavigation_EmitsInitializedCollectionAndUsing()
    {
        var navigations = new List<ScaffoldNavigation> { new(IsCollection: true, TargetClassName: "Orders", PreferredName: "Orders") };

        string result = EntityEmitter.Emit("Customers", Columns(("Id", typeof(int))), navigations, "NS", useRecords: false, nullable: true);

        Assert.Contains("using System.Collections.Generic;", result, StringComparison.Ordinal);
        Assert.Contains("public ICollection<Orders> Orders { get; set; } = new List<Orders>();", result, StringComparison.Ordinal);
    }

    [Fact]
    public void Emit_NoNavigations_DoesNotAddCollectionsUsing()
    {
        string result = EntityEmitter.Emit("Orders", Columns(("Id", typeof(int))), [], "NS", useRecords: false, nullable: true);

        Assert.DoesNotContain("System.Collections.Generic", result, StringComparison.Ordinal);
    }

    [Fact]
    public void Emit_ReferenceNavigation_NonNullable_OmitsQuestionMark()
    {
        var navigations = new List<ScaffoldNavigation> { new(IsCollection: false, TargetClassName: "Customers", PreferredName: "Customer") };

        string result = EntityEmitter.Emit("Orders", Columns(("Id", typeof(int))), navigations, "NS", useRecords: false, nullable: false);

        Assert.Contains("public Customers Customer { get; set; }", result, StringComparison.Ordinal);
    }
}
