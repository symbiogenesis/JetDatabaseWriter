namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Expressions;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// The lexer that finds and renames field references in Access expression
/// text: what counts as a reference, what is copied verbatim, and that a
/// renamed Access-authored expression evaluates exactly like the original.
/// </summary>
/// <param name="db">The database input.</param>
public sealed class ExpressionFieldReferencesTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    /// <summary>Gets the rename cases: expression, old name, new name, table name, expected text.</summary>
    public static TheoryData<string, string, string, string, string> RenameCases => new()
    {
        { "[Price]*[Qty]", "Price", "UnitPrice", "T", "[UnitPrice]*[Qty]" },
        { "[price] *  [Qty]", "Price", "UnitPrice", "T", "[UnitPrice] *  [Qty]" },
        { "\"[Price] is \" & [Price]", "Price", "UnitPrice", "T", "\"[Price] is \" & [UnitPrice]" },
        { "'[Price]' & [Price]", "Price", "UnitPrice", "T", "'[Price]' & [UnitPrice]" },
        { "'it''s ' & Price", "Price", "Cost", "T", "'it''s ' & [Cost]" },
        { "\"say \"\"[Price]\"\"\" & Price", "Price", "Cost", "T", "\"say \"\"[Price]\"\"\" & [Cost]" },
        { "Price + 1", "Price", "Unit Price", "T", "[Unit Price] + 1" },
        { "Price(1) + Price", "Price", "Cost", "T", "Price(1) + [Cost]" },
        { "Price (1) + Not Price", "Price", "Cost", "T", "Price (1) + Not [Cost]" },
        { "[PriceList] + [Price] + PriceX", "Price", "Cost", "T", "[PriceList] + [Cost] + PriceX" },
        { "#2020-01-01# + [Price]", "Price", "Cost", "T", "#2020-01-01# + [Cost]" },
        { "#[Price]# + Price", "Price", "Cost", "T", "#[Price]# + [Cost]" },
        { "[T].[Price] + [Other].[Price] + Forms![F]![Price] + T.Price", "Price", "Cost", "T", "[T].[Cost] + [Other].[Price] + Forms![F]![Price] + T.[Cost]" },
        { "[Price]![Total] + Total", "Total", "Sum", "T", "[Price]![Total] + [Sum]" },
        { "1E5 + [Price] + &HFF + 2.5e-3*Price", "Price", "Cost", "T", "1E5 + [Cost] + &HFF + 2.5e-3*[Cost]" },
        { "[A]&HFF", "HFF", "Hex", "T", "[A]&[Hex]" },
        { "&HFF + HFF & &O17 + &17&", "HFF", "Hex", "T", "&HFF + [Hex] & &O17 + &17&" },
        { "Len([Code]) = 3", "Code", "Sku", "T", "Len([Sku]) = 3" },
        { "IIf([Price] Is Null, 0, [Price])", "Price", "Cost", "T", "IIf([Cost] Is Null, 0, [Cost])" },
        { "[Price] & \"abc", "Price", "Cost", "T", "[Cost] & \"abc" },
        { "\"abc & [Price]", "Price", "Cost", "T", "\"abc & [Price]" },
        { "[Price", "Price", "Cost", "T", "[Price" },
        { "Yes + [Yes] + Mod", "Yes", "Flag", "T", "Yes + [Flag] + Mod" },
        { "vbTrue + [vbTrue]", "vbTrue", "Flag", "T", "vbTrue + [Flag]" },
        { "=[Unit Price]*2", "Unit Price", "Cost", "T", "=[Cost]*2" },
        { "Left$(Name, 2) & Name$", "Name", "Title", "T", "Left$([Title], 2) & Name$" },
        { "([OrderDetails].[OrderID]=7)", "OrderID", "OrderNo", "OrderDetails", "([OrderDetails].[OrderNo]=7)" },
        { "{guid {6F9619FF-8B86-D011-B42D-00C04FC964FF}}", "guid", "Tag", "T", "{guid {6F9619FF-8B86-D011-B42D-00C04FC964FF}}" },
        { "{guid {6F9619FF-8B86-D011-B42D-00C04FC964FF}} & guid", "guid", "Tag", "T", "{guid {6F9619FF-8B86-D011-B42D-00C04FC964FF}} & [Tag]" },
        { "Between 1 And Price", "Price", "Cost", "T", "Between 1 And [Cost]" },
    };

    [Theory]
    [MemberData(nameof(RenameCases))]
    public void Rename_RewritesOnlyFieldReferences(string expression, string oldName, string newName, string tableName, string expected)
        => Assert.Equal(expected, ExpressionFieldReferences.Rename(expression, oldName, newName, tableName));

    [Theory]
    [MemberData(nameof(RenameCases))]
    public void References_MatchesRenameHits(string expression, string oldName, string newName, string tableName, string expected)
    {
        bool references = ExpressionFieldReferences.References(expression, oldName, tableName);

        Assert.Equal(!string.Equals(expected, expression, StringComparison.Ordinal), references);
        Assert.Equal(references, !ReferenceEquals(expression, ExpressionFieldReferences.Rename(expression, oldName, newName, tableName)));
    }

    [Fact]
    public void Rename_NoReference_ReturnsSameInstance()
    {
        const string expression = "[Qty] * 2 & \"[Price]\"";

        Assert.Same(expression, ExpressionFieldReferences.Rename(expression, "Price", "Cost", "T"));
    }

    [Fact]
    public void Rename_NullOrEmptyExpression_ReturnsIt()
    {
        Assert.Null(ExpressionFieldReferences.Rename(null, "Price", "Cost", "T"));
        Assert.Equal(string.Empty, ExpressionFieldReferences.Rename(string.Empty, "Price", "Cost", "T"));
        Assert.False(ExpressionFieldReferences.References(null, "Price", "T"));
    }

    [Fact]
    public void Rename_NewNameWithClosingBracket_ThrowsOnlyWhenReferenced()
    {
        Assert.Equal("[Qty] * 2", ExpressionFieldReferences.Rename("[Qty] * 2", "Price", "Co]st", "T"));

        ArgumentException exception = Assert.Throws<ArgumentException>(() => ExpressionFieldReferences.Rename("[Price] * 2", "Price", "Co]st", "T"));
        Assert.Contains("Co]st", exception.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// Renames Salary to "Base Pay" and LastFirst to "Full Name" in every
    /// calculated expression of the Access-authored calcFieldTestV2010 table,
    /// and checks the text and that each renamed expression evaluates, over
    /// every row Access stored, to exactly what the original evaluates to.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task Rename_AccessAuthoredExpressions_EvaluateLikeTheOriginal()
    {
        AccessReader reader = await db.GetReaderAsync(TestDatabases.CalcFieldTestV2010, TestContext.Current.CancellationToken);
        IReadOnlyList<ColumnMetadata> meta = await reader.GetColumnMetadataAsync("Table1", TestContext.Current.CancellationToken);
        DataTable rows = await reader.ReadDataTableAsync("Table1", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(11, meta.Count(c => c.IsCalculated));

        var renames = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["Salary"] = "Base Pay",
            ["LastFirst"] = "Full Name",
        };

        var originalDef = new TableDef();
        var renamedDef = new TableDef();
        var original = new List<ColumnConstraint>();
        var renamed = new List<ColumnConstraint>();
        var renamedExpressions = new Dictionary<string, string?>(StringComparer.Ordinal);
        foreach (ColumnMetadata column in meta)
        {
            string? expression = column.CalculationExpression;
            foreach (KeyValuePair<string, string> rename in renames)
            {
                expression = ExpressionFieldReferences.Rename(expression, rename.Key, rename.Value, "Table1");
            }

            string name = renames.TryGetValue(column.Name, out string? newName) ? newName : column.Name;
            originalDef.Columns.Add(new ColumnInfo { Name = column.Name });
            renamedDef.Columns.Add(new ColumnInfo { Name = name });
            original.Add(new ColumnConstraint { Name = column.Name, ClrType = column.ClrType, IsCalculated = column.IsCalculated, CalculationExpression = column.CalculationExpression });
            renamed.Add(new ColumnConstraint { Name = name, ClrType = column.ClrType, IsCalculated = column.IsCalculated, CalculationExpression = expression });
            renamedExpressions[column.Name] = expression;
        }

        Assert.Equal("[LastName] & \", \" & [FirstName]", renamedExpressions["LastFirst"]);
        Assert.Equal("Len([Full Name])", renamedExpressions["LastFirstLen"]);
        Assert.Equal("[Base Pay]/12", renamedExpressions["MonthlySalary"]);
        Assert.Equal("[Base Pay]>100000", renamedExpressions["IsRich"]);
        Assert.Equal("[LastName] & \", \" & [FirstName] & \"=\" & [Full Name]", renamedExpressions["AllNames"]);
        Assert.Equal("[Base Pay]/52", renamedExpressions["WeeklySalary"]);
        Assert.Equal("[Base Pay]", renamedExpressions["SalaryTest"]);
        Assert.Equal("True", renamedExpressions["BoolTest"]);
        Assert.Equal("[Popularity]", renamedExpressions["DecimalTest"]);
        Assert.Equal("[Base Pay]*0.13/[DecimalTest]", renamedExpressions["FloatTest"]);
        Assert.Equal("([Base Pay]*[MonthlySalary]/[DecimalTest])*34.12342134", renamedExpressions["BigNumTest"]);

        Assert.Equal(4, rows.Rows.Count);
        foreach (DataRow row in rows.Rows)
        {
            object[] before = [.. meta.Select(c => c.IsCalculated ? DBNull.Value : row[c.Name])];
            object[] after = [.. before];
            CalculatedExpressionEvaluator.Apply(originalDef, original, before, force: false);
            CalculatedExpressionEvaluator.Apply(renamedDef, renamed, after, force: false);
            Assert.Equal(before, after);
        }
    }
}
