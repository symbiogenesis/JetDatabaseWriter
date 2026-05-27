namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Fixture-based read tests for Access 2010+ calculated (expression) columns
/// using <c>calcFieldTestV2010.accdb</c> (Jackcess <c>CalcFieldTest</c>).
/// Verifies that <see cref="ColumnMetadata.IsCalculated"/>,
/// <see cref="ColumnMetadata.CalculationExpression"/>, and
/// <see cref="ColumnMetadata.CalculatedResultType"/> are populated
/// correctly, and that the cached result values decode without error.
/// </summary>
/// <param name="db">The database input.</param>
public sealed class CalculatedColumnFixtureTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    /// <summary>
    /// Table1 contains at least one column where <see cref="ColumnMetadata.IsCalculated"/>
    /// is <see langword="true"/>.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task Table1_HasCalculatedColumns()
    {
        var reader = await db.GetReaderAsync(
            TestDatabases.CalcFieldTestV2010,
            TestContext.Current.CancellationToken);

        IReadOnlyList<ColumnMetadata> meta = await reader.GetColumnMetadataAsync(
            "Table1",
            TestContext.Current.CancellationToken);

        Assert.Contains(meta, c => c.IsCalculated);
    }

    /// <summary>
    /// <c>LastFirst</c> is a calculated text column whose expression
    /// references two non-calculated columns (<c>LastName</c> and
    /// <c>FirstName</c>). The expression text and result type must be
    /// present.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task LastFirst_HasExpression_ReferencingNonCalcColumns()
    {
        var reader = await db.GetReaderAsync(
            TestDatabases.CalcFieldTestV2010,
            TestContext.Current.CancellationToken);

        IReadOnlyList<ColumnMetadata> meta = await reader.GetColumnMetadataAsync(
            "Table1",
            TestContext.Current.CancellationToken);

        var lastFirst = Assert.Single(meta, c => c.Name == "LastFirst");
        Assert.True(lastFirst.IsCalculated);
        Assert.NotNull(lastFirst.CalculationExpression);
        Assert.Contains("LastName", lastFirst.CalculationExpression, StringComparison.Ordinal);
        Assert.Contains("FirstName", lastFirst.CalculationExpression, StringComparison.Ordinal);
        Assert.True(lastFirst.CalculatedResultType > 0, "CalculatedResultType should be a non-zero JET type code.");
    }

    /// <summary>
    /// <c>LastFirstLen</c> is a calculated column whose expression
    /// references <c>LastFirst</c>, which is itself calculated. This covers
    /// the §2.3 gap (calculated-column expressions that reference another
    /// calculated column).
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task LastFirstLen_ReferencesAnotherCalculatedColumn()
    {
        var reader = await db.GetReaderAsync(
            TestDatabases.CalcFieldTestV2010,
            TestContext.Current.CancellationToken);

        IReadOnlyList<ColumnMetadata> meta = await reader.GetColumnMetadataAsync(
            "Table1",
            TestContext.Current.CancellationToken);

        var lastFirstLen = Assert.Single(meta, c => c.Name == "LastFirstLen");
        Assert.True(lastFirstLen.IsCalculated);
        Assert.NotNull(lastFirstLen.CalculationExpression);
        Assert.Contains("LastFirst", lastFirstLen.CalculationExpression, StringComparison.Ordinal);

        // Confirm the referenced column is itself calculated.
        var lastFirst = Assert.Single(meta, c => c.Name == "LastFirst");
        Assert.True(lastFirst.IsCalculated);
    }

    /// <summary>
    /// Boolean, numeric, and text calculated columns all have non-null,
    /// non-empty expressions and non-zero result types.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task AllCalculatedColumns_HaveExpressionAndResultType()
    {
        var reader = await db.GetReaderAsync(
            TestDatabases.CalcFieldTestV2010,
            TestContext.Current.CancellationToken);

        IReadOnlyList<ColumnMetadata> meta = await reader.GetColumnMetadataAsync(
            "Table1",
            TestContext.Current.CancellationToken);

        var calcCols = meta.Where(c => c.IsCalculated);
        Assert.NotEmpty(calcCols);

        foreach (var col in calcCols)
        {
            Assert.False(
                string.IsNullOrEmpty(col.CalculationExpression),
                $"Calculated column '{col.Name}' should have a non-empty CalculationExpression.");
            Assert.True(
                col.CalculatedResultType > 0,
                $"Calculated column '{col.Name}' should have a non-zero CalculatedResultType.");
        }
    }

    /// <summary>
    /// The fixture has 4 data rows (Bruce Wayne, Bart Simpson, John Doe,
    /// Test User). All rows must be readable without throwing, and the reader
    /// unwraps calculated-column cached values into their logical CLR types.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task Table1_ReadDataTable_DecodesAllRows()
    {
        var reader = await db.GetReaderAsync(
            TestDatabases.CalcFieldTestV2010,
            TestContext.Current.CancellationToken);

        var dt = await reader.ReadDataTableAsync(
            "Table1",
            cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(4, dt.Rows.Count);

        // Non-calc columns should decode normally.
        var firstNames = dt.AsEnumerable()
            .Select(r => r["FirstName"]?.ToString())
            .Where(v => !string.IsNullOrEmpty(v))
            .OrderBy(v => v)
            .ToList();

        Assert.Contains("Bruce", firstNames);
        Assert.Contains("Bart", firstNames);
        Assert.Contains("John", firstNames);
        Assert.Contains("Test", firstNames);

        var lastFirstColumn = Assert.Single(dt.Columns.Cast<DataColumn>(), c => c.ColumnName == "LastFirst");
        Assert.Equal(typeof(string), lastFirstColumn.DataType);

        var lastFirstValues = dt.AsEnumerable()
            .Select(r => r["LastFirst"]?.ToString())
            .Where(v => !string.IsNullOrEmpty(v))
            .ToList();

        Assert.Contains(lastFirstValues, v => v!.Contains("Wayne", StringComparison.Ordinal));
    }

    /// <summary>
    /// The calculated column <c>IsRich</c> is present in the metadata, and its
    /// cached result values decode as nulls or the descriptor's CLR type rather
    /// than the raw 23-byte calculated-value envelope.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task IsRich_IsReportedAsCalculatedAndDecodesTypedValues()
    {
        var reader = await db.GetReaderAsync(
            TestDatabases.CalcFieldTestV2010,
            TestContext.Current.CancellationToken);

        IReadOnlyList<ColumnMetadata> meta = await reader.GetColumnMetadataAsync(
            "Table1",
            TestContext.Current.CancellationToken);

        var isRich = Assert.Single(meta, c => c.Name == "IsRich");
        Assert.True(isRich.IsCalculated);
        Assert.NotNull(isRich.CalculationExpression);
        Assert.True(isRich.CalculatedResultType > 0);

        var dt = await reader.ReadDataTableAsync(
            "Table1",
            cancellationToken: TestContext.Current.CancellationToken);

        var isRichColumn = Assert.Single(dt.Columns.Cast<DataColumn>(), c => c.ColumnName == "IsRich");
        Assert.Equal(isRich.ClrType, isRichColumn.DataType);
        Assert.NotEqual(typeof(byte[]), isRichColumn.DataType);
        Assert.All(
            dt.AsEnumerable(),
            row => Assert.True(
                row["IsRich"] is DBNull || row["IsRich"].GetType() == isRich.ClrType,
                $"Expected DBNull or {isRich.ClrType}; got {row["IsRich"].GetType()}"));
    }
}
