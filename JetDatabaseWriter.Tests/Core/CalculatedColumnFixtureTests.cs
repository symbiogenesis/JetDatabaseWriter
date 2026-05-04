namespace JetDatabaseWriter.Tests.Core;

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention

/// <summary>
/// Fixture-based read tests for Access 2010+ calculated (expression) columns
/// using <c>calcFieldTestV2010.accdb</c> (Jackcess <c>CalcFieldTest</c>).
/// Verifies that <see cref="ColumnMetadata.IsCalculated"/>,
/// <see cref="ColumnMetadata.CalculationExpression"/>, and
/// <see cref="ColumnMetadata.CalculatedResultType"/> are populated
/// correctly, and that the cached result values decode without error.
/// Covers §2.3 of <c>docs/design/test-coverage-gaps.md</c>.
/// </summary>
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
        AccessReader reader = await db.GetReaderAsync(
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
        AccessReader reader = await db.GetReaderAsync(
            TestDatabases.CalcFieldTestV2010,
            TestContext.Current.CancellationToken);

        IReadOnlyList<ColumnMetadata> meta = await reader.GetColumnMetadataAsync(
            "Table1",
            TestContext.Current.CancellationToken);

        ColumnMetadata lastFirst = Assert.Single(meta, c => c.Name == "LastFirst");
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
        AccessReader reader = await db.GetReaderAsync(
            TestDatabases.CalcFieldTestV2010,
            TestContext.Current.CancellationToken);

        IReadOnlyList<ColumnMetadata> meta = await reader.GetColumnMetadataAsync(
            "Table1",
            TestContext.Current.CancellationToken);

        ColumnMetadata lastFirstLen = Assert.Single(meta, c => c.Name == "LastFirstLen");
        Assert.True(lastFirstLen.IsCalculated);
        Assert.NotNull(lastFirstLen.CalculationExpression);
        Assert.Contains("LastFirst", lastFirstLen.CalculationExpression, StringComparison.Ordinal);

        // Confirm the referenced column is itself calculated.
        ColumnMetadata lastFirst = Assert.Single(meta, c => c.Name == "LastFirst");
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
        AccessReader reader = await db.GetReaderAsync(
            TestDatabases.CalcFieldTestV2010,
            TestContext.Current.CancellationToken);

        IReadOnlyList<ColumnMetadata> meta = await reader.GetColumnMetadataAsync(
            "Table1",
            TestContext.Current.CancellationToken);

        IEnumerable<ColumnMetadata> calcCols = meta.Where(c => c.IsCalculated);
        Assert.NotEmpty(calcCols);

        foreach (ColumnMetadata col in calcCols)
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
    /// Test User). All rows must be readable without throwing. The reader
    /// surfaces calculated-column values as opaque wrapped bytes (the
    /// 23-byte envelope is preserved); this test verifies the decode path
    /// completes for every row.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task Table1_ReadDataTable_DecodesAllRows()
    {
        AccessReader reader = await db.GetReaderAsync(
            TestDatabases.CalcFieldTestV2010,
            TestContext.Current.CancellationToken);

        DataTable dt = await reader.ReadDataTableAsync(
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
    }

    /// <summary>
    /// The boolean calculated column <c>IsRich</c> is present in the
    /// metadata as a calculated column. The cached result values are
    /// wrapped in a 23-byte envelope (opaque); this test verifies the
    /// column metadata rather than the decoded value.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task IsRich_IsReportedAsCalculatedBoolean()
    {
        AccessReader reader = await db.GetReaderAsync(
            TestDatabases.CalcFieldTestV2010,
            TestContext.Current.CancellationToken);

        IReadOnlyList<ColumnMetadata> meta = await reader.GetColumnMetadataAsync(
            "Table1",
            TestContext.Current.CancellationToken);

        ColumnMetadata isRich = Assert.Single(meta, c => c.Name == "IsRich");
        Assert.True(isRich.IsCalculated);
        Assert.NotNull(isRich.CalculationExpression);
        Assert.True(isRich.CalculatedResultType > 0);
    }
}
