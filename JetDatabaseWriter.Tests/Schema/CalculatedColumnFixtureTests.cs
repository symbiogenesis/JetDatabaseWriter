namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Expressions;
using JetDatabaseWriter.Schema.Models;
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

        foreach (ColumnMetadata? col in calcCols)
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

        DataColumn lastFirstColumn = Assert.Single(dt.Columns.Cast<DataColumn>(), c => c.ColumnName == "LastFirst");
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

        DataTable dt = await reader.ReadDataTableAsync(
            "Table1",
            cancellationToken: TestContext.Current.CancellationToken);

        DataColumn isRichColumn = Assert.Single(dt.Columns.Cast<DataColumn>(), c => c.ColumnName == "IsRich");
        Assert.Equal(isRich.ClrType, isRichColumn.DataType);
        Assert.NotEqual(typeof(byte[]), isRichColumn.DataType);
        Assert.All(
            dt.AsEnumerable(),
            row => Assert.True(
                row["IsRich"] is DBNull || row["IsRich"].GetType() == isRich.ClrType,
                $"Expected DBNull or {isRich.ClrType}; got {row["IsRich"].GetType()}"));
    }

    /// <summary>
    /// <c>AllNames</c> has a Text column descriptor (size 0) but a Memo
    /// <c>ResultType</c>, and Access stores its cached value as a long value: a
    /// 12-byte LVAL header in the row (single-page LVAL rows for three rows,
    /// inline for John Doe) whose payload is the calculated-value wrapper around
    /// the UCS-2 text. Every read path decodes it by the result type.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task AllNames_CalculatedMemo_DecodesAccessCachedText()
    {
        string[] expected =
        [
            "Doe, John=Doe, John",
            "Simpson, Bart=Simpson, Bart",
            "User, Test=User, Test",
            "Wayne, Bruce=Wayne, Bruce",
        ];

        AccessReader reader = await db.GetReaderAsync(TestDatabases.CalcFieldTestV2010, TestContext.Current.CancellationToken);

        DataTable typed = await reader.ReadDataTableAsync("Table1", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(expected, typed.AsEnumerable().Select(r => (string)r["AllNames"]).Order(StringComparer.Ordinal));

        DataTable strings = await reader.ReadTableAsStringsAsync("Table1", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(expected, strings.AsEnumerable().Select(r => (string)r["AllNames"]).Order(StringComparer.Ordinal));

        var mapped = new List<string?>();
        await foreach (AllNamesRow row in reader.Rows<AllNamesRow>("Table1", cancellationToken: TestContext.Current.CancellationToken))
        {
            mapped.Add(row.AllNames);
        }

        Assert.Equal(expected, mapped.Order(StringComparer.Ordinal));
    }

    /// <summary>
    /// Every Access-authored calculated expression in the fixtures parses with
    /// the Access-precedence engine, and re-evaluating it against each stored
    /// row reproduces the value Access cached in the file.
    /// </summary>
    /// <param name="path">The fixture database path.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(CalculatedFixtures))]
    public async Task AccessAuthoredExpressions_ReevaluateToCachedValues(string path)
    {
        AccessReader reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);

        IReadOnlyList<ColumnMetadata> meta = await reader.GetColumnMetadataAsync("Table1", TestContext.Current.CancellationToken);
        Assert.Contains(meta, c => c.IsCalculated);

        var tableDef = new TableDef { Columns = [.. meta.Select(column => new ColumnInfo { Name = column.Name })] };
        var constraints = new List<ColumnConstraint>(meta.Count);
        foreach (ColumnMetadata column in meta)
        {
            constraints.Add(new ColumnConstraint
            {
                Name = column.Name,
                ClrType = column.ClrType,
                IsCalculated = column.IsCalculated,
                CalculationExpression = column.CalculationExpression,
            });
        }

        DataTable dt = await reader.ReadDataTableAsync("Table1", cancellationToken: TestContext.Current.CancellationToken);
        Assert.NotEqual(0, dt.Rows.Count);

        int compared = 0;
        var mismatches = new List<string>();
        foreach (DataRow row in dt.Rows)
        {
            object[] values = new object[meta.Count];
            for (int i = 0; i < meta.Count; i++)
            {
                values[i] = meta[i].IsCalculated ? DBNull.Value : row[meta[i].Name];
            }

            CalculatedExpressionEvaluator.Apply(tableDef, constraints, values, force: false);

            for (int i = 0; i < meta.Count; i++)
            {
                if (!meta[i].IsCalculated)
                {
                    continue;
                }

                object stored = row[meta[i].Name];
                object computed = values[i];
                string label = $"{meta[i].Name} = {meta[i].CalculationExpression}";
                if (stored is DBNull || computed is DBNull)
                {
                    if (!(stored is DBNull && computed is DBNull))
                    {
                        mismatches.Add($"{label}: stored {stored}, computed {computed}");
                    }
                }
                else if (stored is float or double or decimal)
                {
                    double expected = Convert.ToDouble(stored, CultureInfo.InvariantCulture);
                    double actual = Convert.ToDouble(computed, CultureInfo.InvariantCulture);
                    if (Math.Abs(expected - actual) > Math.Max(1e-4, Math.Abs(expected) * 1e-6))
                    {
                        mismatches.Add($"{label}: stored {expected}, computed {actual}");
                    }
                }
                else if (!Equals(stored, computed))
                {
                    mismatches.Add($"{label}: stored {stored} ({stored.GetType().Name}), computed {computed}");
                }

                compared++;
            }
        }

        Assert.NotEqual(0, compared);
        Assert.True(mismatches.Count == 0, string.Join(Environment.NewLine, mismatches));
    }

    /// <summary>Gets the Access-authored fixtures that carry calculated columns.</summary>
    public static TheoryData<string> CalculatedFixtures => new(TestDatabases.CalcFieldTestV2010, TestDatabases.ExtDateTestV2019);

    private sealed class AllNamesRow
    {
        public string? AllNames { get; set; }
    }
}
