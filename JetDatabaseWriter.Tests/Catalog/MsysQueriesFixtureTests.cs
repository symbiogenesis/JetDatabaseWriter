namespace JetDatabaseWriter.Tests.Catalog;

using System;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Fixture-based read tests for <c>MSysQueries</c> rows in the Jackcess
/// <c>queryTestV2010.accdb</c> fixture. Verifies that the reader can
/// decode system-table rows including parameterised query definitions.
/// Covers §4 of <c>docs/design/test-coverage-gaps.md</c>.
/// </summary>
public sealed class MsysQueriesFixtureTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    /// <summary>
    /// MSysQueries Attribute value for parameter rows, per Jackcess
    /// <c>QueryFormat.PARAMETER_ATTRIBUTE</c>.
    /// </summary>
    private const byte ParameterAttribute = 2;

    /// <summary>
    /// The <c>queryTestV2010.accdb</c> fixture contains multiple queries.
    /// <c>MSysQueries</c> must have rows.
    /// </summary>
    [Fact]
    public async Task ReadMSysQueries_ReturnsNonEmptyRowSet()
    {
        AccessReader reader = await db.GetReaderAsync(
            TestDatabases.QueryTestV2010,
            TestContext.Current.CancellationToken);

        DataTable dt = await reader.ReadDataTableAsync(
            "MSysQueries",
            cancellationToken: TestContext.Current.CancellationToken);

        Assert.True(dt.Rows.Count > 0, "MSysQueries should contain at least one row.");
    }

    /// <summary>
    /// The <c>UpdateQuery</c> in the fixture declares
    /// <c>PARAMETERS User Name Text;</c>. There must be at least one row
    /// with <c>Attribute == 2</c> (PARAMETER_ATTRIBUTE) whose <c>Name1</c>
    /// column contains the parameter name.
    /// </summary>
    [Fact]
    public async Task ParameterisedQuery_HasParameterAttributeRows()
    {
        AccessReader reader = await db.GetReaderAsync(
            TestDatabases.QueryTestV2010,
            TestContext.Current.CancellationToken);

        DataTable dt = await reader.ReadDataTableAsync(
            "MSysQueries",
            cancellationToken: TestContext.Current.CancellationToken);

        DataRow[] paramRows = dt.AsEnumerable()
            .Where(r =>
            {
                object attr = r["Attribute"];
                return attr != null && attr is not DBNull &&
                       Convert.ToInt32(attr, CultureInfo.InvariantCulture) == ParameterAttribute;
            })
            .ToArray();

        Assert.NotEmpty(paramRows);

        // At least one parameter row must reference "User Name".
        Assert.Contains(
            paramRows,
            r => r["Name1"]?.ToString()?.Contains("User Name", System.StringComparison.Ordinal) == true);
    }

    /// <summary>
    /// Every query definition must have at least one TYPE row
    /// (<c>Attribute == 0</c>) that identifies the query kind. The fixture
    /// contains Select, Delete, Append, Update, MakeTable, Crosstab, Union,
    /// Passthrough, and DataDefinition queries — at least 9 type rows.
    /// </summary>
    [Fact]
    public async Task AllQueries_HaveTypeAttributeRows()
    {
        AccessReader reader = await db.GetReaderAsync(
            TestDatabases.QueryTestV2010,
            TestContext.Current.CancellationToken);

        DataTable dt = await reader.ReadDataTableAsync(
            "MSysQueries",
            cancellationToken: TestContext.Current.CancellationToken);

        // Attribute == 0 marks TYPE rows (Jackcess TYPE_ATTRIBUTE).
        DataRow[] typeRows = dt.AsEnumerable()
            .Where(r =>
            {
                object attr = r["Attribute"];
                return attr != null && attr is not DBNull &&
                       Convert.ToInt32(attr, CultureInfo.InvariantCulture) == 0;
            })
            .ToArray();

        // The Jackcess fixture has at least 9 distinct query types.
        Assert.True(
            typeRows.Length >= 9,
            $"Expected >= 9 TYPE rows in MSysQueries, got {typeRows.Length}.");
    }
}
