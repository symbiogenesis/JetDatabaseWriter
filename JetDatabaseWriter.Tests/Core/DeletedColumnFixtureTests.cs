namespace JetDatabaseWriter.Tests.Core;

using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using JetDatabaseWriter.Core;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention

/// <summary>
/// Fixture-based tests for databases with deleted columns using the Jackcess
/// <c>delColTestV*.mdb/.accdb</c> fixtures. These databases have table
/// definitions that include deleted-column slots (marked with
/// <c>col_type = 0x00</c> and a null/empty name in the TDEF), which must
/// be skipped during schema enumeration and row decode. A bug here can
/// silently shift column values or crash on fixed-offset miscalculation.
///
/// <para>Jackcess analogue: <c>DatabaseTest.testDeleteColumn</c>.
/// </para>
/// </summary>
public sealed class DeletedColumnFixtureTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    /// <summary>
    /// The fixture lists at least one user table without throwing.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(TestDatabases.DelCol), MemberType = typeof(TestDatabases))]
    public async Task DelCol_ListTables_ReturnsNonEmpty(string path)
    {
        AccessReader reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);

        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        Assert.NotEmpty(tables);
    }

    /// <summary>
    /// Every table reports columns via metadata — deleted-column slots must
    /// be excluded from the public schema.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(TestDatabases.DelCol), MemberType = typeof(TestDatabases))]
    public async Task DelCol_AllTables_HaveVisibleColumns(string path)
    {
        AccessReader reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        foreach (string table in tables)
        {
            List<ColumnMetadata> cols =
                await reader.GetColumnMetadataAsync(table, TestContext.Current.CancellationToken);
            Assert.NotEmpty(cols);
        }
    }

    /// <summary>
    /// No column exposed by the reader has a blank or null name (which would
    /// indicate a deleted-column slot leaking through).
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(TestDatabases.DelCol), MemberType = typeof(TestDatabases))]
    public async Task DelCol_NoColumnHasBlankName(string path)
    {
        AccessReader reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        foreach (string table in tables)
        {
            List<ColumnMetadata> cols =
                await reader.GetColumnMetadataAsync(table, TestContext.Current.CancellationToken);
            foreach (ColumnMetadata col in cols)
            {
                Assert.False(
                    string.IsNullOrWhiteSpace(col.Name),
                    $"Table '{table}' in '{path}' exposed a column with blank/null name (deleted slot leak).");
            }
        }
    }

    /// <summary>
    /// Tables with deleted-column gaps either read successfully (when the
    /// database was compacted after the deletion) or throw
    /// <see cref="JetLimitationException"/> (when stale rows pre-date the
    /// schema change). In either case, the reader must not silently corrupt
    /// data by misaligning columns.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(TestDatabases.DelCol), MemberType = typeof(TestDatabases))]
    public async Task DelCol_RowDecode_SucceedsOrThrowsLimitation(string path)
    {
        AccessReader reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        foreach (string table in tables)
        {
            try
            {
                await reader.ReadDataTableAsync(
                    table, cancellationToken: TestContext.Current.CancellationToken);
            }
            catch (JetLimitationException)
            {
                // Expected for stale rows — confirms the guard fires.
            }
        }
    }

    /// <summary>
    /// For tables that CAN be read (those without stale rows), the DataTable
    /// column count matches the metadata column count — ensures the schema and
    /// data paths agree on deleted-column exclusion.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(TestDatabases.DelCol), MemberType = typeof(TestDatabases))]
    public async Task DelCol_ReadableTables_DataTableColumnCount_MatchesMetadata(string path)
    {
        AccessReader reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        foreach (string table in tables)
        {
            List<ColumnMetadata> meta =
                await reader.GetColumnMetadataAsync(table, TestContext.Current.CancellationToken);

            DataTable dt;
            try
            {
                dt = await reader.ReadDataTableAsync(
                    table, maxRows: 1, cancellationToken: TestContext.Current.CancellationToken);
            }
            catch (JetLimitationException)
            {
                // Stale rows — can't compare column counts but metadata is valid.
                continue;
            }

            Assert.Equal(meta.Count, dt.Columns.Count);
        }
    }
}
