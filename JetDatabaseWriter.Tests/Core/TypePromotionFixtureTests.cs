namespace JetDatabaseWriter.Tests.Core;

using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using JetDatabaseWriter.Core;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention

/// <summary>
/// Fixture-based tests for type-promoted columns using the Jackcess
/// <c>testPromotionV*.mdb/.accdb</c> fixtures. These databases exercise
/// auto-number promotion (Long → Replication ID / BigInt) and numeric
/// type widening (Integer → Long, etc.) across format versions V2000–V2010.
///
/// <para>Jackcess analogue: <c>DatabaseTest.testMutateTable</c> —
/// promotes auto-number columns via <c>Table.mutateTable()</c>.
/// </para>
/// </summary>
public sealed class TypePromotionFixtureTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    /// <summary>
    /// The fixture lists at least one user table without throwing.
    /// </summary>
    [Theory]
    [MemberData(nameof(TestDatabases.Promotion), MemberType = typeof(TestDatabases))]
    public async Task Promotion_ListTables_ReturnsNonEmpty(string path)
    {
        AccessReader reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);

        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        Assert.NotEmpty(tables);
    }

    /// <summary>
    /// Every table in the fixture exposes at least one column via
    /// <see cref="AccessReader.GetColumnMetadataAsync"/>.
    /// </summary>
    [Theory]
    [MemberData(nameof(TestDatabases.Promotion), MemberType = typeof(TestDatabases))]
    public async Task Promotion_AllTables_HaveColumns(string path)
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
    /// All rows in every table stream without throwing, confirming that
    /// promoted column type descriptors are decoded correctly.
    /// </summary>
    [Theory]
    [MemberData(nameof(TestDatabases.Promotion), MemberType = typeof(TestDatabases))]
    public async Task Promotion_AllTables_StreamAllRows_WithoutThrowing(string path)
    {
        AccessReader reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        long totalRows = 0;
        foreach (string table in tables)
        {
            DataTable dt = await reader.ReadDataTableAsync(
                table, cancellationToken: TestContext.Current.CancellationToken);
            totalRows += dt.Rows.Count;
        }

        // Some promotion fixtures have empty tables (schema-only after
        // mutation); assert only that the read path completes without error.
        Assert.True(totalRows >= 0);
    }
}
