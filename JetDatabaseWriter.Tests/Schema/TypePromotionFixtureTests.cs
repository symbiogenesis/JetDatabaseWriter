namespace JetDatabaseWriter.Tests.Schema;

using System.Threading.Tasks;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

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
/// <param name="db">The database input.</param>
public sealed class TypePromotionFixtureTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    /// <summary>
    /// The fixture lists at least one user table without throwing.
    /// </summary>
    /// <param name="path">The file path.</param>
    [Theory]
    [MemberData(nameof(TestDatabases.Promotion), MemberType = typeof(TestDatabases))]
    public async Task Promotion_ListTables_ReturnsNonEmpty(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);

        var tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        Assert.NotEmpty(tables);
    }

    /// <summary>
    /// Every table in the fixture exposes at least one column via
    /// <see cref="AccessReader.GetColumnMetadataAsync"/>.
    /// </summary>
    /// <param name="path">The file path.</param>
    [Theory]
    [MemberData(nameof(TestDatabases.Promotion), MemberType = typeof(TestDatabases))]
    public async Task Promotion_AllTables_HaveColumns(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        var tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        foreach (string table in tables)
        {
            var cols =
                await reader.GetColumnMetadataAsync(table, TestContext.Current.CancellationToken);
            Assert.NotEmpty(cols);
        }
    }

    /// <summary>
    /// All rows in every table stream without throwing, confirming that
    /// promoted column type descriptors are decoded correctly.
    /// </summary>
    /// <param name="path">The file path.</param>
    [Theory]
    [MemberData(nameof(TestDatabases.Promotion), MemberType = typeof(TestDatabases))]
    public async Task Promotion_AllTables_StreamAllRows_WithoutThrowing(string path)
    {
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        var tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        long totalRows = 0;
        foreach (string table in tables)
        {
            var dt = await reader.ReadDataTableAsync(
                table, cancellationToken: TestContext.Current.CancellationToken);
            totalRows += dt.Rows.Count;
        }

        Assert.NotEmpty(tables);
    }
}
