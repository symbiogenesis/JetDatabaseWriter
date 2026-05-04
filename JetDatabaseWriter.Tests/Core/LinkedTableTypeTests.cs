namespace JetDatabaseWriter.Tests.Core;

using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Tests for linked table type classification. The Jackcess
/// <c>linkerTestV2007</c> fixture contains an Access-linked entry
/// (<c>Table2</c>, MSysObjects.Type = 6) pointing to <c>Table1</c> in
/// <c>linkeeTest.accdb</c>. Validates that the reader correctly
/// distinguishes Access-linked (type 6) from ODBC-linked (type 4) tables.
/// </summary>
public sealed class LinkedTableTypeTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    /// <summary>
    /// An Access-linked table entry has <see cref="LinkedTableInfo.IsOdbc"/>
    /// set to <see langword="false"/> and a populated
    /// <see cref="LinkedTableInfo.SourceDatabasePath"/>.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task AccessLinkedTable_IsNotOdbc()
    {
        if (!File.Exists(TestDatabases.LinkerTestV2007))
        {
            return;
        }

        AccessReader reader = await db.GetReaderAsync(
            TestDatabases.LinkerTestV2007, TestContext.Current.CancellationToken);

        List<LinkedTableInfo> linked =
            await reader.ListLinkedTablesAsync(TestContext.Current.CancellationToken);

        LinkedTableInfo? table2 = linked.FirstOrDefault(
            l => string.Equals(l.Name, "Table2", System.StringComparison.OrdinalIgnoreCase));

        Assert.NotNull(table2);
        Assert.False(table2.IsOdbc);
    }

    /// <summary>
    /// An Access-linked table entry carries the source database path in
    /// <see cref="LinkedTableInfo.SourceDatabasePath"/> and the path
    /// references the linkee fixture.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task AccessLinkedTable_HasSourceDatabasePath()
    {
        if (!File.Exists(TestDatabases.LinkerTestV2007))
        {
            return;
        }

        AccessReader reader = await db.GetReaderAsync(
            TestDatabases.LinkerTestV2007, TestContext.Current.CancellationToken);

        List<LinkedTableInfo> linked =
            await reader.ListLinkedTablesAsync(TestContext.Current.CancellationToken);

        LinkedTableInfo table2 = linked.Single(
            l => string.Equals(l.Name, "Table2", System.StringComparison.OrdinalIgnoreCase));

        Assert.False(string.IsNullOrWhiteSpace(table2.SourceDatabasePath));
        Assert.Contains("linkeeTest", table2.SourceDatabasePath, System.StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// An Access-linked table entry does not have a connection string
    /// (that property is for ODBC-linked tables only).
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task AccessLinkedTable_HasNoConnectionString()
    {
        if (!File.Exists(TestDatabases.LinkerTestV2007))
        {
            return;
        }

        AccessReader reader = await db.GetReaderAsync(
            TestDatabases.LinkerTestV2007, TestContext.Current.CancellationToken);

        List<LinkedTableInfo> linked =
            await reader.ListLinkedTablesAsync(TestContext.Current.CancellationToken);

        LinkedTableInfo table2 = linked.Single(
            l => string.Equals(l.Name, "Table2", System.StringComparison.OrdinalIgnoreCase));

        Assert.Null(table2.ConnectionString);
    }

    /// <summary>
    /// The linked table's <see cref="LinkedTableInfo.ForeignName"/> matches the
    /// name of the table in the remote database (Table1 in linkeeTest).
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task AccessLinkedTable_ForeignName_MatchesRemoteTable()
    {
        if (!File.Exists(TestDatabases.LinkerTestV2007))
        {
            return;
        }

        AccessReader reader = await db.GetReaderAsync(
            TestDatabases.LinkerTestV2007, TestContext.Current.CancellationToken);

        List<LinkedTableInfo> linked =
            await reader.ListLinkedTablesAsync(TestContext.Current.CancellationToken);

        LinkedTableInfo table2 = linked.Single(
            l => string.Equals(l.Name, "Table2", System.StringComparison.OrdinalIgnoreCase));

        Assert.Equal("Table1", table2.ForeignName);
    }

    /// <summary>
    /// The linker fixture contains exactly one linked table (Table2).
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task LinkerFixture_HasExactlyOneLinkedTable()
    {
        if (!File.Exists(TestDatabases.LinkerTestV2007))
        {
            return;
        }

        AccessReader reader = await db.GetReaderAsync(
            TestDatabases.LinkerTestV2007, TestContext.Current.CancellationToken);

        List<LinkedTableInfo> linked =
            await reader.ListLinkedTablesAsync(TestContext.Current.CancellationToken);

        Assert.Single(linked);
        Assert.Equal("Table2", linked[0].Name);
    }
}
