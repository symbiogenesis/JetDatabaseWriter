namespace JetDatabaseWriter.Tests.Core;

using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention

/// <summary>
/// Fixture-based tests for the Jackcess <c>linkerTestV2007.accdb</c> and
/// <c>linkeeTest.accdb</c> pair. The linker fixture is a front-end database
/// containing tables that reference (link to) the linkee fixture. Validates
/// that:
///   1. <see cref="AccessReader.ListLinkedTablesAsync"/> surfaces linked entries.
///   2. The linkee database reads normally as a standalone database.
///   3. The linker's local tables (if any) are readable.
///
/// <para>Jackcess analogue: <c>DatabaseTest.testLinkedTables</c>.
/// </para>
/// </summary>
public sealed class LinkerFixtureTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    /// <summary>
    /// The linkee fixture can be opened and lists at least one table.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task LinkeeTest_OpensAndListsTables()
    {
        if (!File.Exists(TestDatabases.LinkeeTest))
        {
            return;
        }

        AccessReader reader = await db.GetReaderAsync(
            TestDatabases.LinkeeTest, TestContext.Current.CancellationToken);

        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        Assert.NotEmpty(tables);
    }

    /// <summary>
    /// The linkee fixture's tables can be read to completion without error.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task LinkeeTest_AllTables_StreamAllRows()
    {
        if (!File.Exists(TestDatabases.LinkeeTest))
        {
            return;
        }

        AccessReader reader = await db.GetReaderAsync(
            TestDatabases.LinkeeTest, TestContext.Current.CancellationToken);
        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        long totalRows = 0;
        foreach (string table in tables)
        {
            DataTable dt = await reader.ReadDataTableAsync(
                table,
                cancellationToken: TestContext.Current.CancellationToken);
            totalRows += dt.Rows.Count;
        }

        Assert.True(
            totalRows > 0,
            "Expected at least one row across all tables in linkeeTest.");
    }

    /// <summary>
    /// The linker fixture reports linked table entries via
    /// <see cref="AccessReader.ListLinkedTablesAsync"/>. The Jackcess
    /// <c>linkerTestV2007</c> fixture contains links to <c>linkeeTest.accdb</c>.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task LinkerTestV2007_ListLinkedTables_ReturnsEntries()
    {
        if (!File.Exists(TestDatabases.LinkerTestV2007))
        {
            return;
        }

        AccessReader reader = await db.GetReaderAsync(
            TestDatabases.LinkerTestV2007, TestContext.Current.CancellationToken);

        List<LinkedTableInfo> linked =
            await reader.ListLinkedTablesAsync(TestContext.Current.CancellationToken);

        Assert.NotEmpty(linked);
    }

    /// <summary>
    /// Each linked table entry has a non-empty foreign name (the name of the
    /// table in the source database). Source metadata (path or connection
    /// string) may be absent for relative-path links whose path wasn't
    /// persisted in the MSysObjects connect string.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task LinkerTestV2007_LinkedTableEntries_HaveValidMetadata()
    {
        if (!File.Exists(TestDatabases.LinkerTestV2007))
        {
            return;
        }

        AccessReader reader = await db.GetReaderAsync(
            TestDatabases.LinkerTestV2007, TestContext.Current.CancellationToken);
        List<LinkedTableInfo> linked =
            await reader.ListLinkedTablesAsync(TestContext.Current.CancellationToken);

        foreach (LinkedTableInfo entry in linked)
        {
            Assert.False(
                string.IsNullOrWhiteSpace(entry.Name),
                "Linked table entry has blank Name.");
            Assert.False(
                string.IsNullOrWhiteSpace(entry.ForeignName),
                $"Linked table '{entry.Name}' has blank ForeignName.");
        }
    }

    /// <summary>
    /// At least one linked entry references a foreign table name that matches
    /// a table in the linkee fixture — confirming cross-database link metadata
    /// is decoded correctly.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task LinkerTestV2007_LinkedEntries_ReferenceLinkeeTableNames()
    {
        if (!File.Exists(TestDatabases.LinkerTestV2007) || !File.Exists(TestDatabases.LinkeeTest))
        {
            return;
        }

        AccessReader linkerReader = await db.GetReaderAsync(
            TestDatabases.LinkerTestV2007, TestContext.Current.CancellationToken);
        List<LinkedTableInfo> linked =
            await linkerReader.ListLinkedTablesAsync(TestContext.Current.CancellationToken);

        AccessReader linkeeReader = await db.GetReaderAsync(
            TestDatabases.LinkeeTest, TestContext.Current.CancellationToken);
        List<string> linkeeTables =
            await linkeeReader.ListTablesAsync(TestContext.Current.CancellationToken);

        // At least one linked entry's ForeignName should match a table in linkee.
        bool foundMatch = false;
        foreach (LinkedTableInfo entry in linked)
        {
            if (linkeeTables.Contains(entry.ForeignName))
            {
                foundMatch = true;
                break;
            }
        }

        Assert.True(
            foundMatch,
            "Expected at least one linked entry whose ForeignName matches a linkee table.");
    }

    /// <summary>
    /// The linker fixture's local user tables (those not linked) are readable.
    /// The fixture may or may not have local tables; if it does, they should
    /// be openable.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task LinkerTestV2007_LocalTables_AreReadable()
    {
        if (!File.Exists(TestDatabases.LinkerTestV2007))
        {
            return;
        }

        AccessReader reader = await db.GetReaderAsync(
            TestDatabases.LinkerTestV2007, TestContext.Current.CancellationToken);
        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        // ListTables returns only local tables (type 1); linked tables are
        // excluded. Read whatever is local.
        foreach (string table in tables)
        {
            DataTable dt = await reader.ReadDataTableAsync(
                table,
                cancellationToken: TestContext.Current.CancellationToken);
            Assert.NotNull(dt);
        }
    }
}
