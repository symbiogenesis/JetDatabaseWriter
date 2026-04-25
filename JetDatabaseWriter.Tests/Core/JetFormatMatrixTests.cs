namespace JetDatabaseWriter.Tests;

using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using Xunit;

/// <summary>
/// Format-matrix sweep that exercises the curated Jackcess baseline (one
/// fixture per Access version, V1997 → V2019) in a single pass: list every
/// user table, fetch column metadata, and stream rows. Catches regressions
/// that only show up on one of Jet3 / Jet4 / ACE when feature-targeted tests
/// miss them. Catalog-only assertions (statistics, bulk ReadAllTables) widen
/// to <see cref="TestDatabases.JackcessAll"/> for broader coverage.
///
/// <para>Jackcess analogues:
/// <list type="bullet">
///   <item><c>impl/JetFormatTest.java</c></item>
///   <item><c>impl/DatabaseReadWriteTest.java</c></item>
/// </list>
/// </para>
/// </summary>
public sealed class JetFormatMatrixTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    /// <summary>
    /// For every readable Jackcess fixture, every user table can be enumerated
    /// to completion via <see cref="AccessReader.Rows(string, IProgress{long}?, System.Threading.CancellationToken)"/>
    /// without throwing, and each enumerated row matches the column-metadata count.
    /// </summary>
    /// <param name="path">Path to the fixture under test.</param>
    /// <returns>A task that completes when the assertion has run.</returns>
    [Theory]
    [MemberData(nameof(TestDatabases.Jackcess), MemberType = typeof(TestDatabases))]
    public async Task EveryFixture_EveryTable_StreamsToCompletion(string path)
    {
        AccessReader reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        Assert.NotEmpty(tables);

        foreach (string table in tables)
        {
            var meta = await reader.GetColumnMetadataAsync(table, TestContext.Current.CancellationToken);
            int expectedColumns = meta.Count;

            long observed = 0;
            await foreach (object[] row in reader.Rows(table, cancellationToken: TestContext.Current.CancellationToken))
            {
                Assert.NotNull(row);
                Assert.Equal(expectedColumns, row.Length);
                observed++;
            }

            long reportedRowCount = await reader.GetRealRowCountAsync(table, TestContext.Current.CancellationToken);
            Assert.Equal(reportedRowCount, observed);
        }
    }

    /// <summary>
    /// For every readable fixture, <c>ReadDataTableAsync</c> on every table returns
    /// a non-null <see cref="DataTable"/> with column count matching
    /// <c>GetColumnMetadataAsync</c>. This is the catalog-side complement of
    /// <see cref="EveryFixture_EveryTable_StreamsToCompletion"/>.
    /// </summary>
    /// <param name="path">Path to the fixture under test.</param>
    /// <returns>A task that completes when the assertion has run.</returns>
    [Theory]
    [MemberData(nameof(TestDatabases.Jackcess), MemberType = typeof(TestDatabases))]
    public async Task EveryFixture_EveryTable_ReadDataTable_ReturnsConsistentSchema(string path)
    {
        AccessReader reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        foreach (string table in tables)
        {
            DataTable? dt = await reader.ReadDataTableAsync(table, cancellationToken: TestContext.Current.CancellationToken);
            Assert.NotNull(dt);

            var meta = await reader.GetColumnMetadataAsync(table, TestContext.Current.CancellationToken);
            Assert.Equal(meta.Count, dt.Columns.Count);

            for (int i = 0; i < meta.Count; i++)
            {
                Assert.Equal(meta[i].Name, dt.Columns[i].ColumnName);
                Assert.Equal(meta[i].ClrType, dt.Columns[i].DataType);
            }
        }
    }

    /// <summary>
    /// For every readable fixture, <see cref="AccessReader.GetStatisticsAsync"/>
    /// returns a populated <see cref="DatabaseStatistics"/> whose advertised
    /// <c>TableCount</c> matches <see cref="AccessReader.ListTablesAsync"/>.
    /// </summary>
    /// <param name="path">Path to the fixture under test.</param>
    /// <returns>A task that completes when the assertion has run.</returns>
    [Theory]
    [MemberData(nameof(TestDatabases.JackcessAll), MemberType = typeof(TestDatabases))]
    public async Task EveryFixture_Statistics_TableCountMatchesListTables(string path)
    {
        AccessReader reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);

        DatabaseStatistics stats = await reader.GetStatisticsAsync(TestContext.Current.CancellationToken);
        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        Assert.Equal(tables.Count, stats.TableCount);
        Assert.True(stats.DatabaseSizeBytes > 0);
        Assert.False(string.IsNullOrWhiteSpace(stats.Version));
    }

    /// <summary>
    /// Bulk <see cref="AccessReader.ReadAllTablesAsync"/> returns one entry per
    /// listed user table on every readable fixture. Catches mismatches between
    /// the bulk path and the single-table path.
    /// </summary>
    /// <param name="path">Path to the fixture under test.</param>
    /// <returns>A task that completes when the assertion has run.</returns>
    [Theory]
    [MemberData(nameof(TestDatabases.Jackcess), MemberType = typeof(TestDatabases))]
    public async Task EveryFixture_ReadAllTables_KeyedByEveryListedTable(string path)
    {
        AccessReader reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);

        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
        Dictionary<string, DataTable> all = await reader.ReadAllTablesAsync(cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(tables.Count, all.Count);
        foreach (string t in tables)
        {
            Assert.True(all.ContainsKey(t), $"ReadAllTables missing entry for table '{t}' in '{System.IO.Path.GetFileName(path)}'.");
        }
    }
}
