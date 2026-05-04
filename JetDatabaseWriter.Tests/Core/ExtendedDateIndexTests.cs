namespace JetDatabaseWriter.Tests.Core;

using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Core;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention

/// <summary>
/// Validates that the extended date/time column type (Access 2019+, TDEF
/// code 0x14) surfaces in index metadata when the column is indexed.
/// The <c>extDateTestV2019</c> fixture is purpose-built by Jackcess to
/// test this type. Closes §1.2 gap: "Extended Date/Time index keys.".
/// </summary>
public sealed class ExtendedDateIndexTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    /// <summary>
    /// The extDateTest fixture has at least one table with an index on a
    /// Date/Time Extended column. Verify the index surfaces via ListIndexesAsync.
    /// </summary>
    [Fact]
    public async Task ExtDateTestV2019_HasIndexes()
    {
        if (!File.Exists(TestDatabases.ExtDateTestV2019))
        {
            return;
        }

        var reader = await db.GetReaderAsync(
            TestDatabases.ExtDateTestV2019,
            TestContext.Current.CancellationToken);

        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
        Assert.NotEmpty(tables);

        int totalIndexes = 0;
        foreach (string table in tables)
        {
            IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync(
                table, TestContext.Current.CancellationToken);
            totalIndexes += indexes.Count;
        }

        Assert.True(totalIndexes > 0, "extDateTestV2019 was expected to contain at least one index.");
    }

    /// <summary>
    /// At least one index in the fixture references a Date/Time Extended
    /// column (verifying the column metadata is resolvable for the index key).
    /// </summary>
    [Fact]
    public async Task ExtDateTestV2019_IndexReferencesExtendedDateColumn()
    {
        if (!File.Exists(TestDatabases.ExtDateTestV2019))
        {
            return;
        }

        var reader = await db.GetReaderAsync(
            TestDatabases.ExtDateTestV2019,
            TestContext.Current.CancellationToken);

        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        bool foundExtDateIndexColumn = false;
        foreach (string table in tables)
        {
            IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync(
                table, TestContext.Current.CancellationToken);
            IReadOnlyList<ColumnMetadata> columns = await reader.GetColumnMetadataAsync(
                table, TestContext.Current.CancellationToken);

            var extDateColumnNames = columns
                .Where(c => c.TypeName == "Date/Time Extended")
                .Select(c => c.Name)
                .ToHashSet();

            foreach (IndexMetadata idx in indexes)
            {
                if (idx.Columns.Any(c => extDateColumnNames.Contains(c.Name)))
                {
                    foundExtDateIndexColumn = true;
                    break;
                }
            }

            if (foundExtDateIndexColumn)
            {
                break;
            }
        }

        Assert.True(foundExtDateIndexColumn, "Expected at least one index whose key column is Date/Time Extended.");
    }

    /// <summary>
    /// All rows in the extended date fixture can be read without throwing,
    /// exercising the index key decode path implicitly through row streaming.
    /// </summary>
    [Fact]
    public async Task ExtDateTestV2019_AllTables_StreamWithoutThrowing()
    {
        if (!File.Exists(TestDatabases.ExtDateTestV2019))
        {
            return;
        }

        var reader = await db.GetReaderAsync(
            TestDatabases.ExtDateTestV2019,
            TestContext.Current.CancellationToken);

        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
        int totalRows = 0;

        foreach (string table in tables)
        {
            await foreach (object[] row in reader.Rows(
                table, cancellationToken: TestContext.Current.CancellationToken))
            {
                Assert.NotNull(row);
                totalRows++;
            }
        }

        Assert.True(totalRows > 0, "Expected rows in the extDateTest fixture.");
    }
}
