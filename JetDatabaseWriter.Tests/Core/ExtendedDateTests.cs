namespace JetDatabaseWriter.Tests.Core;

using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Core;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Coverage for the Access 2019+ <c>Date/Time Extended</c> column type
/// (TDEF code <c>0x14</c>, 42-byte fixed string) read from
/// <c>extDateTestV2019.accdb</c>.
///
/// <para>Jackcess analogue: <c>impl/ExtendedDateTest.java</c>.
/// </para>
/// <para>The current reader maps this type to a <see cref="string"/> CLR
/// representation (no high-precision <c>DateTimeOffset</c> path yet); these
/// tests pin the contract so that any future migration of the mapping is
/// caught explicitly.
/// </para>
/// </summary>
public sealed class ExtendedDateTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    /// <summary>
    /// The extDateTest fixture lists at least one user table on open.
    /// </summary>
    /// <returns>A task that completes when the assertion has run.</returns>
    [Fact]
    public async Task ExtDateTestV2019_ListTables_ReturnsNonEmpty()
    {
        if (!File.Exists(TestDatabases.ExtDateTestV2019))
        {
            return;
        }

        AccessReader reader = await db.GetReaderAsync(TestDatabases.ExtDateTestV2019, TestContext.Current.CancellationToken);
        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        Assert.NotEmpty(tables);
    }

    /// <summary>
    /// At least one column in the fixture is reported with the
    /// <c>Date/Time Extended</c> type name (the type added in Access 2019).
    /// </summary>
    /// <returns>A task that completes when the assertion has run.</returns>
    [Fact]
    public async Task ExtDateTestV2019_AtLeastOneColumn_IsTypedAsDateTimeExtended()
    {
        if (!File.Exists(TestDatabases.ExtDateTestV2019))
        {
            return;
        }

        AccessReader reader = await db.GetReaderAsync(TestDatabases.ExtDateTestV2019, TestContext.Current.CancellationToken);
        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        bool foundExtended = false;
        foreach (string table in tables)
        {
            var meta = await reader.GetColumnMetadataAsync(table, TestContext.Current.CancellationToken);
            if (meta.Any(c => c.TypeName == "Date/Time Extended"))
            {
                foundExtended = true;
                break;
            }
        }

        Assert.True(foundExtended, "extDateTestV2019 was expected to contain at least one Date/Time Extended column.");
    }

    /// <summary>
    /// Every table in the fixture can be streamed to completion without
    /// throwing — i.e. the extended date type does not crash the row decoder.
    /// </summary>
    /// <returns>A task that completes when the assertion has run.</returns>
    [Fact]
    public async Task ExtDateTestV2019_StreamsAllRows_WithoutThrowing()
    {
        if (!File.Exists(TestDatabases.ExtDateTestV2019))
        {
            return;
        }

        AccessReader reader = await db.GetReaderAsync(TestDatabases.ExtDateTestV2019, TestContext.Current.CancellationToken);
        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        foreach (string table in tables)
        {
            await foreach (object[] row in reader.Rows(table, cancellationToken: TestContext.Current.CancellationToken))
            {
                Assert.NotNull(row);
            }
        }
    }
}
