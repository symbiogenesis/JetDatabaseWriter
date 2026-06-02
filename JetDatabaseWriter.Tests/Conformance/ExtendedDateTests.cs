namespace JetDatabaseWriter.Tests.Conformance;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Coverage for the Access 2019+ <c>Date/Time Extended</c> column type
/// (TDEF code <c>0x14</c>, 42-byte fixed payload) read from
/// <c>extDateTestV2019.accdb</c>.
///
/// <para>Jackcess analogue: <c>impl/ExtendedDateTest.java</c>.
/// </para>
/// <para>The reader maps this type to <see cref="DateTime"/> with
/// <see cref="DateTimeKind.Unspecified"/>, preserving the 100 ns fractional
/// precision stored by Access.
/// </para>
/// </summary>
/// <param name="db">The database input.</param>
public sealed class ExtendedDateTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    /// <summary>
    /// The extDateTest fixture lists at least one user table on open.
    /// </summary>
    [Fact]
    public async Task ExtDateTestV2019_ListTables_ReturnsNonEmpty()
    {
        if (!File.Exists(TestDatabases.ExtDateTestV2019))
        {
            return;
        }

        AccessReader reader = await db.GetReaderAsync(TestDatabases.ExtDateTestV2019, TestContext.Current.CancellationToken);
        IReadOnlyList<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        Assert.NotEmpty(tables);
    }

    /// <summary>
    /// At least one column in the fixture is reported with the
    /// <c>Date/Time Extended</c> type name (the type added in Access 2019).
    /// </summary>
    [Fact]
    public async Task ExtDateTestV2019_AtLeastOneColumn_IsTypedAsDateTimeExtended()
    {
        if (!File.Exists(TestDatabases.ExtDateTestV2019))
        {
            return;
        }

        AccessReader reader = await db.GetReaderAsync(TestDatabases.ExtDateTestV2019, TestContext.Current.CancellationToken);
        IReadOnlyList<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        bool foundExtended = false;
        foreach (string table in tables)
        {
            IReadOnlyList<ColumnMetadata> meta = await reader.GetColumnMetadataAsync(table, TestContext.Current.CancellationToken);
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
    [Fact]
    public async Task ExtDateTestV2019_StreamsAllRows_WithoutThrowing()
    {
        if (!File.Exists(TestDatabases.ExtDateTestV2019))
        {
            return;
        }

        AccessReader reader = await db.GetReaderAsync(TestDatabases.ExtDateTestV2019, TestContext.Current.CancellationToken);
        IReadOnlyList<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        foreach (string table in tables)
        {
            await foreach (object[] row in reader.Rows(table, cancellationToken: TestContext.Current.CancellationToken))
            {
                Assert.NotNull(row);
            }
        }
    }

    /// <summary>
    /// Jackcess pairs each extended-date value with a text rendering in
    /// <c>DateExtStr</c>. Decode <c>DateExt</c> to <see cref="DateTime"/> and
    /// verify that formatting the value matches the fixture's own string.
    /// </summary>
    [Fact]
    public async Task ExtDateTestV2019_DateExtRows_DecodeAsDateTime()
    {
        if (!File.Exists(TestDatabases.ExtDateTestV2019))
        {
            return;
        }

        AccessReader reader = await db.GetReaderAsync(TestDatabases.ExtDateTestV2019, TestContext.Current.CancellationToken);
        IReadOnlyList<ColumnMetadata> metadata = await reader.GetColumnMetadataAsync("Table1", TestContext.Current.CancellationToken);
        ColumnMetadata extended = Assert.Single(metadata, column => column.Name == "DateExt");
        Assert.Equal("Date/Time Extended", extended.TypeName);
        Assert.Equal(typeof(DateTime), extended.ClrType);
        ColumnMetadata text = Assert.Single(metadata, column => column.Name == "DateExtStr");

        await foreach (object[] row in reader.Rows("Table1", cancellationToken: TestContext.Current.CancellationToken))
        {
            if (row[extended.Ordinal] is DBNull)
            {
                Assert.Equal(DBNull.Value, row[text.Ordinal]);
                continue;
            }

            DateTime value = Assert.IsType<DateTime>(row[extended.Ordinal]);
            Assert.Equal(DateTimeKind.Unspecified, value.Kind);
            string expectedText = Assert.IsType<string>(row[text.Ordinal]);
            string dateOnly = value.ToString("M/d/yyyy", CultureInfo.InvariantCulture);
            string dateTime = value.ToString("M/d/yyyy h:mm:ss.fffffff tt", CultureInfo.InvariantCulture);
            Assert.True(
                string.Equals(expectedText, dateOnly, StringComparison.Ordinal)
                || string.Equals(expectedText, dateTime, StringComparison.Ordinal),
                $"Expected '{expectedText}' to match '{dateOnly}' or '{dateTime}'.");
        }
    }
}
