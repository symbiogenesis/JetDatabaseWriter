namespace JetDatabaseWriter.Tests.Core;

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Core;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention

/// <summary>
/// Fixture-based read tests for the Jackcess <c>oldDatesV2007.accdb</c>
/// fixture, which contains <c>Date/Time</c> column values from before the
/// Gregorian calendar switch (1582). Verifies that the reader decodes
/// these extreme <see cref="DateTime"/> values without throwing or
/// truncating. Jackcess analogue: <c>CalendarTest.java</c>.
/// </summary>
public sealed class OldDateFixtureTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    /// <summary>
    /// The fixture has exactly one user table (<c>Table1</c>).
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task OldDatesV2007_ListTables_ReturnsSingleTable()
    {
        AccessReader reader = await db.GetReaderAsync(
            TestDatabases.OldDatesV2007,
            TestContext.Current.CancellationToken);

        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        Assert.Single(tables);
        Assert.Equal("Table1", tables[0]);
    }

    /// <summary>
    /// <c>Table1</c> has an <c>ID</c> column and a <c>DateField</c> column
    /// typed as <c>Date/Time</c>.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task Table1_HasDateTimeColumn()
    {
        AccessReader reader = await db.GetReaderAsync(
            TestDatabases.OldDatesV2007,
            TestContext.Current.CancellationToken);

        var meta = await reader.GetColumnMetadataAsync(
            "Table1",
            TestContext.Current.CancellationToken);

        Assert.Contains(meta, m => m.Name == "DateField" && m.TypeName == "Date/Time");
    }

    /// <summary>
    /// All 4 rows can be read, and every <c>DateField</c> value is a
    /// <see cref="DateTime"/> before 1600 — confirming the reader handles
    /// pre-Gregorian dates without corruption.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task Table1_AllDates_AreBeforeYear1600()
    {
        AccessReader reader = await db.GetReaderAsync(
            TestDatabases.OldDatesV2007,
            TestContext.Current.CancellationToken);

        DataTable dt = await reader.ReadDataTableAsync(
            "Table1",
            cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(4, dt.Rows.Count);

        DateTime[] dates = dt.AsEnumerable()
            .Select(r => r.Field<DateTime>("DateField"))
            .ToArray();

        Assert.All(dates, d =>
            Assert.True(d.Year < 1600, $"Expected pre-1600 date, got {d:yyyy-MM-dd}."));
    }

    /// <summary>
    /// The fixture contains a date from 1392, proving the reader can decode
    /// dates more than 500 years before the OLE Automation epoch (1899-12-30).
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task Table1_ContainsDateFrom1392()
    {
        AccessReader reader = await db.GetReaderAsync(
            TestDatabases.OldDatesV2007,
            TestContext.Current.CancellationToken);

        DataTable dt = await reader.ReadDataTableAsync(
            "Table1",
            cancellationToken: TestContext.Current.CancellationToken);

        DateTime[] dates = dt.AsEnumerable()
            .Select(r => r.Field<DateTime>("DateField"))
            .ToArray();

        Assert.Contains(dates, d => d.Year == 1392);
    }
}
