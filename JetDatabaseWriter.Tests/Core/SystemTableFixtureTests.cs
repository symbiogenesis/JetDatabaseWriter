namespace JetDatabaseWriter.Tests.Core;

using System.Data;
using System.Threading.Tasks;
using JetDatabaseWriter.Core;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention

/// <summary>
/// Read tests for the ACE-only system tables <c>MSysAccessStorage</c> and
/// <c>MSysNavPaneGroups</c>. These tables are present in every
/// Access-authored ACCDB file and referenced by Jackcess but were not
/// previously asserted by our test suite.
/// Covers §4 of <c>docs/design/test-coverage-gaps.md</c>.
/// </summary>
public sealed class SystemTableFixtureTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    /// <summary>
    /// <c>MSysAccessStorage</c> is present in <c>complexDataTestV2007.accdb</c>
    /// and has a non-empty schema.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task MSysAccessStorage_HasExpectedColumns()
    {
        AccessReader reader = await db.GetReaderAsync(
            TestDatabases.ComplexDataTestV2007,
            TestContext.Current.CancellationToken);

        var meta = await reader.GetColumnMetadataAsync(
            "MSysAccessStorage",
            TestContext.Current.CancellationToken);

        Assert.NotEmpty(meta);
        Assert.Contains(meta, m => m.Name == "Id");
        Assert.Contains(meta, m => m.Name == "Name");
        Assert.Contains(meta, m => m.Name == "ParentId");
        Assert.Contains(meta, m => m.Name == "Type");
    }

    /// <summary>
    /// <c>MSysAccessStorage</c> rows can be read into a <see cref="DataTable"/>
    /// without throwing, and the fixture contains at least one row.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task MSysAccessStorage_ReadsNonEmptyRowSet()
    {
        AccessReader reader = await db.GetReaderAsync(
            TestDatabases.ComplexDataTestV2007,
            TestContext.Current.CancellationToken);

        DataTable dt = await reader.ReadDataTableAsync(
            "MSysAccessStorage",
            cancellationToken: TestContext.Current.CancellationToken);

        Assert.True(dt.Rows.Count > 0, "MSysAccessStorage should have at least one row.");
    }

    /// <summary>
    /// <c>MSysNavPaneGroups</c> is present in the fixture and has a
    /// non-empty schema with the expected columns.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task MSysNavPaneGroups_HasExpectedColumns()
    {
        AccessReader reader = await db.GetReaderAsync(
            TestDatabases.ComplexDataTestV2007,
            TestContext.Current.CancellationToken);

        var meta = await reader.GetColumnMetadataAsync(
            "MSysNavPaneGroups",
            TestContext.Current.CancellationToken);

        Assert.NotEmpty(meta);
        Assert.Contains(meta, m => m.Name == "Id");
        Assert.Contains(meta, m => m.Name == "Name");
        Assert.Contains(meta, m => m.Name == "Flags");
    }

    /// <summary>
    /// <c>MSysNavPaneGroups</c> rows can be read without throwing.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task MSysNavPaneGroups_ReadsNonEmptyRowSet()
    {
        AccessReader reader = await db.GetReaderAsync(
            TestDatabases.ComplexDataTestV2007,
            TestContext.Current.CancellationToken);

        DataTable dt = await reader.ReadDataTableAsync(
            "MSysNavPaneGroups",
            cancellationToken: TestContext.Current.CancellationToken);

        Assert.True(dt.Rows.Count > 0, "MSysNavPaneGroups should have at least one row.");
    }
}
