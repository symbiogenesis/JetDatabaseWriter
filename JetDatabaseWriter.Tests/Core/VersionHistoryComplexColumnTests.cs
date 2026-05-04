namespace JetDatabaseWriter.Tests.Core;

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention

/// <summary>
/// Validates that the reader identifies version-history complex columns from
/// the Jackcess <c>complexDataTest</c> fixtures. These fixtures contain columns
/// of all three complex subtypes (Attachment, MultiValue, VersionHistory).
/// The reader currently reports the version-history column with
/// <c>Kind = Unknown</c> because the discriminator does not yet recognize
/// the <c>MSysComplexTypeVH_*</c> template table pattern — the column IS
/// present (named <c>VersionHistory_*</c>) but mis-classified.
/// Partially closes §2.2 gap: "Versioned-text column.".
/// </summary>
public sealed class VersionHistoryComplexColumnTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    /// <summary>
    /// The complexDataTest fixture contains a complex column whose name
    /// starts with "VersionHistory" and whose ComplexTypeName matches
    /// the version-history template pattern.
    /// </summary>
    [Theory]
    [MemberData(nameof(TestDatabases.ComplexData), MemberType = typeof(TestDatabases))]
    public async Task ComplexDataFixture_HasVersionHistoryNamedColumn(string path)
    {
        AccessReader reader = await db.GetReaderAsync(
            path,
            TestContext.Current.CancellationToken);

        IReadOnlyList<ComplexColumnInfo> complex = await reader.GetComplexColumnsAsync(
            "Table1",
            TestContext.Current.CancellationToken);

        Assert.Contains(complex, c => c.ColumnName.StartsWith("VersionHistory", System.StringComparison.Ordinal));
    }

    /// <summary>
    /// The version-history column has a ComplexTypeName containing "VH"
    /// (the Access system prefix for version-history template tables).
    /// </summary>
    [Theory]
    [MemberData(nameof(TestDatabases.ComplexData), MemberType = typeof(TestDatabases))]
    public async Task VersionHistoryColumn_HasVHComplexTypeName(string path)
    {
        AccessReader reader = await db.GetReaderAsync(
            path,
            TestContext.Current.CancellationToken);

        IReadOnlyList<ComplexColumnInfo> complex = await reader.GetComplexColumnsAsync(
            "Table1",
            TestContext.Current.CancellationToken);

        ComplexColumnInfo vhCol = complex.First(
            c => c.ColumnName.StartsWith("VersionHistory", System.StringComparison.Ordinal));

        Assert.Contains("VH", vhCol.ComplexTypeName, System.StringComparison.Ordinal);
        Assert.False(string.IsNullOrWhiteSpace(vhCol.FlatTableName), "Version-history column should reference a flat child table.");
        Assert.True(vhCol.ComplexId > 0, "ComplexId should be positive.");
    }

    /// <summary>
    /// All three complex column subtypes are present in the fixture:
    /// Attachment, MultiValue, and a version-history column (currently
    /// classified as Unknown — see class-level doc). This confirms the
    /// fixture exercises the full complex-column range.
    /// </summary>
    [Theory]
    [MemberData(nameof(TestDatabases.ComplexData), MemberType = typeof(TestDatabases))]
    public async Task ComplexDataFixture_HasAllThreeComplexSubtypes(string path)
    {
        AccessReader reader = await db.GetReaderAsync(
            path,
            TestContext.Current.CancellationToken);

        IReadOnlyList<ComplexColumnInfo> complex = await reader.GetComplexColumnsAsync(
            "Table1",
            TestContext.Current.CancellationToken);

        Assert.Contains(complex, c => c.Kind == ComplexColumnKind.Attachment);
        Assert.Contains(complex, c => c.Kind == ComplexColumnKind.MultiValue);

        // Version-history is present but currently reported as Unknown.
        Assert.Contains(complex, c =>
            c.ColumnName.StartsWith("VersionHistory", System.StringComparison.Ordinal)
            && c.ComplexTypeName.Contains("VH", System.StringComparison.Ordinal));
    }

    /// <summary>
    /// The version-history flat child table is readable by the reader
    /// (it is a hidden system table whose rows hold historical values).
    /// </summary>
    [Theory]
    [MemberData(nameof(TestDatabases.ComplexData), MemberType = typeof(TestDatabases))]
    public async Task VersionHistoryFlatTable_IsReadable(string path)
    {
        AccessReader reader = await db.GetReaderAsync(
            path,
            TestContext.Current.CancellationToken);

        IReadOnlyList<ComplexColumnInfo> complex = await reader.GetComplexColumnsAsync(
            "Table1",
            TestContext.Current.CancellationToken);

        ComplexColumnInfo vhCol = complex.First(
            c => c.ColumnName.StartsWith("VersionHistory", System.StringComparison.Ordinal));

        // The flat table should be enumerable even though it's a system table.
        int rowCount = 0;
        await foreach (object[] row in reader.Rows(
            vhCol.FlatTableName,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            Assert.NotNull(row);
            rowCount++;
        }

        // The Jackcess complexDataTest fixture has version history rows.
        Assert.True(rowCount >= 1, $"Expected at least 1 row in version-history flat table '{vhCol.FlatTableName}'.");
    }
}
