namespace JetDatabaseWriter.Tests.Core;

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using JetDatabaseWriter.Core;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention

/// <summary>
/// Tests for <see cref="IAccessReader.GetComplexColumnsAsync"/> against the
/// <c>ComplexFields.accdb</c> fixture.
/// Schema assertions are grounded in
/// <c>docs/design/format-probe-appendix-complex.md</c>.
/// </summary>
public sealed class ComplexColumnInfoTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    [Fact]
    public async Task GetComplexColumns_DocumentsAttachments_ReturnsSingleAttachment()
    {
        var reader = await db.GetReaderAsync(TestDatabases.ComplexFields, TestContext.Current.CancellationToken);
        IReadOnlyList<ComplexColumnInfo> info = await reader.GetComplexColumnsAsync("Documents", TestContext.Current.CancellationToken);

        ComplexColumnInfo entry = Assert.Single(info);
        Assert.Equal("Attachments", entry.ColumnName, ignoreCase: true);
        Assert.Equal(ComplexColumnKind.Attachment, entry.Kind);

        // Per the format probe appendix, Documents.Attachments has ComplexID = 1.
        Assert.Equal(1, entry.ComplexId);

        // The hidden flat table follows the f_<32-hex>_<colName> pattern.
        Assert.StartsWith("f_", entry.FlatTableName, StringComparison.OrdinalIgnoreCase);
        Assert.EndsWith("_Attachments", entry.FlatTableName, StringComparison.OrdinalIgnoreCase);

        Assert.Equal("MSysComplexType_Attachment", entry.ComplexTypeName, ignoreCase: true);
        Assert.True(entry.FlatTableId != 0);
        Assert.True(entry.ComplexTypeObjectId != 0);
    }

    [Fact]
    public async Task GetComplexColumns_TableWithoutComplexColumns_ReturnsEmpty()
    {
        var reader = await db.GetReaderAsync(TestDatabases.ComplexFields, TestContext.Current.CancellationToken);
        IReadOnlyList<ComplexColumnInfo> info = await reader.GetComplexColumnsAsync("Tags", TestContext.Current.CancellationToken);
        Assert.Empty(info);
    }

    [Fact]
    public async Task GetComplexColumns_UnknownTable_ReturnsEmpty()
    {
        var reader = await db.GetReaderAsync(TestDatabases.ComplexFields, TestContext.Current.CancellationToken);
        IReadOnlyList<ComplexColumnInfo> info = await reader.GetComplexColumnsAsync("NoSuchTable", TestContext.Current.CancellationToken);
        Assert.Empty(info);
    }

    [Fact]
    public async Task GetComplexColumns_NorthwindCategories_ResolvesAttachment()
    {
        // NorthwindTraders.accdb has multiple complex/attachment columns
        // (e.g. ProductCategories.ProductCategoryImage, Employees.Attachments).
        var reader = await db.GetReaderAsync(TestDatabases.NorthwindTraders, TestContext.Current.CancellationToken);
        IReadOnlyList<ComplexColumnInfo> info = await reader.GetComplexColumnsAsync("ProductCategories", TestContext.Current.CancellationToken);

        Assert.NotEmpty(info);
        Assert.All(info, c => Assert.NotEqual(0, c.ComplexId));
        Assert.All(info, c => Assert.False(string.IsNullOrEmpty(c.ColumnName)));
        Assert.All(info, c => Assert.NotEqual(ComplexColumnKind.Unknown, c.Kind));
    }
}
