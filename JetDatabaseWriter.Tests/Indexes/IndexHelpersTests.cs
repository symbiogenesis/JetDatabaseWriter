namespace JetDatabaseWriter.Tests.Indexes;

using System;
using System.Collections.Generic;
using System.Linq;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes.Helpers;
using JetDatabaseWriter.Models;
using Xunit;

/// <summary>
/// Tests for <see cref="IndexHelpers.ProjectIndexes"/>, the index projection
/// AddColumn, DropColumn and RenameColumn share. An index on a column the
/// rewrite drops goes with it; an index whose key column has no name, as the
/// reader reports a key column the table does not have, cannot be carried to
/// the rebuilt table, and the rewrite must refuse rather than drop it.
/// </summary>
public sealed class IndexHelpersTests
{
    [Fact]
    public void ProjectIndexes_EmptyKeyColumnName_Throws()
    {
        // IndexCatalogReader.ReadMetadata names a key column whose col_num the
        // table does not have "", as for the phantom column 0x02FF of wide
        // tables written by builds before 4.0.0.
        IndexMetadata[] existing =
        [
            Index("IX_A", IndexKind.Normal, ("A", 0)),
            Index("IX_B", IndexKind.Normal, ("B", 1), (string.Empty, 0x02FF)),
        ];

        JetLimitationException ex = Assert.Throws<JetLimitationException>(
            () => IndexHelpers.ProjectIndexes(existing, Columns("A", "B", "C"), name => name));

        Assert.Contains("'IX_B'", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ProjectIndexes_MappedAwayColumn_DropsThatIndex()
    {
        IndexMetadata[] existing =
        [
            Index("PK", IndexKind.PrimaryKey, ("A", 0)),
            Index("IX_B", IndexKind.Normal, ("B", 1)),
            Index("IX_AB", IndexKind.Normal, ("A", 0), ("B", 1)),
            Index("UX_C", IndexKind.Normal, ("C", 2)) with { HasUniqueFlag = true, IgnoreNulls = true },
        ];

        List<IndexDefinition> projected = IndexHelpers.ProjectIndexes(existing, Columns("A", "C"), DropB);

        Assert.Equal(["PK", "UX_C"], projected.Select(index => index.Name));
        Assert.True(projected[0].IsPrimaryKey);
        Assert.Equal(["A"], projected[0].Columns);
        Assert.True(projected[1].IsUnique);
        Assert.True(projected[1].IgnoreNulls);
        Assert.Equal(["C"], projected[1].Columns);
    }

    /// <summary>
    /// An index that is dropped with a dropped column is dropped whatever its
    /// other key columns are, so a nameless key column beside the dropped one
    /// does not refuse the rewrite.
    /// </summary>
    [Fact]
    public void ProjectIndexes_EmptyKeyColumnNameBesideMappedAwayColumn_DropsThatIndex()
    {
        IndexMetadata[] existing =
        [
            Index("IX_A", IndexKind.Normal, ("A", 0)),
            Index("IX_PhantomB", IndexKind.Normal, (string.Empty, 0x02FF), ("B", 1)),
        ];

        List<IndexDefinition> projected = IndexHelpers.ProjectIndexes(existing, Columns("A", "C"), DropB);

        Assert.Equal(["IX_A"], projected.Select(index => index.Name));
    }

    private static string? DropB(string name) => string.Equals(name, "B", StringComparison.OrdinalIgnoreCase) ? null : name;

    private static List<ColumnDefinition> Columns(params string[] names) =>
        [.. names.Select(name => new ColumnDefinition(name, typeof(int)))];

    private static IndexMetadata Index(string name, IndexKind kind, params (string Name, int Number)[] keyColumns) => new()
    {
        Name = name,
        Kind = kind,
        Columns = [.. keyColumns.Select(column => new IndexColumnReference { Name = column.Name, ColumnNumber = column.Number })],
    };
}
