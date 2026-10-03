namespace JetDatabaseWriter.Tests.Relationships;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Reads every foreign-key logical-index entry in a database and checks the
/// links between them: each entry's <c>rel_tbl_page</c> and <c>rel_idx_num</c>
/// must name a partner entry that points back at it.
/// </summary>
internal static class ForeignKeyLinks
{
    /// <summary>
    /// Reads every FK logical-index entry that belongs to a relationship from
    /// every user table, following <c>rel_tbl_page</c> links to tables the
    /// catalog scan does not list, keyed by TDEF page. A linked page that is
    /// not a table definition maps to an empty list.
    /// </summary>
    /// <param name="stream">The database.</param>
    /// <returns>The FK entries of each TDEF page.</returns>
    public static async ValueTask<Dictionary<long, List<IndexMetadata>>> ReadForeignKeyEntriesAsync(MemoryStream stream)
    {
        stream.Position = 0;
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(stream, cancellationToken: TestContext.Current.CancellationToken);
        var pending = new Queue<long>();
        foreach (CatalogEntry entry in await harness.Services.TableCatalog.GetUserTablesAsync(TestContext.Current.CancellationToken))
        {
            pending.Enqueue(entry.TDefPage);
        }

        var result = new Dictionary<long, List<IndexMetadata>>();
        var seen = new HashSet<long>();
        while (pending.Count > 0)
        {
            long page = pending.Dequeue();
            if (!seen.Add(page))
            {
                continue;
            }

            TableDef? definition = await harness.ReadTableDefAsync(page, TestContext.Current.CancellationToken);
            byte[]? tdef = await harness.Database.ReadTDefBytesAsync(page, TestContext.Current.CancellationToken);
            if (definition is null || tdef is null)
            {
                result[page] = [];
                continue;
            }

            List<IndexMetadata> fks = [.. IndexCatalogReader.ReadMetadata(harness.Database, tdef, definition.Columns).Where(i => i.Kind == IndexKind.ForeignKey && i.IsForeignKey)];
            result[page] = fks;
            foreach (IndexMetadata fk in fks)
            {
                pending.Enqueue(fk.RelatedTablePage);
            }
        }

        return result;
    }

    /// <summary>Asserts that every FK entry names another entry that points back at it.</summary>
    /// <param name="fksByPage">The entries read by <see cref="ReadForeignKeyEntriesAsync"/>.</param>
    public static void AssertConsistent(Dictionary<long, List<IndexMetadata>> fksByPage)
    {
        List<string> broken = FindUnreciprocatedLinks(fksByPage);
        Assert.True(broken.Count == 0, string.Join(Environment.NewLine, broken));
    }

    /// <summary>
    /// Describes every FK entry whose partner does not point back at it,
    /// including an entry that names itself as its partner: the two sides of
    /// a relationship, self-referencing ones too, are always two entries.
    /// </summary>
    /// <param name="fksByPage">The entries read by <see cref="ReadForeignKeyEntriesAsync"/>.</param>
    /// <returns>One message per broken link; empty when every link is reciprocal.</returns>
    public static List<string> FindUnreciprocatedLinks(Dictionary<long, List<IndexMetadata>> fksByPage)
    {
        var broken = new List<string>();
        foreach ((long page, List<IndexMetadata> fks) in fksByPage)
        {
            foreach (IndexMetadata fk in fks)
            {
                bool namesItself = fk.RelatedTablePage == page && fk.RelatedIndexNumber == fk.IndexNumber;
                if (namesItself
                    || !fksByPage.TryGetValue(fk.RelatedTablePage, out List<IndexMetadata>? partner)
                    || !partner.Any(p => p.IndexNumber == fk.RelatedIndexNumber && p.RelatedTablePage == page && p.RelatedIndexNumber == fk.IndexNumber))
                {
                    broken.Add($"FK index '{fk.Name}' (#{fk.IndexNumber}) on TDEF page {page} points at page {fk.RelatedTablePage} entry #{fk.RelatedIndexNumber}, which does not point back{(namesItself ? " (it names itself)" : string.Empty)}.");
                }
            }
        }

        return broken;
    }

    /// <summary>Asserts that no FK entry names <paramref name="page"/> as its partner table.</summary>
    /// <param name="fksByPage">The entries read by <see cref="ReadForeignKeyEntriesAsync"/>.</param>
    /// <param name="page">The TDEF page no entry may name.</param>
    public static void AssertNoEntryNamesPage(Dictionary<long, List<IndexMetadata>> fksByPage, long page)
    {
        foreach ((long owner, List<IndexMetadata> fks) in fksByPage)
        {
            foreach (IndexMetadata fk in fks)
            {
                Assert.False(
                    fk.RelatedTablePage == page,
                    $"FK index '{fk.Name}' (#{fk.IndexNumber}) on TDEF page {owner} still names page {page}.");
            }
        }
    }
}
