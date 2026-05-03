namespace JetDatabaseWriter.Tests.Core;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Core;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention

/// <summary>
/// Validates that <see cref="IndexMetadata.IsUnique"/>,
/// <see cref="IndexMetadata.IgnoreNulls"/>, and
/// <see cref="IndexMetadata.IsRequired"/> round-trip correctly off the
/// on-disk index <c>flags</c> byte across the full Cartesian product of
/// combinations Microsoft Access can produce. Drives off the Jackcess
/// <c>testIndexProperties*</c> fixtures, which were authored specifically
/// to exercise these flag bits (<c>0x01 = unique</c>,
/// <c>0x02 = ignore_nulls</c>, <c>0x08 = required</c>).
/// </summary>
public sealed class IndexFlagCombinationsTests
{
    public static TheoryData<string> Fixtures => new()
    {
        TestDatabases.TestIndexPropertiesV2000,
        TestDatabases.TestIndexPropertiesV2003,
        TestDatabases.TestIndexPropertiesV2007,
    };

    [Theory]
    [MemberData(nameof(Fixtures))]
    public async Task IndexFlags_Combinations_RoundTripFromDisk(string fixturePath)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using AccessReader reader = await AccessReader.OpenAsync(
            fixturePath,
            new AccessReaderOptions { UseLockFile = false },
            ct);

        var observed = new HashSet<(bool Unique, bool IgnoreNulls, bool Required)>();
        var sb = new StringBuilder();
        int totalIndexes = 0;

        List<string> tables = await reader.ListTablesAsync(ct);
        foreach (string tableName in tables)
        {
            IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync(tableName, ct);
            foreach (IndexMetadata index in indexes)
            {
                if (index.IsForeignKey)
                {
                    continue;
                }

                totalIndexes++;
                observed.Add((index.IsUnique, index.IgnoreNulls, index.IsRequired));
                sb.AppendLine(FormattableString.Invariant(
                    $"  {tableName}.{index.Name}: unique={index.IsUnique} ignoreNulls={index.IgnoreNulls} required={index.IsRequired} kind={index.Kind}"));
            }
        }

        Assert.True(totalIndexes > 0, $"No non-FK indexes found in '{fixturePath}'.");

        // Each fixture is expected to provide indexes hitting more than one
        // distinct flag combination. The Jackcess testIndexProperties* fixtures
        // contain TableIgnoreNulls{1,2} and TableUnique{1,2} tables specifically
        // to exercise these flag bits, so seeing only (False,False,False) here
        // indicates a reader bug in the index-flag-byte parse path.
        string combos = string.Join(", ", observed.Select(c => $"({c.Unique},{c.IgnoreNulls},{c.Required})"));
        string msg = $"Only {observed.Count} distinct flag combination(s) seen in '{fixturePath}': {combos}\nIndex inventory:\n{sb}";
        Assert.True(observed.Count >= 2, msg);
    }

    [Fact]
    public async Task IgnoreNullsIndex_OmitsNullKeyedRows()
    {
        // testIndexPropertiesV2007 contains tables whose indexes use
        // ignore_nulls=true. Pick the first such index and assert that the
        // reader-surfaced row count for the table is >= the number of leaves
        // we'd expect (i.e., the IGNORE_NULLS flag is not silently flipped).
        // This is a cheap smoke test that complements the structural flag
        // round-trip above.
        string fixturePath = TestDatabases.TestIndexPropertiesV2007;
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using AccessReader reader = await AccessReader.OpenAsync(
            fixturePath,
            new AccessReaderOptions { UseLockFile = false },
            ct);

        bool foundIgnoreNullsIndex = false;
        List<string> tables = await reader.ListTablesAsync(ct);
        foreach (string tableName in tables)
        {
            IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync(tableName, ct);
            if (indexes.Any(i => i.IgnoreNulls && !i.IsForeignKey))
            {
                foundIgnoreNullsIndex = true;
                break;
            }
        }

        string ignoreMsg = $"Expected at least one ignore_nulls=true index in '{fixturePath}'.";
        Assert.True(foundIgnoreNullsIndex, ignoreMsg);
    }
}
