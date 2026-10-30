namespace JetDatabaseWriter.Tests.Indexes.Collation;

using System.Collections.Generic;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes.Collation;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Production collation metadata and dispatch agree with Access-authored index keys.</summary>
public sealed class TextSortOrderFixtureTests
{
    public static TheoryData<string, bool, byte> Families => new()
    {
        { TestDatabases.TestIndexCodesV1997, false, 0 },
        { TestDatabases.TestIndexCodesV2000, true, 0 },
        { TestDatabases.TestIndexCodesV2003, true, 0 },
        { TestDatabases.TestIndexCodesV2007, true, 0 },
        { TestDatabases.TestIndexCodesV2010, true, 1 },
    };

    /// <summary>Gets the fixtures with at least one supported index.</summary>
    public static TheoryData<string> DispatchFixtures =>
    [
        TestDatabases.TestUnicodeCompV2003,
        TestDatabases.TestIndexCodesV1997,
        TestDatabases.TestIndexCodesV2000,
        TestDatabases.TestIndexCodesV2003,
        TestDatabases.TestIndexCodesV2007,
        TestDatabases.TestIndexCodesV2010,
        TestDatabases.FixedTextTestV2000,
        TestDatabases.FixedTextTestV2003,
        TestDatabases.FixedTextTestV2007,
        TestDatabases.FixedTextTestV2010,
    ];

    [Theory]
    [MemberData(nameof(Families))]
    public async Task HeaderAndColumnSortOrders_MatchFixtureFamily(string fixturePath, bool hasVersion, byte version)
    {
        await using AccessReader harness = await TestDatabase.OpenReaderAsync(fixturePath, cancellationToken: TestContext.Current.CancellationToken);
        var expected = new TextSortOrder(0x0409, version, hasVersion);
        Assert.Equal(expected, harness.Database.Format.DefaultTextSortOrder);
        List<CatalogEntry> tables = await harness.Services.Catalog.GetUserTablesAsync(TestContext.Current.CancellationToken);
        int columnsChecked = 0;
        foreach (CatalogEntry entry in tables)
        {
            TableDef definition = await harness.Database.TableDefs.ReadRequiredTableDefAsync(entry.TDefPage, entry.Name, TestContext.Current.CancellationToken);
            IReadOnlyList<ColumnMetadata> metadata = await harness.GetColumnMetadataAsync(entry.Name, TestContext.Current.CancellationToken);
            foreach (ColumnInfo column in definition.Columns)
            {
                if (column.Type is ColumnType.TextType or ColumnType.MemoType)
                {
                    Assert.Equal(expected, column.TextSortOrder);
                    ColumnMetadata exposed = Assert.Single(metadata, value => value.Name == column.Name);
                    TextCollation expectedCollation = (hasVersion, version) switch
                    {
                        (false, _) => TextCollation.General97,
                        (_, 0) => TextCollation.GeneralLegacy,
                        _ => TextCollation.General,
                    };
                    Assert.Equal(expectedCollation, exposed.TextCollation);
                    Assert.Equal((ushort)0x0409, exposed.TextSortOrderId);
                    Assert.Equal(hasVersion ? version : null, exposed.TextSortOrderVersion);
                    columnsChecked++;
                }
            }
        }

        Assert.True(columnsChecked > 0);
    }

    [Theory]
    [MemberData(nameof(DispatchFixtures))]
    public Task ColumnSortOrderDispatch_MatchesEveryStoredTextKey(string fixturePath)
        => TextIndexEncoderFixtureHarness.ValidateSortOrdersAsync(fixturePath, TestContext.Current.CancellationToken);
}
