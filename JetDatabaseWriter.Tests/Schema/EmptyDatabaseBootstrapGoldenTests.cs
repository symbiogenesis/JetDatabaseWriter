namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Pins the bytes the writer authors for a new database and for a new table,
/// as SHA-256 hashes captured on main before the wave-2 core split, with wave
/// 1 included. The refactors that move format knowledge, page I/O and TDEF
/// parsing must leave these bytes unchanged. A later change that alters them
/// on purpose updates the one hash it changes and names the changed bytes in
/// its commit.
/// </summary>
/// <remarks>
/// <para>
/// The bootstrap hashes cover <see cref="TDefPageBuilder.BuildEmptyDatabase"/>
/// for each format and catalog schema: the masked page 0, the global usage map
/// and the <c>MSysObjects</c> TDEF.
/// </para>
/// <para>
/// The table hashes cover the decrypted TDEF page chain of one
/// <c>CreateTableAsync</c> per format, on a database
/// <c>CreateDatabaseAsync</c> built in memory: every column type the format
/// can author (Text, Memo, Hyperlink, Binary, OLE, Boolean, Byte, Integer,
/// Long, AutoNumber, Single, Double, Date/Time, Currency, Decimal, GUID, and
/// on ACCDB also Large Number, Date/Time Extended, a calculated column, an
/// Attachment and a multi-value column) and two text indexes, one of them
/// unique over two columns.
/// </para>
/// <para>
/// On ACCDB the same <c>CreateTableAsync</c> also writes the hidden flat
/// tables of the Attachment and the multi-value column, and their TDEF chains
/// are pinned as separate hashes. They are the only TDEFs in the scenario
/// built through the column-descriptor overrides
/// (<c>ForceVariableLengthStorage</c>, <c>DescriptorFlagsOverride</c>,
/// <c>DescriptorExtraFlagsOverride</c> and <c>DescriptorNonTextMiscOverride</c>).
/// The GUID in a flat table's name is not in its TDEF, so the bytes are the
/// same on every run.
/// </para>
/// </remarks>
public sealed class EmptyDatabaseBootstrapGoldenTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, true, "D99ABF7F931CEFCCA40A76D2D7E341593992E0F1809F92D31AFA4D821E7A06BD")]
    [InlineData(DatabaseFormat.Jet3Mdb, false, "4F3DC4F579E518F807B5D53A9E3E934EBE7B8D18AF25C9A2477AA45001707358")]
    [InlineData(DatabaseFormat.Jet4Mdb, true, "F8A34D1D4232CB91CBB813650023D4ADAC24A0B0D639DF9DA8DB995FD1807152")]
    [InlineData(DatabaseFormat.Jet4Mdb, false, "74280E912941AA1DC2F8D8DC3635B68243AE7A834C03F4F2BEE7021CF39282D5")]
    [InlineData(DatabaseFormat.AceAccdb, true, "2BF119CCEEAA076A9AE7C2A64476707A1DA60AFFD8E72E2996E53F564D6D5DF0")]
    [InlineData(DatabaseFormat.AceAccdb, false, "70B7DF1BE440BBCFCB0DD3036506683095F5B1E9C91CE285E2A54789AC439A0B")]
    public void BuildEmptyDatabase_MatchesGoldenHash(DatabaseFormat format, bool fullCatalogSchema, string expectedSha256)
    {
        byte[] image = TDefPageBuilder.BuildEmptyDatabase(format, fullCatalogSchema);

        AssertGolden(expectedSha256, image, $"{format} bootstrap, full catalog: {fullCatalogSchema}");
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, "F622EC30B3810C08029732707404BB0EFE2EB480646B16EB147E759D1E139A98")]
    [InlineData(DatabaseFormat.Jet4Mdb, "9763A6A440BD89F28D7C43FC76149DA0D0B39EE847B25733FACD8600718C7DC9")]
    [InlineData(DatabaseFormat.AceAccdb, "F2D690BFBF470FE48C2C09AA32BF4BC87BE667217B526FB3208F7143707F0AC7")]
    public async Task CreateTable_EveryAuthorableTypeAndTwoTextIndexes_TDefMatchesGoldenHash(DatabaseFormat format, string expectedSha256)
    {
        await using MemoryStream stream = await CreateGoldenTableAsync(format);
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(stream, cancellationToken: Ct);
        CatalogEntry? entry = await harness.GetCatalogEntryAsync("Golden", Ct);
        Assert.NotNull(entry);

        byte[] chain = await ReadTDefChainPagesAsync(harness, entry.TDefPage);

        AssertGolden(expectedSha256, chain, $"{format} Golden TDEF chain");
    }

    [Theory]
    [InlineData("Files", "3CE2A501E5BB8EAAA2914F6C63541D67BEA34F8FF458443E6B5B2E6B5A134211")]
    [InlineData("Tags", "0827A884DE991779260A344AE8FE5F5921EE87D3D7D18C6DFD61EF27267AD441")]
    public async Task CreateTable_AccdbComplexColumn_FlatTableTDefMatchesGoldenHash(string column, string expectedSha256)
    {
        await using MemoryStream stream = await CreateGoldenTableAsync(DatabaseFormat.AceAccdb);
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(stream, cancellationToken: Ct);
        IReadOnlyList<ComplexColumnInfo> complex = await harness.Services.Schema.GetComplexColumnsAsync("Golden", Ct);
        ComplexColumnInfo info = Assert.Single(complex, c => c.ColumnName == column);
        Assert.True(info.FlatTableId > 0, $"The {column} column has no flat table.");

        byte[] chain = await ReadTDefChainPagesAsync(harness, CatalogValueReader.TdefPageFromId((long)info.FlatTableId));

        AssertGolden(expectedSha256, chain, $"AceAccdb {column} flat-table TDEF chain");
    }

    /// <summary>
    /// Creates a database of <paramref name="format"/> in memory and creates
    /// the <c>Golden</c> table in it: every authorable column type and two
    /// text indexes.
    /// </summary>
    /// <param name="format">The database format.</param>
    private static async Task<MemoryStream> CreateGoldenTableAsync(DatabaseFormat format)
    {
        var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream,
            format,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            Ct))
        {
            await writer.CreateTableAsync(
                "Golden",
                ColumnsFor(format),
                [
                    new IndexDefinition("IX_Name", "Name"),
                    new IndexDefinition("UX_CodeName", ["Code", "Name"]) { IsUnique = true },
                ],
                Ct);
        }

        return stream;
    }

    private static ColumnDefinition[] ColumnsFor(DatabaseFormat format)
    {
        var columns = new List<ColumnDefinition>
        {
            new("Id", typeof(int)) { IsAutoIncrement = true },
            new("Name", typeof(string), maxLength: 50),
            new("Code", typeof(string), maxLength: 10),
            new("Notes", typeof(string)),
            new("Link", typeof(Hyperlink)),
            new("Raw", typeof(byte[]), maxLength: 16),
            new("Blob", typeof(byte[])),
            new("Flag", typeof(bool)),
            new("Tiny", typeof(byte)),
            new("Small", typeof(short)),
            new("Whole", typeof(int)),
            new("Single", typeof(float)),
            new("Real", typeof(double)),
            new("When", typeof(DateTime)),
            new("Money", typeof(decimal)) { IsCurrency = true },
            new("Amount", typeof(decimal)) { NumericPrecision = 10, NumericScale = 2 },
            new("Key", typeof(Guid)),
        };

        if (format == DatabaseFormat.AceAccdb)
        {
            columns.Add(new("Big", typeof(long)));
            columns.Add(new("Stamp", typeof(DateTime)) { IsDateTimeExtended = true });
            columns.Add(new("Twice", typeof(int)) { IsCalculated = true, CalculationExpression = "[Whole] * 2" });
            columns.Add(new("Files", typeof(byte[])) { IsAttachment = true });
            columns.Add(new("Tags", typeof(object)) { IsMultiValue = true, MultiValueElementType = typeof(int) });
        }

        return [.. columns];
    }

    /// <summary>
    /// Reads every page of the TDEF chain rooted at <paramref name="tdefPage"/>
    /// (decrypted, headers included) and concatenates them in chain order.
    /// </summary>
    /// <param name="harness">The open reader harness.</param>
    /// <param name="tdefPage">The first TDEF page.</param>
    private static async ValueTask<byte[]> ReadTDefChainPagesAsync(ReaderHarness harness, long tdefPage)
    {
        var chain = new List<byte>();
        var visited = new HashSet<long>();
        long pageNumber = tdefPage;
        while (pageNumber > 0 && visited.Add(pageNumber))
        {
            byte[] page = await harness.ReadPageCopyAsync(pageNumber, Ct);
            Assert.Equal(0x02, page[0]);
            chain.AddRange(page);
            pageNumber = BitConverter.ToInt32(page, 4);
        }

        return [.. chain];
    }

    /// <summary>
    /// Compares the SHA-256 hash of <paramref name="bytes"/> with
    /// <paramref name="expectedSha256"/>, naming the whole actual hash in the
    /// failure message so a deliberate change can update it.
    /// </summary>
    /// <param name="expectedSha256">The golden hash, upper-case hex.</param>
    /// <param name="bytes">The bytes the writer authored.</param>
    /// <param name="what">What the bytes are, for the message.</param>
    private static void AssertGolden(string expectedSha256, byte[] bytes, string what)
    {
        string actual = Convert.ToHexString(SHA256.HashData(bytes));
        Assert.True(
            string.Equals(expectedSha256, actual, StringComparison.Ordinal),
            $"The {what} ({bytes.Length} bytes) changed: SHA-256 {actual}, golden {expectedSha256}. Update the golden hash only for a deliberate format change, and name the changed bytes in the commit.");
    }
}
