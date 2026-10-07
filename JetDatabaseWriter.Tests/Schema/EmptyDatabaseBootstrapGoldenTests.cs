namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Text;
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
/// Pins the bytes the writer authors for a new database and its tables.
/// Deliberate format corrections update the affected hashes and describe
/// the changed descriptors or page references in the fixing commit.
/// </summary>
/// <remarks>
/// <para>
/// The bootstrap hashes cover <see cref="TDefPageBuilder.BuildEmptyDatabase"/>
/// for each format: the masked page 0, the global usage map
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
    [InlineData(DatabaseFormat.Jet3Mdb, "D99ABF7F931CEFCCA40A76D2D7E341593992E0F1809F92D31AFA4D821E7A06BD")]
    [InlineData(DatabaseFormat.Jet4Mdb, "942B0938E09313C3D0E24DE79D1944201D9AAD875CA0ED5C2BE6AE3383A0FBB5")]
    [InlineData(DatabaseFormat.AceAccdb, "2BF119CCEEAA076A9AE7C2A64476707A1DA60AFFD8E72E2996E53F564D6D5DF0")]
    public void BuildEmptyDatabase_MatchesGoldenHash(DatabaseFormat format, string expectedSha256)
    {
        byte[] image = TDefPageBuilder.BuildEmptyDatabase(format);

        AssertGolden(expectedSha256, image, $"{format} bootstrap");
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, "2ABF6DDF372074E1699935BAD4CEB9D3268FBE65D42F3D857D486CE33A5347A9")]
    [InlineData(DatabaseFormat.Jet4Mdb, "E4DB83C4364CFF3460610F75334C41080DE7555E16AAE7438D116FA18836087E")]
    [InlineData(DatabaseFormat.AceAccdb, "5F8BEAF19B9917EFC865EB8A82FF9A733949A614153DB255DBF1ACFDF5A1C4C1")]
    public async Task CreateTable_EveryAuthorableTypeAndTwoTextIndexes_TDefMatchesGoldenHash(DatabaseFormat format, string expectedSha256)
    {
        await using MemoryStream stream = await CreateGoldenTableAsync(format);
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(stream, cancellationToken: Ct);
        CatalogEntry? entry = await harness.GetCatalogEntryAsync("Golden", Ct);
        Assert.NotNull(entry);

        byte[] chain = await ReadTDefChainPagesAsync(harness, entry.TDefPage);

        if (format == DatabaseFormat.AceAccdb)
        {
            NormalizeComplexIndexNames(chain);
        }

        AssertGolden(expectedSha256, chain, $"{format} Golden TDEF chain");
    }

    [Theory]
    [InlineData("Files", "28D0925A7E9B4A4075FB6B61EEA99DCC1D49664F7BC7E49AF88D1628EC7FFFBF")]
    [InlineData("Tags", "BEA5FE793536EFE2ABF9633E5AB53A560A7A4603EA89BB6E773755C113A0C6F5")]
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

    /// <summary>
    /// Normalizes only the generated GUID suffixes of the two native complex
    /// reference index names; their prefixes, lengths and all descriptor bytes
    /// remain covered by the golden hash.
    /// </summary>
    /// <param name="chain">The ACE table-definition bytes.</param>
    private static void NormalizeComplexIndexNames(byte[] chain)
    {
        string[] names = ["Files_", "Tags_"];
        byte[] canonical = Encoding.Unicode.GetBytes(new string('0', 32));
        foreach (string name in names)
        {
            byte[] prefix = Encoding.Unicode.GetBytes(name);
            int count = 0;
            for (int offset = 0; offset <= chain.Length - prefix.Length - canonical.Length; offset++)
            {
                if (!chain.AsSpan(offset, prefix.Length).SequenceEqual(prefix))
                {
                    continue;
                }

                string suffix = Encoding.Unicode.GetString(chain, offset + prefix.Length, canonical.Length);
                Assert.True(Guid.TryParseExact(suffix, "N", out _), $"Invalid complex reference index name: {name}{suffix}");
                canonical.CopyTo(chain, offset + prefix.Length);
                count++;
                offset += prefix.Length + canonical.Length - 1;
            }

            Assert.Equal(1, count);
        }
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
