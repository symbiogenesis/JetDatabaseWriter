namespace JetDatabaseWriter.Tests.Encryption;

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// <para>
/// Pins what the library decrypts from one database in each encrypted format
/// it reads, so that a change to a page cipher, an Office Crypto container or
/// the open path cannot change a decrypted byte unnoticed.
/// </para>
/// <para>
/// The six fixtures under <c>Databases/Encrypted/</c> were written once, at
/// <c>af1ca4d</c>, and are frozen: never regenerate them, or every digest
/// below changes with the new creation date. Each started as a database the
/// writer created, a Jet4 <c>.mdb</c> for <see cref="GoldenFixture.Jet4Rc4"/>
/// and an ACE <c>.accdb</c> for the other five, holding table <c>T</c>
/// (<c>Id</c> Long, <c>Body</c> Memo) with three rows, the second a
/// 20,480-character Memo stored on LVAL pages. Each was then encrypted by
/// <c>AccessWriter.EncryptAsync</c> with the password
/// <see cref="TestDatabases.EncryptedFixturePassword"/>. Jet3 XOR has no
/// <see cref="AccessEncryptionFormat"/> member and no encrypt API, so it is
/// built here from <see cref="TestDatabases.Jet3Test"/> with
/// <see cref="Jet3Jet4EncryptionTests.ApplyXorMask"/>.
/// </para>
/// <para>
/// Every format decrypts to its source database: the recorded plaintext
/// digest is the source file's, page 0 included, and every page from 1 on
/// reads as the source's page. The five ACE formats therefore share one set
/// of digests.
/// </para>
/// </summary>
public sealed class EncryptedGoldenFixtureTests
{
    /// <summary>The 20,480-character Memo of row 2, long enough to need LVAL pages.</summary>
    private static readonly string LongMemo = BuildLongMemo();

    /// <summary>One encrypted fixture, named by its format.</summary>
    [SuppressMessage("Design", "CA1515:Consider making public types internal", Justification = "Theory parameters of public xUnit test methods must be public.")]
    public enum GoldenFixture
    {
        /// <summary>Jet3Test.mdb with the Jet3 page XOR mask and flag, built by the test.</summary>
        Jet3Xor = 0,

        /// <summary><c>Databases/Encrypted/Jet4Rc4.mdb</c>.</summary>
        Jet4Rc4 = 1,

        /// <summary><c>Databases/Encrypted/AccdbLegacyPassword.accdb</c>.</summary>
        AccdbLegacyPassword = 2,

        /// <summary><c>Databases/Encrypted/AccdbAesCfbWrapped.accdb</c>.</summary>
        AccdbAesCfbWrapped = 3,

        /// <summary><c>Databases/Encrypted/AccdbAgile.accdb</c>, flat Agile.</summary>
        AccdbAgile = 4,

        /// <summary><c>Databases/Encrypted/AccdbAgileCfb.accdb</c>.</summary>
        AccdbAgileCfb = 5,

        /// <summary><c>Databases/Encrypted/AccdbStandard.accdb</c>.</summary>
        AccdbStandard = 6,
    }

    /// <summary>Gets every fixture.</summary>
    public static TheoryData<GoldenFixture> Fixtures
    {
        get
        {
            var data = new TheoryData<GoldenFixture>();
            foreach (GoldenFixture fixture in Enum.GetValues<GoldenFixture>())
            {
                data.Add(fixture);
            }

            return data;
        }
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static ReadOnlyMemory<char> Password => TestDatabases.EncryptedFixturePassword.AsMemory();

    [Theory]
    [MemberData(nameof(Fixtures))]
    public async Task ReadDecrypted_FrozenFixture_MatchesRecordedPlaintextDigest(GoldenFixture fixture)
    {
        await using var source = new MemoryStream(await LoadFixtureAsync(fixture), writable: false);

        (byte[] plaintext, AccessEncryptionFormat sourceFormat) = await EncryptionConverter.ReadDecryptedAsync(source, Password, Ct);

        Assert.Equal(ExpectedFormat(fixture), sourceFormat);
        Assert.Equal(Recorded(fixture).PlaintextSha256, Sha256Hex(plaintext));
    }

    [Theory]
    [MemberData(nameof(Fixtures))]
    public async Task ReadPage_FrozenFixture_EveryPageMatchesRecordedDigest(GoldenFixture fixture)
    {
        await using var source = new MemoryStream(await LoadFixtureAsync(fixture), writable: false);

        List<string> digests = await ReadPageDigestsAsync(source);

        Assert.Equal(Recorded(fixture).PageSha256, digests);
    }

    [Theory]
    [MemberData(nameof(Fixtures))]
    public async Task Reader_FrozenFixture_ReadsRecordedRows(GoldenFixture fixture)
    {
        await using var source = new MemoryStream(await LoadFixtureAsync(fixture), writable: false);
        var options = new AccessReaderOptions { UseLockFile = false, Password = Password };
        await using AccessReader reader = await AccessReader.OpenAsync(source, options, leaveOpen: true, Ct);

        (string Table, object[][] Rows)[] expected = fixture == GoldenFixture.Jet3Xor ? Golden.Jet3TestRows : Golden.WriterCreatedRows;
        Assert.Equal(expected.Select(static table => table.Table), await reader.ListTablesAsync(Ct));
        foreach ((string table, object[][] rows) in expected)
        {
            var actual = new List<object[]>();
            await foreach (object[] row in reader.Rows(table, cancellationToken: Ct))
            {
                actual.Add(row);
            }

            Assert.Equal(rows, actual.OrderBy(static row => (int)row[0]).ToArray());
        }
    }

    [Theory]
    [MemberData(nameof(Fixtures))]
    public async Task Detect_FrozenFixture_ReturnsFormat(GoldenFixture fixture)
    {
        await using var source = new MemoryStream(await LoadFixtureAsync(fixture), writable: false);
        Assert.Equal(ExpectedFormat(fixture), await AccessWriter.DetectEncryptionFormatAsync(source, Ct));

        if (fixture != GoldenFixture.Jet3Xor)
        {
            Assert.Equal(ExpectedFormat(fixture), await AccessWriter.DetectEncryptionFormatAsync(FixturePath(fixture), Ct));
        }
    }

    /// <summary>Returns the bytes of <paramref name="fixture"/>, building Jet3 XOR from Jet3Test.mdb.</summary>
    /// <param name="fixture">The fixture.</param>
    private static async Task<byte[]> LoadFixtureAsync(GoldenFixture fixture)
    {
        if (fixture != GoldenFixture.Jet3Xor)
        {
            return await File.ReadAllBytesAsync(FixturePath(fixture), Ct);
        }

        byte[] data = await File.ReadAllBytesAsync(TestDatabases.Jet3Test, Ct);
        Jet3Jet4EncryptionTests.ApplyXorMask(data, EncryptionManager.Jet3PageXorMask);
        Jet3Jet4EncryptionTests.SetJet3EncryptionFlag(data);
        return data;
    }

    /// <summary>
    /// Opens <paramref name="source"/> the way <see cref="AccessReader"/>
    /// does, first unwrapping an Office Crypto container or a flat Agile file
    /// into its decrypted image, and returns the SHA-256 of every page from 1
    /// on, read through the database file's page reads.
    /// </summary>
    /// <param name="source">The encrypted database.</param>
    private static async Task<List<string>> ReadPageDigestsAsync(Stream source)
    {
        byte[] header = await PageFile.ReadHeaderAsync(source, Ct);
        byte[]? decrypted = await EncryptionManager.TryDecryptAgileCompoundFileAsync(
            source,
            header,
            Password,
            EncryptionManager.ReaderPasswordOption,
            Ct);

        await using var image = new MemoryStream(decrypted ?? [], writable: false);
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(
            decrypted is null ? source : image,
            new AccessReaderOptions { UseLockFile = false, Password = Password },
            leaveOpen: true,
            Ct);

        var digests = new List<string>();
        for (long page = 1; page < harness.Database.PageCount; page++)
        {
            digests.Add(Sha256Hex(await harness.ReadPageCopyAsync(page, Ct)));
        }

        return digests;
    }

    private static string Sha256Hex(byte[] bytes) => Convert.ToHexString(SHA256.HashData(bytes));

    private static string FixturePath(GoldenFixture fixture) => fixture switch
    {
        GoldenFixture.Jet3Xor => throw new ArgumentException("Jet3 XOR is built by the test, not stored.", nameof(fixture)),
        GoldenFixture.Jet4Rc4 => TestDatabases.EncryptedJet4Rc4,
        GoldenFixture.AccdbLegacyPassword => TestDatabases.EncryptedAccdbLegacyPassword,
        GoldenFixture.AccdbAesCfbWrapped => TestDatabases.EncryptedAccdbAesCfbWrapped,
        GoldenFixture.AccdbAgile => TestDatabases.EncryptedAccdbAgile,
        GoldenFixture.AccdbAgileCfb => TestDatabases.EncryptedAccdbAgileCfb,
        GoldenFixture.AccdbStandard => TestDatabases.EncryptedAccdbStandard,
        _ => throw new ArgumentOutOfRangeException(nameof(fixture), fixture, null),
    };

    /// <summary>
    /// Returns the format detection and <c>ReadDecryptedAsync</c> report.
    /// Jet3 XOR has no <see cref="AccessEncryptionFormat"/> member, so both
    /// report it as <see cref="AccessEncryptionFormat.None"/>, although the
    /// reader removes the mask.
    /// </summary>
    /// <param name="fixture">The fixture.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="fixture"/> is not a <see cref="GoldenFixture"/> member.</exception>
    private static AccessEncryptionFormat ExpectedFormat(GoldenFixture fixture) => fixture switch
    {
        GoldenFixture.Jet3Xor => AccessEncryptionFormat.None,
        GoldenFixture.Jet4Rc4 => AccessEncryptionFormat.Jet4Rc4,
        GoldenFixture.AccdbLegacyPassword => AccessEncryptionFormat.AccdbLegacyPassword,
        GoldenFixture.AccdbAesCfbWrapped => AccessEncryptionFormat.AccdbAesCfbWrapped,
        GoldenFixture.AccdbAgile => AccessEncryptionFormat.AccdbAgile,
        GoldenFixture.AccdbAgileCfb => AccessEncryptionFormat.AccdbAgileCfb,
        GoldenFixture.AccdbStandard => AccessEncryptionFormat.AccdbStandard,
        _ => throw new ArgumentOutOfRangeException(nameof(fixture), fixture, null),
    };

    private static (string PlaintextSha256, string[] PageSha256) Recorded(GoldenFixture fixture) => fixture switch
    {
        GoldenFixture.Jet3Xor => (Golden.Jet3PlaintextSha256, Golden.Jet3PageSha256),
        GoldenFixture.Jet4Rc4 => (Golden.Jet4PlaintextSha256, Golden.Jet4PageSha256),
        GoldenFixture.AccdbLegacyPassword or GoldenFixture.AccdbAesCfbWrapped or GoldenFixture.AccdbAgile
            or GoldenFixture.AccdbAgileCfb or GoldenFixture.AccdbStandard => (Golden.AcePlaintextSha256, Golden.AcePageSha256),
        _ => throw new ArgumentOutOfRangeException(nameof(fixture), fixture, null),
    };

    private static string BuildLongMemo()
    {
        var text = new StringBuilder(20_480 + 64);
        for (int line = 0; text.Length < 20_480; line++)
        {
            _ = text.Append("Line ")
                .Append(line.ToString("D4", CultureInfo.InvariantCulture))
                .Append(": the quick brown fox jumps over the lazy dog.\r\n");
        }

        return text.ToString(0, 20_480);
    }

    /// <summary>What the fixtures held when they were frozen, at <c>af1ca4d</c>.</summary>
    private static class Golden
    {
        /// <summary>SHA-256 of Jet3Test.mdb, which the Jet3 XOR fixture decrypts to.</summary>
        internal const string Jet3PlaintextSha256 = "4E3863F433E0FDD47B703EA6307C287A1534D942835AA729CE621A2628C08123";

        /// <summary>SHA-256 of the writer-created Jet4 source of Jet4Rc4.mdb.</summary>
        internal const string Jet4PlaintextSha256 = "A2C3E1D3270D2927F1735CC546391BC7334329F6A61166D83B4826CB9E42DB11";

        /// <summary>SHA-256 of the writer-created ACE source of the five ACE fixtures.</summary>
        internal const string AcePlaintextSha256 = "D62DB73FBEB8ECA886FC05922A959050C6DD7CCA1863EECC9C2DB10DE7A37494";

        /// <summary>The rows of table T in every writer-created fixture, by Id.</summary>
        internal static readonly (string Table, object[][] Rows)[] WriterCreatedRows =
        [
            ("T",
            [
                [1, "First row"],
                [2, LongMemo],
                [3, "Third row"],
            ]),
        ];

        /// <summary>The rows of Jet3Test.mdb, by table in catalog order and then by Id.</summary>
        internal static readonly (string Table, object[][] Rows)[] Jet3TestRows =
        [
            ("Products",
            [
                [1, "Widget", 9.99m, true, new DateTime(2024, 1, 15)],
                [2, "Gadget", 24.5m, true, new DateTime(2024, 2, 20)],
                [3, "Doohickey", 4.75m, false, new DateTime(2024, 3, 10)],
                [4, "Thingamajig", 15m, true, new DateTime(2024, 4, 5)],
                [5, "Whatchamacallit", 31.25m, false, new DateTime(2024, 5, 1)],
            ]),
            ("Categories",
            [
                [1, "Electronics", "Devices and components"],
                [2, "Hardware", "Tools and fasteners"],
                [3, "Misc", "Everything else that does not fit in other categories"],
            ]),
        ];

        /// <summary>SHA-256 of Jet3Test.mdb's pages 1 to 28.</summary>
        internal static readonly string[] Jet3PageSha256 =
        [
            "53D360B49F65E1F464D1952991A944D372D353E34D90165BA9265C486C353979",
            "C03BD1369727A3C2A93EFAC1C7BF0E2842F9C20A695DC4AEB055179CF921D70A",
            "BC848695B18735212371AF18FA2BFAB318BC42FEF10FC8C212A7DAECC5C9227E",
            "93B10CF9506355DF09B3C50F27BA24F432FE2D3B97EE0D4F432C3D99C37097DE",
            "111C3F044036E5BC1FA25DCE0C5F39703160A263E48D5F16229DCB78CCE6C679",
            "830B92E913A7D2969337818E4184716D7C015231427EA37049B0ED428CEF5EF6",
            "E26469500C570DF7EA957A0F1CD4D406481532AC4C195C4F96AFA76AF26BF2D6",
            "BFA92CF8873B2789722EC0767948F9766097BDC6C3F82EB8CF081BE793A95836",
            "D6796524094CE5A3C53A97E273144A274241601E38E1A6A8214D1E5DA47E2653",
            "59D9D40629B5B46750E96CB6576E897E151C881F484FE1905BD9A8BD2A286B28",
            "E5C94567B7B052C2A235B4C3BD0228626D4602DF844F1A6FE707BBCEB1BF69C6",
            "AB2F35A8DA937C11937EEFD3B6C4F86D8E19E7B9FFDA1A8EA1D5FC4936289BFB",
            "2C6527F0A60EDBE82706B5E35EF5363FA7DC06DB8EBA1E1DB2F9F0A44BCC2F88",
            "D8A723D150D52B46B5233E8EC13B8F8005FC0E6764278C139C1C9E5088318798",
            "F54C77DB9972DEF007572B9A64CA7ECB413266DBAB2C72AE2B4E91A7A39B5513",
            "F54C77DB9972DEF007572B9A64CA7ECB413266DBAB2C72AE2B4E91A7A39B5513",
            "F54C77DB9972DEF007572B9A64CA7ECB413266DBAB2C72AE2B4E91A7A39B5513",
            "C0732FF1684E6BA53F9CC32206F9CC16645B4D0BD2ECA90EB4A93B4577456CCB",
            "D5731EA02F20FFDBC754B11DAE2DBCF705C01BE00282B6224120C0633F118914",
            "1A537E4461B3E1DDCCE50245C140E1D30BBAD71F361FE6E954660096D2B7B137",
            "1E2EDF3A883FBFA5A17819393D121491A4EEEAC0B0EC5C403D6C610ABCCF6D70",
            "BBDDE49ED95501D170FB5375B69CA712A21C73909EC9D88E181D99B4EC38B158",
            "0D6FB8EB4688F3190186CEE267A8FDF85A3B55AA5FC2CE623B789DCA588AEBAA",
            "641042AEF05A67FFD7B77E57F48AB814F632F279D9F61CD9ADE284146849B870",
            "D66EB7105D9030695B12C1783D96DBEF552A9D0766663D9BF6357A55EC126585",
            "F3DC2365ED731167395F96EDE0B9121EC55C42E060DA47E0658F4F968B9B32EC",
            "23F9FF448CB453259D9746819120BEED053DFE6B2F7C88A8A0BD4AC9A11D485A",
            "6641D26903D9A738662E6ADF1B4BA50353A908EFDBFCEAAFDF6B6A8C24582046",
        ];

        /// <summary>SHA-256 of the Jet4 source's pages 1 to 15.</summary>
        internal static readonly string[] Jet4PageSha256 =
        [
            "53B5C9703F8957BD658A112A960A7FEC43BC19C5246E019124BBD9687D72585E",
            "723F0B110718ADC9CCE58124F094859A3C4701EA53CA232A15C212D8E03D7EAB",
            "5D805D163ADED823445E8D5F7412877356D97D85F2E05C7FD51641C29872ACBB",
            "E02D032690AAA1839830B2CE1D3F83874ADD3E9C35EC69E7668718E925CB250A",
            "081B65BA70A199D5271EE528D918C3D679F7236A73BF0AD7E1DEFEB29BD64D9B",
            "77E85183A72B41A8679C6EC115F32367BA8750AF8DD114E0BFDF0DFB1C44DBB5",
            "8331AE966EFB2DA284075F9FDA34123208D72755BE66A1360713EA4A945DF27B",
            "32086650628B9FCC35F3CE851EC0563FF2C15AAF79674FAA1CA94FCE4CA7684B",
            "FBCC4FE19277F5A1097F332E3686BF9043EC51217CAD5CC0A41D6A09B7D37F03",
            "9DF7B3F5F8E4E74C1F60FA2B3100410268344D1800694C7C8774877E90040F0F",
            "BE709F24297754C38802758303A429906E50B7A1AF0C849185F54A26314A9548",
            "80C8AF7331984DB6F300FD64FBF0DF75C88582716554A6887DA69093DDF0488A",
            "D73A77BB532764937287D2D38B2196C6252BE6BF368ABCFCFDF40915A58B0F9C",
            "260949EE25499BDE448AB78D572F988C90353F3DC5FF19CB66C43ECBB68EAAA2",
            "D0CF8DE7282AC40DEA707D00D35C0AD35C9C05B8A73DC26ED8A9CCE36C4DA60E",
        ];

        /// <summary>SHA-256 of the ACE source's pages 1 to 41.</summary>
        internal static readonly string[] AcePageSha256 =
        [
            "53B5C9703F8957BD658A112A960A7FEC43BC19C5246E019124BBD9687D72585E",
            "D126EEEE48F378078290B334860F18405C778CC86609266E3A2B47B909D70510",
            "E2E7551EE8C13A10C8ED73D29FD2A0E2E06170F1E405CC8B08C1C9F7397E6E8C",
            "ED3D546657B254E8500E47719B0283347FA9F30E3D6A802A3EA52FF1488074BC",
            "14742433C79D7A834663B8D4FADE37562B7D808EAD6DE5B792B0556282AAC053",
            "A77A8B1A04B5F804DF40EEF7B40D54A8EED065179508880186A8FCB6AC7D57ED",
            "3E2AAC8B3116F7136E1F561EDD58460BAF4235D81F3FE288DCBA96A49FBC383F",
            "04AD152164477231F6E5386EB1942BD9FA5954A46355596B49783CB9021B937C",
            "28F351D74121CAEB142550BCF5991339E7EA67C00893D4E55426339768321084",
            "BBE9533E2E2A8C6BA0FB4C98A0E09FB2F3452B3DA63EDB5C8FCDEFEFE9814612",
            "73CCB77BAD2BFBB93E24FAE037F7E345E5B41631A1E550D0D06D942108801B88",
            "0476376006EA9AC1605FB268A56BFC332C4C143C1383739B90C9CA74B12D6042",
            "2CB25D659EEBB69D53348E4ADEE3258DED0BEB91D1F55F60307E774EC15D6B84",
            "0791B4AA4DC12A5D229B1DE558FFB757048554F85FB0DAB51EF3E2687014D2F0",
            "0791B4AA4DC12A5D229B1DE558FFB757048554F85FB0DAB51EF3E2687014D2F0",
            "0791B4AA4DC12A5D229B1DE558FFB757048554F85FB0DAB51EF3E2687014D2F0",
            "7C272E32185B08052BBA855554FAC045EEE9A782F5E00F7BBEF49DAC463CAD64",
            "F1C00B2938F2E6F14B48DD836A59229CEA63077885E11A85F382A37BCA1C379D",
            "C142297F85F03775E745056175CB49FD05575CD75EFF85AA2FD26AB77ACEE9FE",
            "C142297F85F03775E745056175CB49FD05575CD75EFF85AA2FD26AB77ACEE9FE",
            "C142297F85F03775E745056175CB49FD05575CD75EFF85AA2FD26AB77ACEE9FE",
            "6BEE5DDA624098C6F4783C780DFFF4BA1761A3D633ADEC53A8989CD7B5D10808",
            "217B5E412805034CCC66CB32F18B0A8958079ABA65528A1E82341465405E538C",
            "7AEB9F18FC43D9ED116DA9D71F313D53E54552D06BECF5F242CCDA1C25A2E500",
            "7F1CDB95EA0D17669950DA72233E7B189EE3FE7005219690A428B89669B80832",
            "99511F1A4A93F760245DD63B9AAC4FD6DFC40B5D121C62A929FF723AB4F8D57C",
            "15F60FDADD4E4049C7F469E43189308963CB1B564868BF38AE584F187B361521",
            "CD22D12BFE639352402BFD05E2B38C807E6EDFAD6C10C9F7ACBA7CE05DB27C18",
            "70A4E84240A82C48A29A154C4A22FB0A6CA4A9EFAE2559E3294D0A7498644C44",
            "AC8777FBD75AAA14E6F2D22111FC7769181A21FCCE17DAA23D6CD0A4E1FD6C62",
            "EBFDC3139C4DBB7C3802EC5887EC06B29BDB1E8638DC4B0C08B873A74CF8E68C",
            "FC73877A4D5E0C56E2BB50E397DFFF23E86ED54CB5DE9C0FD590317D199368A4",
            "B13EC3F0444771040DD68CBE9C799D3AB424CFC7686BC888DCAC7B42BFD2C007",
            "C98A83CC80F904A4721C155721311834F029EC4ECDA49BB84E33C20FD82FBD53",
            "409119245064E889A56AC486FA779D23D92E249836667E25628E500A2D303ECF",
            "9DF7B3F5F8E4E74C1F60FA2B3100410268344D1800694C7C8774877E90040F0F",
            "D68069EBF0A370CB1D00891E17D41204DFE6FE6420A068DC69A40ECE580B47F4",
            "14ED025B2A29FA092D38583A43FDD1F6EFC3778C28220F4AB06977843B42E59D",
            "C8C433960D101A6FBA71161B3967C8036B869EFE0F044A4CD5336EB6A807D69F",
            "519B936535F1A95EFFFED2F54823DF7C592A63411F002269384EB8498C88AED6",
            "56CF5B840EE401165A56FDE55BEA390715EE419CCFEC605995135F81A4D35626",
        ];
    }
}
