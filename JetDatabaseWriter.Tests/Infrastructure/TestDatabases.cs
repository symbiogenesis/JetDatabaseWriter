namespace JetDatabaseWriter.Tests;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

/// <summary>
/// Paths to the local test databases and shared MemberData helpers.
/// Tests are skipped automatically when the file does not exist on the machine.
/// </summary>
internal static class TestDatabases
{
    /// <summary>The password required to open <see cref="AesEncrypted"/>.</summary>
    public const string AesEncryptedPassword = "secret";

    // ── In-repo (project-owned) databases ────────────────────────────

    public static readonly string NorthwindTraders =
        Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Databases", "NorthwindTraders.accdb");

    public static readonly string AdventureWorks =
        Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Databases", "AdventureLT2008.mdb");

    public static readonly string Jet3Test =
        Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Databases", "Jet3Test.mdb");

    /// <summary>
    /// ACCDB with a table "Documents" that has an Attachment column (type 0x11)
    /// containing two rows, each with one attachment file attached.
    /// Created by Access 16 COM automation.
    /// Password: none. Tables: Documents, Tags.
    /// </summary>
    public static readonly string ComplexFields =
        Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Databases", "ComplexFields.accdb");

    /// <summary>
    /// ACCDB created by Access 16 CompactDatabase with password.
    /// Header byte 0x62 = 0x07 (bits 0/1/2 set); version = 0x03 (Access 2010 format).
    /// The reader detects this as requiring a password (ACCDB AES check fires).
    /// Data pages are in ACE native format; password is stored via ACE internal scheme
    /// (not the Jet4 XOR scheme). Opening with password via
    /// <see cref="OpenAsync"/> succeeds (handled automatically by that helper).
    /// </summary>
    public static readonly string AesEncrypted =
        Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Databases", "AesEncrypted.accdb");

    // ── Jackcess fixtures (Databases/Jackcess/) ──────────────────────
    // The full upstream Jackcess test/data tree, mirrored under Databases/Jackcess/.
    // See Databases/Jackcess/THIRD-PARTY-NOTICES.txt for license + provenance.

    private static string Jc(string relPath) =>
        Path.Combine(
            AppDomain.CurrentDomain.BaseDirectory,
            "Databases",
            "Jackcess",
            relPath.Replace('/', Path.DirectorySeparatorChar));

    /// <summary>
    /// Builds a path to a versioned Jackcess fixture: <c>V{version}/{baseName}V{version}.{ext}</c>,
    /// where the extension is <c>.mdb</c> for V2003 and earlier and <c>.accdb</c> for V2007+.
    /// </summary>
    private static string Jc(int version, string baseName)
    {
        string ext = version <= 2003 ? "mdb" : "accdb";
        return Jc($"V{version}/{baseName}V{version}.{ext}");
    }

    /// <summary>Gets the folder containing the Jackcess test fixture tree (V1997/, V2000/, V2003/, V2007/, V2010/, V2019/, root files).</summary>
    public static string JackcessRoot =>
        Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Databases", "Jackcess");

    // ── mdbtools fixtures (Databases/mdbtools/) ──────────────────────
    // Mirrored from https://github.com/mdbtools/mdbtestdata/tree/master/data.
    // Provides a cross-implementation conformance signal — i.e. that this
    // library can read every fixture mdbtools' own test suite reads.
    // See Databases/mdbtools/THIRD-PARTY-NOTICES.txt for provenance.

    private static string Mt(string fileName) =>
        Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Databases", "mdbtools", fileName);

    /// <summary>Northwind sample from mdbtestdata; Jet 4 .mdb. Contains the German-named "Umsätze" table mdbtools uses for codepage tests.</summary>
    public static readonly string MdbtoolsNwind = Mt("nwind.mdb");

    /// <summary>Asset-tracking sample ACCDB from mdbtestdata. Contains the "Asset Items" table and the "qryCostsSummedByOwner" stored query.</summary>
    public static readonly string MdbtoolsASampleDatabase = Mt("ASampleDatabase.accdb");

    /// <summary>Small database created to regression-test mdbtools' date parser.</summary>
    public static readonly string MdbtoolsDateTestDatabase = Mt("DateTestDatabase.mdb");

    // Root-level Jackcess fixtures
    public static readonly string AdoxJet4 = Jc("adox_jet4.mdb");
    public static readonly string LinkeeTest = Jc("linkeeTest.accdb");
    public static readonly string Test2BinData = Jc("test2BinData.dat");

    // V1997 — Jet 3 / Access 97
    public static readonly string CompIndexTestV1997 = Jc(1997, "compIndexTest");
    public static readonly string DelColTestV1997 = Jc(1997, "delColTest");
    public static readonly string DelTestV1997 = Jc(1997, "delTest");
    public static readonly string IndexTestV1997 = Jc(1997, "indexTest");
    public static readonly string OverflowTestV1997 = Jc(1997, "overflowTest");
    public static readonly string QueryTestV1997 = Jc(1997, "queryTest");
    public static readonly string Test2V1997 = Jc(1997, "test2");
    public static readonly string TestIndexCodesV1997 = Jc(1997, "testIndexCodes");
    public static readonly string TestV1997 = Jc(1997, "test");

    // V2000 — Jet 4 / Access 2000
    public static readonly string BigIndexTestV2000 = Jc(2000, "bigIndexTest");
    public static readonly string CompIndexTestV2000 = Jc(2000, "compIndexTest");
    public static readonly string DelColTestV2000 = Jc(2000, "delColTest");
    public static readonly string DelTestV2000 = Jc(2000, "delTest");
    public static readonly string FixedNumericTestV2000 = Jc(2000, "fixedNumericTest");
    public static readonly string FixedTextTestV2000 = Jc(2000, "fixedTextTest");
    public static readonly string IndexCursorTestV2000 = Jc(2000, "indexCursorTest");
    public static readonly string IndexTestV2000 = Jc(2000, "indexTest");
    public static readonly string OverflowTestV2000 = Jc(2000, "overflowTest");
    public static readonly string QueryTestV2000 = Jc(2000, "queryTest");
    public static readonly string Test2V2000 = Jc(2000, "test2");
    public static readonly string TestIndexCodesV2000 = Jc(2000, "testIndexCodes");
    public static readonly string TestIndexPropertiesV2000 = Jc(2000, "testIndexProperties");
    public static readonly string TestPromotionV2000 = Jc(2000, "testPromotion");
    public static readonly string TestRefGlobalV2000 = Jc(2000, "testRefGlobal");
    public static readonly string TestV2000 = Jc(2000, "test");

    // V2003 — Jet 4 / Access 2003
    public static readonly string BigIndexTestV2003 = Jc(2003, "bigIndexTest");
    public static readonly string CompIndexTestV2003 = Jc(2003, "compIndexTest");
    public static readonly string DelColTestV2003 = Jc(2003, "delColTest");
    public static readonly string DelTestV2003 = Jc(2003, "delTest");
    public static readonly string FixedNumericTestV2003 = Jc(2003, "fixedNumericTest");
    public static readonly string FixedTextTestV2003 = Jc(2003, "fixedTextTest");
    public static readonly string IndexCursorTestV2003 = Jc(2003, "indexCursorTest");
    public static readonly string IndexTestV2003 = Jc(2003, "indexTest");
    public static readonly string OverflowTestV2003 = Jc(2003, "overflowTest");
    public static readonly string QueryTestV2003 = Jc(2003, "queryTest");
    public static readonly string Test2V2003 = Jc(2003, "test2");
    public static readonly string TestIndexCodesV2003 = Jc(2003, "testIndexCodes");
    public static readonly string TestIndexPropertiesV2003 = Jc(2003, "testIndexProperties");
    public static readonly string TestPromotionV2003 = Jc(2003, "testPromotion");
    public static readonly string TestUnicodeCompV2003 = Jc(2003, "testUnicodeComp");
    public static readonly string TestV2003 = Jc(2003, "test");

    // V2007 — ACE / Access 2007
    public static readonly string BigIndexTestV2007 = Jc(2007, "bigIndexTest");
    public static readonly string CompIndexTestV2007 = Jc(2007, "compIndexTest");
    public static readonly string ComplexDataTestV2007 = Jc(2007, "complexDataTest");
    public static readonly string DelColTestV2007 = Jc(2007, "delColTest");
    public static readonly string DelTestV2007 = Jc(2007, "delTest");
    public static readonly string FixedNumericTestV2007 = Jc(2007, "fixedNumericTest");
    public static readonly string FixedTextTestV2007 = Jc(2007, "fixedTextTest");
    public static readonly string IndexCursorTestV2007 = Jc(2007, "indexCursorTest");
    public static readonly string IndexTestV2007 = Jc(2007, "indexTest");
    public static readonly string LinkerTestV2007 = Jc(2007, "linkerTest");
    public static readonly string OdbcLinkerTestV2007 = Jc(2007, "odbcLinkerTest");
    public static readonly string OldDatesV2007 = Jc(2007, "oldDates");
    public static readonly string OverflowTestV2007 = Jc(2007, "overflowTest");
    public static readonly string QueryTestV2007 = Jc(2007, "queryTest");
    public static readonly string Test2V2007 = Jc(2007, "test2");
    public static readonly string TestIndexCodesV2007 = Jc(2007, "testIndexCodes");
    public static readonly string TestIndexPropertiesV2007 = Jc(2007, "testIndexProperties");
    public static readonly string TestOleV2007 = Jc(2007, "testOle");
    public static readonly string TestPromotionV2007 = Jc(2007, "testPromotion");
    public static readonly string TestV2007 = Jc(2007, "test");
    public static readonly string UnsupportedFieldsTestV2007 = Jc(2007, "unsupportedFieldsTest");

    // V2010 — ACE / Access 2010
    public static readonly string BigIndexTestV2010 = Jc(2010, "bigIndexTest");
    public static readonly string BinIdxTestV2010 = Jc(2010, "binIdxTest");
    public static readonly string CalcFieldTestV2010 = Jc(2010, "calcFieldTest");
    public static readonly string CompIndexTestV2010 = Jc(2010, "compIndexTest");
    public static readonly string ComplexDataTestV2010 = Jc(2010, "complexDataTest");
    public static readonly string DelColTestV2010 = Jc(2010, "delColTest");
    public static readonly string DelTestV2010 = Jc(2010, "delTest");
    public static readonly string FixedNumericTestV2010 = Jc(2010, "fixedNumericTest");
    public static readonly string FixedTextTestV2010 = Jc(2010, "fixedTextTest");
    public static readonly string IndexCursorTestV2010 = Jc(2010, "indexCursorTest");
    public static readonly string IndexTestV2010 = Jc(2010, "indexTest");
    public static readonly string OverflowTestV2010 = Jc(2010, "overflowTest");
    public static readonly string QueryTestV2010 = Jc(2010, "queryTest");
    public static readonly string Test2V2010 = Jc(2010, "test2");
    public static readonly string TestEmoticonsV2010 = Jc(2010, "testEmoticons");
    public static readonly string TestIndexCodesV2010 = Jc(2010, "testIndexCodes");
    public static readonly string TestIndexPropertiesV2010 = Jc(2010, "testIndexProperties");
    public static readonly string TestPromotionV2010 = Jc(2010, "testPromotion");
    public static readonly string TestV2010 = Jc(2010, "test");

    // V2019 — ACE / Access 2019
    public static readonly string ExtDateTestV2019 = Jc(2019, "extDateTest");

    // ── Curated lists ────────────────────────────────────────────────

    private static readonly string[] InRepoDatabases =
        [NorthwindTraders, AdventureWorks, Jet3Test, ComplexFields, AesEncrypted];

    /// <summary>All Jackcess fixtures (mdb + accdb only; excludes the binary blob test2BinData.dat).</summary>
    public static readonly string[] AllJackcessDatabases =
    [
        AdoxJet4,
        LinkeeTest,

        // V1997
        CompIndexTestV1997, DelColTestV1997, DelTestV1997, IndexTestV1997, OverflowTestV1997,
        QueryTestV1997, Test2V1997, TestIndexCodesV1997, TestV1997,

        // V2000
        BigIndexTestV2000, CompIndexTestV2000, DelColTestV2000, DelTestV2000, FixedNumericTestV2000,
        FixedTextTestV2000, IndexCursorTestV2000, IndexTestV2000, OverflowTestV2000, QueryTestV2000,
        Test2V2000, TestIndexCodesV2000, TestIndexPropertiesV2000, TestPromotionV2000,
        TestRefGlobalV2000, TestV2000,

        // V2003
        BigIndexTestV2003, CompIndexTestV2003, DelColTestV2003, DelTestV2003, FixedNumericTestV2003,
        FixedTextTestV2003, IndexCursorTestV2003, IndexTestV2003, OverflowTestV2003, QueryTestV2003,
        Test2V2003, TestIndexCodesV2003, TestIndexPropertiesV2003, TestPromotionV2003,
        TestUnicodeCompV2003, TestV2003,

        // V2007
        BigIndexTestV2007, CompIndexTestV2007, ComplexDataTestV2007, DelColTestV2007, DelTestV2007,
        FixedNumericTestV2007, FixedTextTestV2007, IndexCursorTestV2007, IndexTestV2007,
        LinkerTestV2007, OdbcLinkerTestV2007, OldDatesV2007, OverflowTestV2007, QueryTestV2007,
        Test2V2007, TestIndexCodesV2007, TestIndexPropertiesV2007, TestOleV2007, TestPromotionV2007,
        TestV2007, UnsupportedFieldsTestV2007,

        // V2010
        BigIndexTestV2010, BinIdxTestV2010, CalcFieldTestV2010, CompIndexTestV2010,
        ComplexDataTestV2010, DelColTestV2010, DelTestV2010, FixedNumericTestV2010,
        FixedTextTestV2010, IndexCursorTestV2010, IndexTestV2010, OverflowTestV2010,
        QueryTestV2010, Test2V2010, TestEmoticonsV2010, TestIndexCodesV2010,
        TestIndexPropertiesV2010, TestPromotionV2010, TestV2010,

        // V2019
        ExtDateTestV2019,
    ];

    private static readonly ConcurrentDictionary<string, bool> _readableCache = new(StringComparer.OrdinalIgnoreCase);

    // ── MemberData sets (properties) ──────────────────────────────────

    /// <summary>Gets all in-repo + baseline Jackcess databases (skips any that don't exist or can't be opened).</summary>
    public static TheoryData<string> All =>
        ToTheoryData(InRepoDatabases.Where(IsReadable));

    /// <summary>Gets the smaller in-repo databases (skips any that can't be opened).</summary>
    public static TheoryData<string> Small => ToTheoryData(
        new[] { NorthwindTraders, AdventureWorks, Jet3Test, FixedTextTestV2007 }
            .Where(IsReadable));

    /// <summary>Gets the baseline Jackcess test databases (one per Access version).</summary>
    public static TheoryData<string> Jackcess => ToTheoryData(
        new[] { TestV1997, TestV2000, TestV2003, TestV2007, TestV2010, ExtDateTestV2019 }
            .Where(IsReadable));

    /// <summary>
    /// Gets every Jackcess fixture that exists on disk and can be opened by the reader.
    /// Use for broad-coverage round-trip tests; expect this to skip
    /// any fixture that requires reader features not yet implemented (encrypted,
    /// query-only, calculated fields, etc.).
    /// </summary>
    public static TheoryData<string> JackcessAll => ToTheoryData(
        AllJackcessDatabases.Where(IsReadable));

    /// <summary>
    /// Gets every Jackcess fixture that exists on disk, regardless of whether
    /// the reader can open it. Use this for tests that assert specific failure
    /// modes (e.g. "calcFieldTestV2010 throws JetLimitationException").
    /// </summary>
    public static TheoryData<string> JackcessAllExisting => ToTheoryData(
        AllJackcessDatabases.Where(File.Exists));

    /// <summary>
    /// Gets all known database files that exist on disk, without an IsReadable check.
    /// Use this when you need to assert something about files that may fail to open
    /// (e.g., verifying they are not password-protected).
    /// </summary>
    public static TheoryData<string> AllExisting => ToTheoryData(
        InRepoDatabases.Where(File.Exists));

    // ── Helpers (methods) ─────────────────────────────────────────────

    /// <summary>
    /// Returns a skip reason string when the file is missing, or null when it exists.
    /// Use with <c>Skip = SkipIfMissing(path)</c> on [Fact].
    /// </summary>
    /// <returns></returns>
    public static string? SkipIfMissing(string path) =>
        File.Exists(path) ? null : $"Test database not found: {path}";

    public static ValueTask<AccessReader> OpenAsync(string path, AccessReaderOptions? options = null, CancellationToken cancellationToken = default)
    {
        // Auto-supply the known password for the encrypted fixture so MemberData-driven
        // tests (e.g. AllExisting) can open it without each test having to know the
        // password. Callers that want to test the password path explicitly should call
        // AccessReader.OpenAsync directly.
        if (options is null && string.Equals(path, AesEncrypted, StringComparison.OrdinalIgnoreCase))
        {
            options = new AccessReaderOptions { Password = SecureStringTestHelper.FromString(TestDatabases.AesEncryptedPassword) };
        }

        return AccessReader.OpenAsync(path, options, cancellationToken);
    }

    /// <summary>Returns true when the file exists and can be opened by the reader (not encrypted, not corrupt).</summary>
    internal static bool IsReadable(string path) =>
        _readableCache.GetOrAdd(path, static p =>
        {
            if (!File.Exists(p))
            {
                return false;
            }

            try
            {
                return Task.Run(async () =>
                {
                    await using var r = await AccessReader.OpenAsync(p, new AccessReaderOptions { UseLockFile = false });
                    return true;
                }).GetAwaiter().GetResult();
            }
            catch (Exception ex)
                when (ex is IOException or InvalidDataException or UnauthorizedAccessException or JetLimitationException)
            {
                return false;
            }
        });

    /// <summary>Returns true when the file exists and can be opened by the reader (not encrypted, not corrupt).</summary>
    internal static async ValueTask<bool> IsReadableAsync(string path)
    {
        if (_readableCache.TryGetValue(path, out bool cached))
        {
            return cached;
        }

        if (!File.Exists(path))
        {
            _readableCache.TryAdd(path, false);
            return false;
        }

        try
        {
            await using var r = await AccessReader.OpenAsync(path, new AccessReaderOptions { UseLockFile = false });
            _readableCache.TryAdd(path, true);
            return true;
        }
        catch (Exception ex) when (ex is IOException or InvalidDataException or UnauthorizedAccessException or JetLimitationException)
        {
            _readableCache.TryAdd(path, false);
            return false;
        }
    }

    private static TheoryData<string> ToTheoryData(IEnumerable<string> paths)
    {
        var data = new TheoryData<string>();
        foreach (string p in paths)
        {
            data.Add(p);
        }

        return data;
    }
}
