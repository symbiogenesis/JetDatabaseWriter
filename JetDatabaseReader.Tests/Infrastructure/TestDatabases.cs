namespace JetDatabaseReader.Tests;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Xunit;

/// <summary>
/// Paths to the local test databases and shared MemberData helpers.
/// Tests are skipped automatically when the file does not exist on the machine.
/// </summary>
internal static class TestDatabases
{
    // ── Paths ────────────────────────────────────────────────────────

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
    /// ACCDB created by Access 16 CompactDatabase with password "secret".
    /// Header byte 0x62 = 0x07 (bits 0/1/2 set); version = 0x03 (Access 2010 format).
    /// The reader detects this as requiring a password (ACCDB AES check fires).
    /// Data pages are in ACE native format; password is stored via ACE internal scheme
    /// (not the Jet4 XOR scheme), so DecodeJet4Password does not return "secret".
    /// Once ACCDB AES/ACE password verification is implemented, opening with "secret"
    /// should succeed.
    /// </summary>
    public static readonly string AesEncrypted =
        Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Databases", "AesEncrypted.accdb");

    /// <summary>Jackcess V1997 (Jet 3 / Access 97) general-purpose test database.</summary>
    public static readonly string TestV1997 =
        Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Databases", "testV1997.mdb");

    /// <summary>Jackcess V2000 (Jet 4 / Access 2000) general-purpose test database.</summary>
    public static readonly string TestV2000 =
        Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Databases", "testV2000.mdb");

    /// <summary>Jackcess V2003 (Jet 4 / Access 2003) general-purpose test database.</summary>
    public static readonly string TestV2003 =
        Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Databases", "testV2003.mdb");

    /// <summary>Jackcess V2007 (ACE / Access 2007) general-purpose test database.</summary>
    public static readonly string TestV2007 =
        Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Databases", "testV2007.accdb");

    /// <summary>Jackcess V2010 (ACE / Access 2010) general-purpose test database.</summary>
    public static readonly string TestV2010 =
        Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Databases", "testV2010.accdb");

    /// <summary>Jackcess V2019 (ACE / Access 2019) extended date/time test database.</summary>
    public static readonly string ExtDateTestV2019 =
        Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Databases", "extDateTestV2019.accdb");

    /// <summary>Jackcess V2007 (ACE) fixed-length text test database – tables with text columns only (no OLE/MEMO).</summary>
    public static readonly string FixedTextTestV2007 =
        Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Databases", "fixedTextTestV2007.accdb");

    private static readonly string[] InRepoDatabases =
        [NorthwindTraders, AdventureWorks, Jet3Test, TestV1997, TestV2000, TestV2003, TestV2007, TestV2010, ExtDateTestV2019, FixedTextTestV2007];

    private static readonly ConcurrentDictionary<string, bool> _readableCache = new(StringComparer.OrdinalIgnoreCase);

    // ── MemberData sets (properties) ──────────────────────────────────

    /// <summary>Gets all databases (skips any that don't exist or can't be opened).</summary>
    public static TheoryData<string> All => ToTheoryData(
        InRepoDatabases.Where(IsReadable));

    /// <summary>Gets the smaller in-repo databases (skips any that can't be opened).</summary>
    public static TheoryData<string> Small => ToTheoryData(
        new[] { NorthwindTraders, AdventureWorks, Jet3Test, FixedTextTestV2007 }
            .Where(IsReadable));

    /// <summary>Gets the Jackcess test databases across multiple Access versions.</summary>
    public static TheoryData<string> Jackcess => ToTheoryData(
        new[] { TestV1997, TestV2000, TestV2003, TestV2007, TestV2010, ExtDateTestV2019 }
            .Where(IsReadable));

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

    public static AccessReader Open(string path, AccessReaderOptions? options = null) =>
        AccessReader.Open(path, options);

    /// <summary>Returns true when the file exists and can be opened by the reader (not encrypted, not corrupt).</summary>
    /// <returns></returns>
    internal static bool IsReadable(string path) =>
        _readableCache.GetOrAdd(path, static p =>
        {
            if (!File.Exists(p))
            {
                return false;
            }

            try
            {
                using var r = AccessReader.Open(p);
                return true;
            }
            catch (Exception ex) when (ex is IOException or InvalidDataException or UnauthorizedAccessException or JetLimitationException)
            {
                return false;
            }
        });

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
