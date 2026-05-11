namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Tests round-trip-openrecordset hypothesis H24: that the writer leaves the
/// "<c>misc_ext</c>" / text-sort-order-version field at column-descriptor
/// bytes 13-14 of the Jet4/ACE 25-byte column descriptor as <c>0x0000</c>
/// for TEXT/MEMO columns. Per mdbtools <c>HACKING.md</c> ("text sort order
/// version num is 2nd byte" of the misc_ext slot) and Jackcess
/// <c>ColumnImpl</c>, the bytes 11-14 four-byte slot for TEXT columns
/// encodes the LCID in the low word (bytes 11-12) and the sort-order
/// VERSION in the high word (bytes 13-14). Access 2010+ ACE files default
/// to the "General" sort order (version 1); legacy Jet4 / ACE 2007 use
/// "General Legacy" (version 0). The writer currently stamps
/// <c>0x00000409</c> as a single 4-byte little-endian write at offset 11,
/// which leaves bytes 13-14 == 0 (i.e. always version 0 / "General
/// Legacy"). DAO post-2010 may reject TEXT columns whose sort version
/// disagrees with the database's declared format, surfacing as
/// "Unrecognized database format ''.".
///
/// <para>Test strategy:</para>
/// <list type="number">
///   <item><see cref="DaoAuthoredFixture_TextSortVersion_Survey"/>
///     — establishes ground truth from the DAO-authored
///     <c>NorthwindTraders.accdb</c> fixture (an ACE 2007+ file): collects
///     the bytes-13-14 value of every TEXT/MEMO column descriptor across
///     every user table and asserts the dominant value, identifying
///     whether DAO writes 0 (General Legacy) or 1 (General).</item>
///   <item><see cref="WriterAuthored_TextSortVersion_MatchesDaoBaseline"/>
///     — runs the same byte-level check against a freshly writer-created
///     ACE database and reports the writer's bytes-13-14 value alongside
///     the ground-truth expectation. Skipped from
///     pass/fail expectations until the ground-truth scan completes; its
///     output is the H24 confirm/disconfirm signal.</item>
/// </list>
/// </summary>
public sealed class WriterColumnDescriptorTextSortOrderTests
{
    private const byte T_TEXT = 0x0A;
    private const byte T_MEMO = 0x0C;

    [Fact]
    public async Task DaoAuthoredFixture_TextSortVersion_Survey()
    {
        Assert.True(File.Exists(TestDatabases.NorthwindTraders), "NorthwindTraders.accdb fixture not found.");

        byte[] fileBytes = await File.ReadAllBytesAsync(
            TestDatabases.NorthwindTraders,
            TestContext.Current.CancellationToken);

        await using var ms = new MemoryStream(fileBytes, writable: false);
        await using var reader = await AccessReader.OpenAsync(
            ms,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        IReadOnlyList<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
        Assert.NotEmpty(tables);

        var allTextColumns = new List<(string Table, int ColIndex, byte ColType, ushort Lcid, ushort SortVersion)>();
        foreach (string tableName in tables)
        {
            var entry = await reader.GetCatalogEntryAsync(tableName, TestContext.Current.CancellationToken);
            Assert.NotNull(entry);

            allTextColumns.AddRange(ReadTextColumnSortInfo(fileBytes, (int)entry!.TDefPage, reader.PageSize, tableName));
        }

        Assert.NotEmpty(allTextColumns);

        // Distinct (LCID, sort-version) pairs across the entire fixture. A
        // homogeneous fixture means DAO always picks the same pair for the
        // file format under test.
        var distinct = allTextColumns
            .Select(c => (c.Lcid, c.SortVersion))
            .Distinct()
            .OrderBy(p => p.Lcid)
            .ThenBy(p => p.SortVersion)
            .ToArray();

        string distinctRender = string.Join(
            ", ",
            distinct.Select(p => $"(LCID=0x{p.Lcid:X4}, version={p.SortVersion})"));

        // Surface the survey result as a soft assertion: we expect the
        // ACE 2007+ fixture to use a single (LCID, version) pair across
        // every TEXT/MEMO column. If DAO's behaviour ever diverges, dump
        // every column rather than failing silently.
        if (distinct.Length != 1)
        {
            string columnDump = string.Join(
                "; ",
                allTextColumns.Select(c =>
                    $"{c.Table}.col[{c.ColIndex}](type=0x{c.ColType:X2}) LCID=0x{c.Lcid:X4} ver={c.SortVersion}"));
            Assert.Fail(
                $"Expected a single (LCID, sort-version) pair across all TEXT/MEMO columns of NorthwindTraders, "
                + $"found {distinct.Length}: [{distinctRender}]. Per-column: [{columnDump}].");
        }

        (ushort lcid, ushort version) = distinct[0];

        // Record the ground truth so future regressions on the writer side
        // can be diagnosed against this baseline.
        string lcidMessage =
            $"Expected en-US LCID 0x0409 in NorthwindTraders TEXT columns; observed 0x{lcid:X4}. "
            + $"Sort-order version observed = {version}.";
        Assert.True(lcid == 0x0409, lcidMessage);

        // The actual H24 signal: which version does ACE 2010+ DAO emit?
        // Per Jackcess: GENERAL_SORT_ORDER = (1033, 1); GENERAL_LEGACY_SORT_ORDER = (1033, 0).
        // We don't hard-fail either way; we encode the observation as a
        // distinguishing assertion so the test name + message tell the
        // story.
        Assert.True(
            version == 0 || version == 1,
            $"Unexpected sort-order version {version} for LCID 0x{lcid:X4} in NorthwindTraders.");
    }

    [Fact]
    public async Task WriterAuthored_TextSortVersion_MatchesDaoBaseline()
    {
        await using var ms = new MemoryStream();
        await using (var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "Customers",
                [
                    new("CustomerID", typeof(int)) { IsPrimaryKey = true, IsAutoIncrement = true, IsNullable = false },
                    new("FirstName", typeof(string), maxLength: 100),
                    new("LastName", typeof(string), maxLength: 100),
                    new("Notes", typeof(string), maxLength: int.MaxValue), // MEMO
                ],
                TestContext.Current.CancellationToken);
        }

        byte[] fileBytes = ms.ToArray();
        ms.Position = 0;

        await using var reader = await AccessReader.OpenAsync(
            ms,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        var entry = await reader.GetCatalogEntryAsync("Customers", TestContext.Current.CancellationToken);
        Assert.NotNull(entry);

        var rows = ReadTextColumnSortInfo(fileBytes, (int)entry!.TDefPage, reader.PageSize, "Customers").ToArray();
        Assert.NotEmpty(rows);

        // Pull the DAO-authored ground truth in-line so the failure message
        // is actionable on its own.
        byte[] daoBytes = await File.ReadAllBytesAsync(
            TestDatabases.NorthwindTraders,
            TestContext.Current.CancellationToken);

        await using var daoMs = new MemoryStream(daoBytes, writable: false);
        await using var daoReader = await AccessReader.OpenAsync(
            daoMs,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        IReadOnlyList<string> daoTables = await daoReader.ListTablesAsync(TestContext.Current.CancellationToken);
        var daoTextCols = new List<(string Table, int ColIndex, byte ColType, ushort Lcid, ushort SortVersion)>();
        foreach (string tableName in daoTables)
        {
            var daoEntry = await daoReader.GetCatalogEntryAsync(tableName, TestContext.Current.CancellationToken);
            if (daoEntry is null)
            {
                continue;
            }

            daoTextCols.AddRange(ReadTextColumnSortInfo(daoBytes, (int)daoEntry.TDefPage, daoReader.PageSize, tableName));
        }

        ushort expectedVersion = daoTextCols
            .GroupBy(c => c.SortVersion)
            .OrderByDescending(g => g.Count())
            .First()
            .Key;
        ushort expectedLcid = daoTextCols
            .GroupBy(c => c.Lcid)
            .OrderByDescending(g => g.Count())
            .First()
            .Key;

        var mismatches = rows
            .Where(r => r.SortVersion != expectedVersion || r.Lcid != expectedLcid)
            .Select(r => $"col[{r.ColIndex}](type=0x{r.ColType:X2}) LCID=0x{r.Lcid:X4} ver={r.SortVersion}")
            .ToArray();

        string failureMessage =
            $"H24 reproduced: writer-authored Customers TDEF stamps a different (LCID, sort-version) than DAO. "
            + $"DAO baseline (mode across NorthwindTraders TEXT/MEMO columns): LCID=0x{expectedLcid:X4} version={expectedVersion}. "
            + $"Writer mismatches: [{string.Join("; ", mismatches)}]. "
            + $"Fix: in JetDatabaseWriter/Schema/TDefPageBuilder.cs the TEXT/MEMO branch currently writes "
            + $"Wi32(page, o + MiscOff, 0x00000409), zeroing bytes 13-14 (sort-order version). "
            + $"For ACE 2010+ this should be 0x00010409 (LCID 0x0409, version 1 = 'General' sort).";

        Assert.True(mismatches.Length == 0, failureMessage);
    }

    /// <summary>
    /// Walks the column descriptors of a Jet4/ACE single-page user-table TDEF
    /// and yields one tuple per TEXT (0x0A) or MEMO (0x0C) column carrying
    /// the LCID (bytes 11-12 of the descriptor) and the sort-order version
    /// (bytes 13-14). Reads only one TDEF page; callers should pick fixtures
    /// whose TDEF fits in one 4096-byte page.
    /// </summary>
    private static IEnumerable<(string Table, int ColIndex, byte ColType, ushort Lcid, ushort SortVersion)> ReadTextColumnSortInfo(
        byte[] fileBytes,
        int tdefPage,
        int pageSize,
        string tableName)
    {
        int off = tdefPage * pageSize;

        // Jet4 TDEF header layout matches ReadColumnNumberPairs in
        // WriterColumnDescriptorRedundantColNumTests.
        int numCols = BinaryPrimitives.ReadUInt16LittleEndian(fileBytes.AsSpan(off + 45, 2));
        int numRealIdx = BinaryPrimitives.ReadInt32LittleEndian(fileBytes.AsSpan(off + 51, 4));
        int colStart = off + 63 + (numRealIdx * 12);

        for (int i = 0; i < numCols; i++)
        {
            int desc = colStart + (i * 25);
            byte colType = fileBytes[desc + 0];
            if (colType != T_TEXT && colType != T_MEMO)
            {
                continue;
            }

            ushort lcid = BinaryPrimitives.ReadUInt16LittleEndian(fileBytes.AsSpan(desc + 11, 2));
            ushort sortVersion = BinaryPrimitives.ReadUInt16LittleEndian(fileBytes.AsSpan(desc + 13, 2));
            yield return (tableName, i, colType, lcid, sortVersion);
        }
    }
}
