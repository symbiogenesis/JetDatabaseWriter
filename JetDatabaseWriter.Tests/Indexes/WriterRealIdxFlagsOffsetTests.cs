namespace JetDatabaseWriter.Tests.Indexes;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Tests round-trip-openrecordset hypothesis H23: that the writer places the
/// real-idx <c>flags</c> byte at offset 46 within the 52-byte Jet4/ACE
/// physical descriptor, while ground-truth (DAO-authored) ACCDBs place it
/// at offset 42 (per mdbtools <c>HACKING.md</c>).
///
/// <para>
/// Diagnostic strategy: per Jackcess <c>IndexData.UNKNOWN_INDEX_FLAG = 0x80</c>
/// is set unconditionally by Microsoft Access on every real-idx slot. That
/// gives us a byte-pattern fingerprint for locating the actual flags byte —
/// scan every real-idx slot in DAO-authored <c>NorthwindTraders.accdb</c> and
/// observe which candidate offset (42 vs 46) consistently has the <c>0x80</c>
/// bit set. Whichever offset wins is where DAO writes <c>flags</c>; the other
/// offset belongs to the surrounding "unknown" filler bytes.
/// </para>
/// </summary>
public sealed class WriterRealIdxFlagsOffsetTests
{
    private const int Jet4RealIdxPhysSize = 52;
    private const int CandidateOffset_Mdbtools = 42;
    private const int CandidateOffset_Jackcess = 46;
    private const byte UnknownIndexFlag = 0x80;

    /// <summary>
    /// DAO ground truth: dumps every real-idx slot's bytes at offsets 42 and
    /// 46 across every user table in <c>NorthwindTraders.accdb</c> and reports
    /// which candidate offset consistently has the <c>0x80</c> "unknown
    /// always-set" flag bit. The winner is where DAO physically stamps
    /// <c>flags</c>. The test asserts a clear winner emerges — failure here
    /// would mean both candidates are wrong and the search must broaden.
    /// </summary>
    [Fact]
    public async Task DaoAuthoredFixture_FlagsByte_LocatedByUnknownAlwaysSetBit()
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

        int pageSize = reader.PageSize;
        IReadOnlyList<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        int slotCount = 0;
        int hitsAt42 = 0;
        int hitsAt46 = 0;
        var sample = new StringBuilder();

        foreach (string tableName in tables)
        {
            var entry = await reader.GetCatalogEntryAsync(tableName, TestContext.Current.CancellationToken);
            Assert.NotNull(entry);

            var physOffsets = LocateRealIdxPhysOffsets(fileBytes, (int)entry!.TDefPage, pageSize);
            for (int i = 0; i < physOffsets.Count; i++)
            {
                int phys = physOffsets[i];
                byte at42 = fileBytes[phys + CandidateOffset_Mdbtools];
                byte at46 = fileBytes[phys + CandidateOffset_Jackcess];
                slotCount++;
                if ((at42 & UnknownIndexFlag) != 0)
                {
                    hitsAt42++;
                }

                if ((at46 & UnknownIndexFlag) != 0)
                {
                    hitsAt46++;
                }

                if (sample.Length < 1024)
                {
                    sample.AppendFormat(
                        CultureInfo.InvariantCulture,
                        "{0}[{1}]: phys=0x{2:X} byte42=0x{3:X2} byte46=0x{4:X2}; ",
                        tableName,
                        i,
                        phys,
                        at42,
                        at46);
                }
            }
        }

        Assert.True(slotCount > 0, "Expected at least one real-idx slot in NorthwindTraders user tables.");

        string summary =
            $"DAO ground-truth survey across {slotCount} real-idx slots: "
            + $"0x80 bit set at offset 42 in {hitsAt42}/{slotCount} slots; "
            + $"0x80 bit set at offset 46 in {hitsAt46}/{slotCount} slots. "
            + $"Sample: [{sample}]";

        // Per Jackcess, every real-idx slot must have the 0x80 unknown flag
        // set. Whichever offset consistently shows that pattern is the true
        // flags location. We require ONE of the two candidates to match
        // every slot, and the other to NOT match every slot — anything else
        // means the hypothesis itself is wrong.
        bool offset42IsFlags = hitsAt42 == slotCount && hitsAt46 != slotCount;
        bool offset46IsFlags = hitsAt46 == slotCount && hitsAt42 != slotCount;

        string inconclusiveMessage =
            "H23 inconclusive: neither offset 42 nor offset 46 consistently has the 0x80 'unknown always-set' bit. "
            + "Either Microsoft Access does not stamp 0x80 on every slot (Jackcess assumption wrong), or the flags "
            + "byte lives at a third offset, or the LocateRealIdxPhysOffsets walker is mis-locating the descriptor "
            + "block. " + summary;
        Assert.True(offset42IsFlags || offset46IsFlags, inconclusiveMessage);

        // Record which offset DAO uses so the writer test below can assert
        // the writer agrees with ground truth (not with mdbtools docs).
        int daoFlagsOffset = offset42IsFlags ? CandidateOffset_Mdbtools : CandidateOffset_Jackcess;
        const string Fmt = "H23 CONFIRMED: DAO writes the real-idx flags byte at offset {0} (NOT the writer's current offset {1}). The writer must move the flags stamp from Constants.TableDefinition.Jet4.RealIdx.FlagsOffset = 46 to {0}. {2}";
        string h23Message = string.Format(CultureInfo.InvariantCulture, Fmt, daoFlagsOffset, CandidateOffset_Jackcess, summary);
        Assert.True(daoFlagsOffset == CandidateOffset_Jackcess, h23Message);
    }

    /// <summary>
    /// Writer-side check: a freshly created table's real-idx flags byte must
    /// land at the same offset DAO uses (verified by the ground-truth test
    /// above). Reads every real-idx slot in the writer-authored TDEF and
    /// asserts <c>0x80</c> is set at the DAO offset (46 if H23 is
    /// disconfirmed, 42 if confirmed).
    /// </summary>
    [Fact]
    public async Task WriterAuthored_FlagsByte_PositionedAtDaoOffset()
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
                    new("Name", typeof(string), maxLength: 100) { IsNullable = false },
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

        int pageSize = reader.PageSize;
        var entry = await reader.GetCatalogEntryAsync("Customers", TestContext.Current.CancellationToken);
        Assert.NotNull(entry);

        var physOffsets = LocateRealIdxPhysOffsets(fileBytes, (int)entry!.TDefPage, pageSize);
        Assert.NotEmpty(physOffsets);

        var sample = new StringBuilder();
        foreach (int phys in physOffsets)
        {
            byte at42 = fileBytes[phys + CandidateOffset_Mdbtools];
            byte at46 = fileBytes[phys + CandidateOffset_Jackcess];
            sample.AppendFormat(
                CultureInfo.InvariantCulture,
                "phys=0x{0:X} byte42=0x{1:X2} byte46=0x{2:X2}; ",
                phys,
                at42,
                at46);
        }

        // Writer must put 0x80 at the same offset DAO does. Today the writer
        // stamps at 46. If the DAO test above proved 46 is correct, this
        // test passes; if the DAO test proved 42 is correct, this test fails
        // and points at the bytes to move.
        bool writerOffset42 = physOffsets.All(p => (fileBytes[p + CandidateOffset_Mdbtools] & UnknownIndexFlag) != 0);
        bool writerOffset46 = physOffsets.All(p => (fileBytes[p + CandidateOffset_Jackcess] & UnknownIndexFlag) != 0);

        string writerMessage =
            "Writer did not set the 0x80 'unknown always-set' bit at either candidate offset on every real-idx slot. "
            + "Sample: [" + sample + "]";
        Assert.True(writerOffset42 || writerOffset46, writerMessage);

        int writerFlagsOffset = writerOffset46 ? CandidateOffset_Jackcess : CandidateOffset_Mdbtools;
        Assert.Equal(CandidateOffset_Jackcess, writerFlagsOffset);
    }

    /// <summary>
    /// Walks a Jet4/ACE TDEF header and returns the absolute byte offsets of
    /// each real-idx physical descriptor (52 bytes apiece). Single-page TDEFs
    /// only — sufficient for the small synthetic tables used here and for
    /// every NorthwindTraders user table.
    /// </summary>
    private static List<int> LocateRealIdxPhysOffsets(byte[] fileBytes, int tdefPage, int pageSize)
    {
        int off = tdefPage * pageSize;

        // Jet4 TDEF header: numCols at +45 (uint16), numRealIdx at +51 (int32),
        // logical-idx block starts at +63 and is numRealIdx * 12 bytes,
        // followed by numCols * 25-byte column descriptors, followed by
        // length-prefixed UCS-2 column names, followed by the real-idx
        // physical descriptors (52 bytes each).
        int numCols = BinaryPrimitives.ReadUInt16LittleEndian(fileBytes.AsSpan(off + 45, 2));
        int numRealIdx = BinaryPrimitives.ReadInt32LittleEndian(fileBytes.AsSpan(off + 51, 4));
        int colStart = off + 63 + (numRealIdx * 12);
        int namePos = colStart + (numCols * 25);
        for (int c = 0; c < numCols; c++)
        {
            int nameLen = BinaryPrimitives.ReadUInt16LittleEndian(fileBytes.AsSpan(namePos, 2));
            namePos += 2 + nameLen;
        }

        var offsets = new List<int>(numRealIdx);
        for (int i = 0; i < numRealIdx; i++)
        {
            offsets.Add(namePos + (i * Jet4RealIdxPhysSize));
        }

        return offsets;
    }
}
