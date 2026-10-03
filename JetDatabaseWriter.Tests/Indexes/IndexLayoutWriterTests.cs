namespace JetDatabaseWriter.Tests.Indexes;

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Helpers;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// The format-aware TDEF index-section writers (<see cref="IndexLayout.WriteRealIdxDescriptor"/>,
/// <see cref="IndexLayout.WriteLogicalEntry"/>, <see cref="JetFormat.EncodeTDefNameRecord"/>)
/// round-trip through the readers and reproduce the foreign-key entries
/// Access wrote in indexTestV1997.mdb (Jet3) and indexTestV2000.mdb (Jet4).
/// </summary>
/// <param name="cache">Caches the Access-authored fixtures.</param>
public sealed class IndexLayoutWriterTests(DatabaseCache cache) : IClassFixture<DatabaseCache>
{
    private readonly CancellationToken ct = TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public void WriteRealIdxDescriptor_RoundTripsThroughTryReadRealIdxSlotWithKeyColumns(DatabaseFormat format)
    {
        var layout = IndexLayout.For(format);
        const int descStart = 17;
        byte[] td = new byte[descStart + (3 * layout.RealIdxPhysSize)];
        td.AsSpan().Fill(0xCC);

        layout.WriteRealIdxDescriptor(td, layout.RealIdxPhysOffset(descStart, 1), [3, 1], flags: 0x89, firstDp: 0x00123456);

        Assert.True(layout.TryReadRealIdxSlotWithKeyColumns(td, descStart, 1, out RealIdxSlot slot, out List<KeyColumn> keyColumns));
        Assert.Equal([new KeyColumn(3, true), new KeyColumn(1, true)], keyColumns);
        Assert.Equal(0x00123456, Ri32(td, slot.FirstDpOffset));
        Assert.Equal(0x89, slot.Flags);
        Assert.Equal(0, Ri32(td, slot.FirstDpOffset - 4));
        Assert.True(IndexHelpers.RealIdxColMapMatches(layout, td, slot.PhysStart, [3, 1]));
        Assert.False(IndexHelpers.RealIdxColMapMatches(layout, td, slot.PhysStart, [3]));

        // The neighbouring descriptors are untouched.
        Assert.Equal(0xCC, td[slot.PhysStart - 1]);
        Assert.Equal(0xCC, td[slot.PhysStart + layout.RealIdxPhysSize]);
    }

    [Fact]
    public void WriteRealIdxDescriptor_MoreColumnsThanColMapHolds_Throws()
    {
        var layout = IndexLayout.For(DatabaseFormat.Jet4Mdb);
        byte[] td = new byte[layout.RealIdxPhysSize];

        Assert.Throws<ArgumentException>(() => layout.WriteRealIdxDescriptor(td, 0, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 0, 1));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public void WriteLogicalEntry_RoundTripsThroughTryReadLogicalEntry(DatabaseFormat format)
    {
        var layout = IndexLayout.For(format);
        const int logStart = 9;
        byte[] td = new byte[logStart + (3 * layout.LogicalEntrySize)];
        td.AsSpan().Fill(0xCC);
        int entryStart = layout.LogicalIdxEntryOffset(logStart, 2);

        layout.WriteLogicalEntry(td, entryStart, 7, 4, relTblType: 2, relIdxNum: 5, relTblPage: 0x2A, cascadeUps: 1, cascadeDels: 0, IndexKind.ForeignKey);

        Assert.True(layout.TryReadLogicalEntry(td, logStart, 2, out LogicalIdxEntry entry));
        Assert.Equal(7, entry.IndexNum);
        Assert.Equal(4, entry.IndexNum2);
        Assert.Equal(5, entry.RelIdxNum);
        Assert.Equal(0x2A, entry.RelTblPage);
        Assert.Equal(1, entry.CascadeUps);
        Assert.Equal(0, entry.CascadeDels);
        Assert.Equal(IndexKind.ForeignKey, entry.IndexType);
        Assert.Equal(2, td[entry.FieldsOffset + Constants.TableDefinition.Jet3.LogicalIdx.RelTblTypeOffset]);
        Assert.Equal(0xCC, td[entryStart - 1]);

        if (format == DatabaseFormat.Jet3Mdb)
        {
            Assert.Equal(entryStart, entry.FieldsOffset);
        }
        else
        {
            Assert.Equal(Constants.TableDefinition.Jet4.FormatMagic, Ri32(td, entryStart));
            Assert.Equal(0, Ri32(td, entryStart + layout.LogicalEntrySize - 4));
        }
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, "Table2Table1")]
    [InlineData(DatabaseFormat.Jet3Mdb, "Café Ærø")]
    [InlineData(DatabaseFormat.Jet4Mdb, ".rB")]
    [InlineData(DatabaseFormat.AceAccdb, "Café 中文")]
    public async Task EncodeTDefNameRecord_RoundTripsThroughReadColumnName(DatabaseFormat format, string name)
    {
        // Jet3 names are in the database's ANSI code page, so use the
        // Access-authored fixture's.
        await using MemoryStream stream = format == DatabaseFormat.Jet3Mdb
            ? await cache.CopyToStreamAsync(TestDatabases.IndexTestV1997, this.ct)
            : await CreateDatabaseAsync(format, this.ct);
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(stream, cancellationToken: this.ct);
        DatabaseFile db = harness.Database;
        Assert.Equal(format, db.Format);

        byte[] record = db.EncodeTDefNameRecord(name);
        byte[] td = [0xEE, .. record, 0xEE];
        int pos = 1;

        Assert.Equal(record.Length, db.ReadColumnName(td, ref pos, out string decoded));
        Assert.Equal(name, decoded);
        Assert.Equal(1 + record.Length, pos);
        if (format == DatabaseFormat.Jet3Mdb)
        {
            Assert.Equal(1252, db.CodePage);
            byte[] ansi = db.AnsiEncoding.GetBytes(name);
            Assert.Equal([(byte)ansi.Length, .. ansi], record);
        }
        else
        {
            Assert.Equal(Encoding.Unicode.GetByteCount(name), Ru16(record, 0));
        }
    }

    [Fact]
    public async Task EncodeTDefNameRecord_Jet3NameOver255Bytes_Throws()
    {
        await using MemoryStream stream = await cache.CopyToStreamAsync(TestDatabases.IndexTestV1997, this.ct);
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(stream, cancellationToken: this.ct);

        Assert.Equal(256, harness.Database.EncodeTDefNameRecord(new string('n', 255)).Length);
        Assert.Throws<ArgumentException>(() => harness.Database.EncodeTDefNameRecord(new string('n', 256)));
    }

    /// <summary>
    /// Rewrites, with the format-aware writers, the child-side foreign-key
    /// entry 'Table2Table1' and the real index behind it that Access wrote on
    /// Table1 of the Jackcess indexTest fixtures, and compares the bytes. Only
    /// <c>used_pages</c>, which the writers leave 0, and the order bytes of
    /// unused <c>col_map</c> slots, which Access fills with junk, may differ.
    /// </summary>
    /// <param name="format">The fixture's format.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    public async Task Writers_ReproduceAccessForeignKeyEntryAndRealIndex(DatabaseFormat format)
    {
        string path = format == DatabaseFormat.Jet3Mdb ? TestDatabases.IndexTestV1997 : TestDatabases.IndexTestV2000;
        await using MemoryStream stream = await cache.CopyToStreamAsync(path, this.ct);
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(stream, cancellationToken: this.ct);
        DatabaseFile db = harness.Database;
        IndexLayout layout = db.IndexLayoutInfo;
        Assert.Equal(format, db.Format);

        CatalogEntry child = (await harness.GetCatalogEntryAsync("Table1", this.ct))!;
        CatalogEntry parent = (await harness.GetCatalogEntryAsync("Table2", this.ct))!;
        int foreignKeyColumn = (await harness.ReadTableDefAsync(child.TDefPage, this.ct))!.FindColumn("otherfk1")!.ColNum;
        byte[] td = (await db.ReadTDefBytesAsync(child.TDefPage, this.ct))!;
        int numCols = Ru16(td, db.TDef.NumCols);
        int numIdx = Ri32(td, db.TDef.NumIdx);
        int numRealIdx = Ri32(td, db.TDef.NumRealIdx);
        int realIdxDescStart = IndexCatalogReader.LocateRealIdxDescStart(db, td, numCols, numRealIdx);
        IndexSectionAnchors anchors = layout.GetIndexSection(realIdxDescStart, numRealIdx, numIdx);
        List<string> names = IndexCatalogReader.ReadLogicalIdxNames(db, td, anchors.LogIdxNamesStart, numIdx);
        int li = names.IndexOf("Table2Table1");
        Assert.True(li >= 0);

        // The logical entry: index 2 backed by real index 2, the child side of
        // Table2's '.rC' (index 2), cascading deletes only.
        int entryStart = layout.LogicalIdxEntryOffset(anchors.LogIdxStart, li);
        byte[] entry = new byte[layout.LogicalEntrySize];
        layout.WriteLogicalEntry(entry, 0, 2, 2, Constants.TableDefinition.ChildRelationshipTableType, 2, parent.TDefPage, 0, 1, IndexKind.ForeignKey);
        Assert.Equal(td.AsSpan(entryStart, layout.LogicalEntrySize).ToArray(), entry);

        // The real index on otherfk1, flags 0x00 on Jet3 and 0x80 on Jet4.
        int phys = layout.RealIdxPhysOffset(realIdxDescStart, 2);
        byte flags = td[layout.FlagsAbsoluteOffset(phys)];
        Assert.Equal(format == DatabaseFormat.Jet3Mdb ? 0x00 : Constants.TableDefinition.UnknownIndexFlag, flags);
        long firstDp = (uint)Ri32(td, layout.FirstDpAbsoluteOffset(phys));
        byte[] descriptor = new byte[layout.RealIdxPhysSize];
        layout.WriteRealIdxDescriptor(descriptor, 0, [foreignKeyColumn], flags, firstDp);
        byte[] expected = td.AsSpan(phys, layout.RealIdxPhysSize).ToArray();
        int usedPages = layout.FirstDpAbsoluteOffset(0) - 4;
        Array.Clear(expected, usedPages, 4);
        for (int slot = 1; slot < Constants.TableDefinition.ColMapSlotCount; slot++)
        {
            expected[layout.ColMapSlotOffset(0, slot) + 2] = Constants.TableDefinition.ColMapDescendingFlag;
        }

        Assert.Equal(expected, descriptor);
        Assert.True(IndexHelpers.RealIdxColMapMatches(layout, td, phys, [foreignKeyColumn]));

        // The name record.
        int namePos = anchors.LogIdxNamesStart;
        for (int i = 0; i < li; i++)
        {
            Assert.True(db.ReadColumnName(td, ref namePos, out _) > 0);
        }

        byte[] nameRecord = db.EncodeTDefNameRecord("Table2Table1");
        Assert.Equal(td.AsSpan(namePos, nameRecord.Length).ToArray(), nameRecord);
    }

    private static async Task<MemoryStream> CreateDatabaseAsync(DatabaseFormat format, CancellationToken cancellationToken)
    {
        var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream,
            format,
            new AccessWriterOptions { UseLockFile = false, UseByteRangeLocks = false },
            leaveOpen: true,
            cancellationToken))
        {
        }

        stream.Position = 0;
        return stream;
    }
}
