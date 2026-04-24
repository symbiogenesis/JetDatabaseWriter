namespace JetDatabaseWriter.Tests;

using System;
using System.IO;
using System.Text;
using Xunit;

public class ColumnPropertyBlockTests
{
    [Fact]
    public void Parse_NullBlob_Returns_Null()
    {
        Assert.Null(ColumnPropertyBlock.Parse(null, DatabaseFormat.Jet4Mdb));
    }

    [Fact]
    public void Parse_EmptyBlob_Returns_Null()
    {
        Assert.Null(ColumnPropertyBlock.Parse(Array.Empty<byte>(), DatabaseFormat.Jet4Mdb));
    }

    [Fact]
    public void Parse_UnknownMagic_Returns_Null()
    {
        byte[] blob = [(byte)'X', (byte)'X', (byte)'X', 0x00];
        Assert.Null(ColumnPropertyBlock.Parse(blob, DatabaseFormat.Jet4Mdb));
    }

    [Fact]
    public void Parse_MagicOnly_Returns_Empty()
    {
        byte[] blob = [(byte)'M', (byte)'R', (byte)'2', 0x00];
        ColumnPropertyBlock? block = ColumnPropertyBlock.Parse(blob, DatabaseFormat.Jet4Mdb);

        Assert.NotNull(block);
        Assert.Empty(block!.Targets);
        Assert.Empty(block.UnknownChunks);
    }

    [Fact]
    public void Parse_SingleColumn_DefaultValue_RoundTrips()
    {
        SyntheticEntry[] entries = [new SyntheticEntry(0, ColumnPropertyBlock.DataTypeText, 0x00, Encoding.Unicode.GetBytes("0"))];
        SyntheticBlock[] blocks = [new SyntheticBlock("Qty", 0x0000, entries)];
        string[] names = ["DefaultValue"];

        byte[] blob = BuildBlob(true, names, blocks);

        ColumnPropertyBlock? parsed = ColumnPropertyBlock.Parse(blob, DatabaseFormat.Jet4Mdb);

        Assert.NotNull(parsed);
        Assert.Single(parsed!.Targets);

        ColumnPropertyTarget target = parsed.Targets[0];
        Assert.Equal("Qty", target.Name);
        Assert.Single(target.Entries);

        ColumnPropertyEntry entry = target.Entries[0];
        Assert.Equal(ColumnPropertyNames.DefaultValue, entry.Name);
        Assert.Equal(ColumnPropertyBlock.DataTypeText, entry.DataType);
        Assert.Equal("0", target.GetTextValue(ColumnPropertyNames.DefaultValue, DatabaseFormat.Jet4Mdb));
    }

    [Fact]
    public void Parse_MultipleProperties_PerColumn()
    {
        SyntheticEntry[] entries =
        [
            new SyntheticEntry(0, ColumnPropertyBlock.DataTypeText, 0x00, Encoding.Unicode.GetBytes("0")),
            new SyntheticEntry(1, ColumnPropertyBlock.DataTypeText, 0x00, Encoding.Unicode.GetBytes(">=0 And <=100")),
            new SyntheticEntry(2, ColumnPropertyBlock.DataTypeText, 0x00, Encoding.Unicode.GetBytes("Score must be 0-100")),
            new SyntheticEntry(3, ColumnPropertyBlock.DataTypeText, 0x00, Encoding.Unicode.GetBytes("Test score (0-100)")),
        ];
        SyntheticBlock[] blocks = [new SyntheticBlock("Score", 0x0000, entries)];
        string[] names = ["DefaultValue", "ValidationRule", "ValidationText", "Description"];

        byte[] blob = BuildBlob(true, names, blocks);

        ColumnPropertyBlock? parsed = ColumnPropertyBlock.Parse(blob, DatabaseFormat.Jet4Mdb);

        Assert.NotNull(parsed);
        ColumnPropertyTarget? target = parsed!.FindTarget("Score");
        Assert.NotNull(target);
        Assert.Equal("0", target!.GetTextValue(ColumnPropertyNames.DefaultValue, DatabaseFormat.Jet4Mdb));
        Assert.Equal(">=0 And <=100", target.GetTextValue(ColumnPropertyNames.ValidationRule, DatabaseFormat.Jet4Mdb));
        Assert.Equal("Score must be 0-100", target.GetTextValue(ColumnPropertyNames.ValidationText, DatabaseFormat.Jet4Mdb));
        Assert.Equal("Test score (0-100)", target.GetTextValue(ColumnPropertyNames.Description, DatabaseFormat.Jet4Mdb));
    }

    [Fact]
    public void Parse_TableLevelAndColumnLevel_BothSurfaced()
    {
        SyntheticEntry[] tableEntries = [new SyntheticEntry(0, ColumnPropertyBlock.DataTypeText, 0x00, Encoding.Unicode.GetBytes("Customer orders"))];
        SyntheticEntry[] colEntries = [new SyntheticEntry(0, ColumnPropertyBlock.DataTypeText, 0x00, Encoding.Unicode.GetBytes("Primary key"))];
        SyntheticBlock[] blocks =
        [
            new SyntheticBlock("Orders", 0x0000, tableEntries),
            new SyntheticBlock("OrderId", 0x0000, colEntries),
        ];
        string[] names = ["Description"];

        byte[] blob = BuildBlob(true, names, blocks);

        ColumnPropertyBlock? parsed = ColumnPropertyBlock.Parse(blob, DatabaseFormat.Jet4Mdb);

        Assert.NotNull(parsed);
        Assert.Equal(2, parsed!.Targets.Count);
        Assert.Equal("Customer orders", parsed.FindTarget("Orders")!.GetTextValue("Description", DatabaseFormat.Jet4Mdb));
        Assert.Equal("Primary key", parsed.FindTarget("OrderId")!.GetTextValue("Description", DatabaseFormat.Jet4Mdb));
    }

    [Fact]
    public void Parse_FindTarget_IsCaseInsensitive()
    {
        SyntheticEntry[] entries = [new SyntheticEntry(0, ColumnPropertyBlock.DataTypeText, 0x00, Encoding.Unicode.GetBytes("hi"))];
        SyntheticBlock[] blocks = [new SyntheticBlock("Foo", 0x0000, entries)];
        string[] names = ["Description"];

        byte[] blob = BuildBlob(true, names, blocks);

        ColumnPropertyBlock parsed = ColumnPropertyBlock.Parse(blob, DatabaseFormat.Jet4Mdb)!;
        Assert.NotNull(parsed.FindTarget("foo"));
        Assert.NotNull(parsed.FindTarget("FOO"));
        Assert.Null(parsed.FindTarget("bar"));
    }

    [Fact]
    public void Parse_UnknownChunkType_PreservedVerbatim()
    {
        var ms = new MemoryStream();
        WriteMagic(ms, mr2: true);

        byte[] unknownPayload = [0xDE, 0xAD, 0xBE, 0xEF];
        WriteChunk(ms, 0xABCD, unknownPayload);

        string[] names = ["Description"];
        WriteChunk(ms, 0x0080, BuildNamePoolPayload(names));

        SyntheticEntry[] entries = [new SyntheticEntry(0, ColumnPropertyBlock.DataTypeText, 0x00, Encoding.Unicode.GetBytes("ok"))];
        WriteChunk(ms, 0x0000, BuildPropertyBlockPayload("X", entries));

        ColumnPropertyBlock parsed = ColumnPropertyBlock.Parse(ms.ToArray(), DatabaseFormat.Jet4Mdb)!;

        Assert.Single(parsed.UnknownChunks);
        Assert.Equal((ushort)0xABCD, parsed.UnknownChunks[0].ChunkType);
        Assert.Equal(unknownPayload, parsed.UnknownChunks[0].Payload);

        Assert.Single(parsed.Targets);
        Assert.Equal("ok", parsed.FindTarget("X")!.GetTextValue("Description", DatabaseFormat.Jet4Mdb));
    }

    [Fact]
    public void Parse_TruncatedChunkLength_StopsAtBoundary()
    {
        var ms = new MemoryStream();
        WriteMagic(ms, mr2: true);

        WriteUInt32(ms, 0xFFFFFFFFu);
        WriteUInt16(ms, 0x0080);

        ColumnPropertyBlock parsed = ColumnPropertyBlock.Parse(ms.ToArray(), DatabaseFormat.Jet4Mdb)!;

        Assert.Empty(parsed.Targets);
        Assert.Empty(parsed.UnknownChunks);
    }

    [Fact]
    public void Parse_NameIndexOutOfRange_Stops_DoesNotThrow()
    {
        SyntheticEntry[] entries = [new SyntheticEntry(5, ColumnPropertyBlock.DataTypeText, 0x00, Encoding.Unicode.GetBytes("oops"))];
        SyntheticBlock[] blocks = [new SyntheticBlock("X", 0x0000, entries)];
        string[] names = ["Description"];

        byte[] blob = BuildBlob(true, names, blocks);

        ColumnPropertyBlock parsed = ColumnPropertyBlock.Parse(blob, DatabaseFormat.Jet4Mdb)!;
        ColumnPropertyTarget target = parsed.FindTarget("X")!;
        Assert.Empty(target.Entries);
    }

    [Fact]
    public void Parse_AcceptsAllPropertyBlockSubtypes()
    {
        SyntheticEntry[] aEntries = [new SyntheticEntry(0, ColumnPropertyBlock.DataTypeText, 0, Encoding.Unicode.GetBytes("a"))];
        SyntheticEntry[] bEntries = [new SyntheticEntry(0, ColumnPropertyBlock.DataTypeText, 0, Encoding.Unicode.GetBytes("b"))];
        SyntheticEntry[] cEntries = [new SyntheticEntry(0, ColumnPropertyBlock.DataTypeText, 0, Encoding.Unicode.GetBytes("c"))];
        SyntheticBlock[] blocks =
        [
            new SyntheticBlock("A", 0x0000, aEntries),
            new SyntheticBlock("B", 0x0001, bEntries),
            new SyntheticBlock("C", 0x0002, cEntries),
        ];
        string[] names = ["Description"];

        byte[] blob = BuildBlob(true, names, blocks);

        ColumnPropertyBlock parsed = ColumnPropertyBlock.Parse(blob, DatabaseFormat.Jet4Mdb)!;
        Assert.Equal(3, parsed.Targets.Count);
        Assert.Equal("a", parsed.FindTarget("A")!.GetTextValue("Description", DatabaseFormat.Jet4Mdb));
        Assert.Equal("b", parsed.FindTarget("B")!.GetTextValue("Description", DatabaseFormat.Jet4Mdb));
        Assert.Equal("c", parsed.FindTarget("C")!.GetTextValue("Description", DatabaseFormat.Jet4Mdb));
    }

    private static byte[] BuildBlob(bool magicMr2, string[] namePool, SyntheticBlock[] propertyBlocks)
    {
        var ms = new MemoryStream();
        WriteMagic(ms, magicMr2);
        WriteChunk(ms, 0x0080, BuildNamePoolPayload(namePool));
        foreach (SyntheticBlock pb in propertyBlocks)
        {
            WriteChunk(ms, pb.ChunkType, BuildPropertyBlockPayload(pb.TargetName, pb.Entries));
        }

        return ms.ToArray();
    }

    private static byte[] BuildNamePoolPayload(string[] names)
    {
        var ms = new MemoryStream();
        foreach (string n in names)
        {
            byte[] bytes = Encoding.Unicode.GetBytes(n);
            WriteUInt16(ms, (ushort)bytes.Length);
            ms.Write(bytes, 0, bytes.Length);
        }

        return ms.ToArray();
    }

    private static byte[] BuildPropertyBlockPayload(string targetName, SyntheticEntry[] entries)
    {
        var ms = new MemoryStream();

        WriteUInt32(ms, 0);
        byte[] nameBytes = Encoding.Unicode.GetBytes(targetName);
        WriteUInt16(ms, (ushort)nameBytes.Length);
        ms.Write(nameBytes, 0, nameBytes.Length);

        foreach (SyntheticEntry e in entries)
        {
            int entryLen = 8 + e.Value.Length;
            WriteUInt16(ms, (ushort)entryLen);
            ms.WriteByte(e.DdlFlag);
            ms.WriteByte(e.DataType);
            WriteUInt16(ms, (ushort)e.NameIndex);
            WriteUInt16(ms, (ushort)e.Value.Length);
            ms.Write(e.Value, 0, e.Value.Length);
        }

        return ms.ToArray();
    }

    private static void WriteMagic(MemoryStream ms, bool mr2)
    {
        byte[] magic = mr2
            ? [(byte)'M', (byte)'R', (byte)'2', 0x00]
            : [(byte)'K', (byte)'K', (byte)'D', 0x00];
        ms.Write(magic, 0, 4);
    }

    private static void WriteChunk(MemoryStream ms, ushort chunkType, byte[] payload)
    {
        uint chunkLen = (uint)(6 + payload.Length);
        WriteUInt32(ms, chunkLen);
        WriteUInt16(ms, chunkType);
        ms.Write(payload, 0, payload.Length);
    }

    private static void WriteUInt16(MemoryStream ms, ushort v)
    {
        ms.WriteByte((byte)(v & 0xFF));
        ms.WriteByte((byte)((v >> 8) & 0xFF));
    }

    private static void WriteUInt32(MemoryStream ms, uint v)
    {
        ms.WriteByte((byte)(v & 0xFF));
        ms.WriteByte((byte)((v >> 8) & 0xFF));
        ms.WriteByte((byte)((v >> 16) & 0xFF));
        ms.WriteByte((byte)((v >> 24) & 0xFF));
    }

    private sealed record SyntheticEntry(int NameIndex, byte DataType, byte DdlFlag, byte[] Value);

    private sealed record SyntheticBlock(string TargetName, ushort ChunkType, SyntheticEntry[] Entries);
}
