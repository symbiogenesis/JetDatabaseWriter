namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.IO;
using System.Text;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using Xunit;

public class ColumnPropertyBlockTests
{
    [Fact]
    public void Parse_NullBlob_Returns_Null() => Assert.Null(ColumnPropertyBlock.Parse(null, JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb)));

    [Fact]
    public void Parse_EmptyBlob_ThrowsCorruption() => Assert.Throws<JetCorruptDataException>(() => ColumnPropertyBlock.Parse([], JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb)));

    [Fact]
    public void Parse_UnknownMagic_ThrowsCorruption()
    {
        byte[] blob = [(byte)'X', (byte)'X', (byte)'X', 0x00];
        Assert.Throws<JetCorruptDataException>(() => ColumnPropertyBlock.Parse(blob, JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb)));
    }

    [Fact]
    public void Parse_MagicOnly_Returns_Empty()
    {
        byte[] blob = [(byte)'M', (byte)'R', (byte)'2', 0x00];
        var block = ColumnPropertyBlock.Parse(blob, JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb));

        Assert.NotNull(block);
        Assert.Empty(block.Targets);
        Assert.Empty(block.UnknownChunks);
    }

    [Fact]
    public void Parse_SingleColumn_DefaultValue_RoundTrips()
    {
        SyntheticEntry[] entries = [new SyntheticEntry(0, ColumnType.TextType, 0x00, Encoding.Unicode.GetBytes("0"))];
        SyntheticBlock[] blocks = [new SyntheticBlock("Qty", 0x0000, entries)];
        string[] names = ["DefaultValue"];

        byte[] blob = BuildBlob(true, names, blocks);

        var parsed = ColumnPropertyBlock.Parse(blob, JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb));

        Assert.NotNull(parsed);

        ColumnPropertyTarget target = Assert.Single(parsed.Targets);
        Assert.Equal("Qty", target.Name);

        ColumnPropertyEntry entry = Assert.Single(target.Entries);
        Assert.Equal(Constants.ColumnPropertyNames.DefaultValue, entry.Name);
        Assert.Equal(ColumnType.TextType, entry.DataType);
        Assert.Equal("0", target.GetTextValue(Constants.ColumnPropertyNames.DefaultValue, JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb)));
    }

    [Fact]
    public void Parse_MultipleProperties_PerColumn()
    {
        SyntheticEntry[] entries =
        [
            new SyntheticEntry(0, ColumnType.TextType, 0x00, Encoding.Unicode.GetBytes("0")),
            new SyntheticEntry(1, ColumnType.TextType, 0x00, Encoding.Unicode.GetBytes(">=0 And <=100")),
            new SyntheticEntry(2, ColumnType.TextType, 0x00, Encoding.Unicode.GetBytes("Score must be 0-100")),
            new SyntheticEntry(3, ColumnType.TextType, 0x00, Encoding.Unicode.GetBytes("Test score (0-100)")),
        ];
        SyntheticBlock[] blocks = [new SyntheticBlock("Score", 0x0000, entries)];
        string[] names = ["DefaultValue", "ValidationRule", "ValidationText", "Description"];

        byte[] blob = BuildBlob(true, names, blocks);

        var parsed = ColumnPropertyBlock.Parse(blob, JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb));

        Assert.NotNull(parsed);
        ColumnPropertyTarget? target = parsed.FindTarget("Score");
        Assert.NotNull(target);
        Assert.Equal("0", target.GetTextValue(Constants.ColumnPropertyNames.DefaultValue, JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb)));
        Assert.Equal(">=0 And <=100", target.GetTextValue(Constants.ColumnPropertyNames.ValidationRule, JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb)));
        Assert.Equal("Score must be 0-100", target.GetTextValue(Constants.ColumnPropertyNames.ValidationText, JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb)));
        Assert.Equal("Test score (0-100)", target.GetTextValue(Constants.ColumnPropertyNames.Description, JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb)));
    }

    [Fact]
    public void Parse_TableLevelAndColumnLevel_BothSurfaced()
    {
        SyntheticEntry[] tableEntries = [new SyntheticEntry(0, ColumnType.TextType, 0x00, Encoding.Unicode.GetBytes("Customer orders"))];
        SyntheticEntry[] colEntries = [new SyntheticEntry(0, ColumnType.TextType, 0x00, Encoding.Unicode.GetBytes("Primary key"))];
        SyntheticBlock[] blocks =
        [
            new SyntheticBlock("Orders", 0x0000, tableEntries),
            new SyntheticBlock("OrderId", 0x0000, colEntries),
        ];
        string[] names = ["Description"];

        byte[] blob = BuildBlob(true, names, blocks);

        var parsed = ColumnPropertyBlock.Parse(blob, JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb));

        Assert.NotNull(parsed);
        Assert.Equal(2, parsed!.Targets.Count);
        Assert.Equal("Customer orders", parsed.FindTarget("Orders")!.GetTextValue("Description", JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb)));
        Assert.Equal("Primary key", parsed.FindTarget("OrderId")!.GetTextValue("Description", JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb)));
    }

    [Fact]
    public void Parse_FindTarget_IsCaseInsensitive()
    {
        SyntheticEntry[] entries = [new SyntheticEntry(0, ColumnType.TextType, 0x00, Encoding.Unicode.GetBytes("hi"))];
        SyntheticBlock[] blocks = [new SyntheticBlock("Foo", 0x0000, entries)];
        string[] names = ["Description"];

        byte[] blob = BuildBlob(true, names, blocks);

        ColumnPropertyBlock parsed = ColumnPropertyBlock.Parse(blob, JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb))!;
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

        SyntheticEntry[] entries = [new SyntheticEntry(0, ColumnType.TextType, 0x00, Encoding.Unicode.GetBytes("ok"))];
        WriteChunk(ms, 0x0000, BuildPropertyBlockPayload("X", entries));

        ColumnPropertyBlock parsed = ColumnPropertyBlock.Parse(ms.ToArray(), JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb))!;

        ColumnPropertyUnknownChunk chunk = Assert.Single(parsed.UnknownChunks);
        Assert.Equal((ushort)0xABCD, chunk.ChunkType);
        Assert.Equal(unknownPayload, chunk.Payload);

        Assert.Single(parsed.Targets);
        Assert.Equal("ok", parsed.FindTarget("X")!.GetTextValue("Description", JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb)));
    }

    [Fact]
    public void Parse_TruncatedChunkLength_ThrowsCorruption()
    {
        using var ms = new MemoryStream();
        WriteMagic(ms, mr2: true);
        WriteUInt32(ms, 0xFFFFFFFFu);
        WriteUInt16(ms, 0x0080);
        JetCorruptDataException error = Assert.Throws<JetCorruptDataException>(() => ColumnPropertyBlock.Parse(ms.ToArray(), JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb)));
        Assert.Equal(JetErrorCode.CorruptCatalog, error.ErrorCode);
    }

    [Fact]
    public void Parse_NameIndexOutOfRange_ThrowsCorruption()
    {
        SyntheticEntry[] entries = [new SyntheticEntry(5, ColumnType.TextType, 0x00, Encoding.Unicode.GetBytes("oops"))];
        SyntheticBlock[] blocks = [new SyntheticBlock("X", 0x0000, entries)];
        string[] names = ["Description"];

        byte[] blob = BuildBlob(true, names, blocks);

        Assert.Throws<JetCorruptDataException>(() => ColumnPropertyBlock.Parse(blob, JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb)));
    }

    [Fact]
    public void Parse_AcceptsAllPropertyBlockSubtypes()
    {
        SyntheticEntry[] aEntries = [new SyntheticEntry(0, ColumnType.TextType, 0, Encoding.Unicode.GetBytes("a"))];
        SyntheticEntry[] bEntries = [new SyntheticEntry(0, ColumnType.TextType, 0, Encoding.Unicode.GetBytes("b"))];
        SyntheticEntry[] cEntries = [new SyntheticEntry(0, ColumnType.TextType, 0, Encoding.Unicode.GetBytes("c"))];
        SyntheticBlock[] blocks =
        [
            new SyntheticBlock("A", 0x0000, aEntries),
            new SyntheticBlock("B", 0x0001, bEntries),
            new SyntheticBlock("C", 0x0002, cEntries),
        ];
        string[] names = ["Description"];

        byte[] blob = BuildBlob(true, names, blocks);

        ColumnPropertyBlock parsed = ColumnPropertyBlock.Parse(blob, JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb))!;
        Assert.Equal(3, parsed.Targets.Count);
        Assert.Equal("a", parsed.FindTarget("A")!.GetTextValue("Description", JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb)));
        Assert.Equal("b", parsed.FindTarget("B")!.GetTextValue("Description", JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb)));
        Assert.Equal("c", parsed.FindTarget("C")!.GetTextValue("Description", JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb)));
    }

    [Fact]
    public void Parse_EveryTruncatedFieldIsRejected()
    {
        byte[][] malformed =
        [
            [0x4D],
            [0x4D, 0x52, 0x32, 0x00, 0xFF],
            [0x4D, 0x52, 0x32, 0x00, 0x07, 0, 0, 0, 0x80, 0, 0x01],
            [0x4D, 0x52, 0x32, 0x00, 0x0A, 0, 0, 0, 0x80, 0, 0x04, 0, 0x41, 0],
            [0x4D, 0x52, 0x32, 0x00, 0x0B, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            [0x4D, 0x52, 0x32, 0x00, 0x0D, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x04, 0, 0x41],
        ];
        foreach (byte[] blob in malformed)
        {
            Assert.Throws<JetCorruptDataException>(() => ColumnPropertyBlock.Parse(blob, JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb)));
        }
    }

    [Fact]
    public void Parse_TruncationAfterValidEntriesDoesNotReturnPartialMetadata()
    {
        byte[] valid = BuildBlob(true, ["Required"], [new SyntheticBlock("Id", 0, [new SyntheticEntry(0, ColumnType.BooleanType, 1, [0xFF])])]);
        byte[] trailing = [.. valid, 0x01];
        Assert.Throws<JetCorruptDataException>(() => ColumnPropertyBlock.Parse(trailing, JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb)));
    }

    [Fact]
    public void Parse_InvalidTextEncodingIsNotReplacedSilently()
    {
        byte[] blob = BuildBlob(true, ["Description"], [new SyntheticBlock("Id", 0, [new SyntheticEntry(0, ColumnType.TextType, 0, [0x41])])]);
        Assert.Throws<JetCorruptDataException>(() => ColumnPropertyBlock.Parse(blob, JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb)));
    }

    [Fact]
    public void Parse_RecognizedSignatureIsPreservedAcrossOuterFormatDifferences()
    {
        var source = JetFormat.ForNewDatabase(DatabaseFormat.Jet3Mdb);
        var outer = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        var builder = new ColumnPropertyBlockBuilder();
        builder.GetOrAddTarget("A").AddText("Description", "Café", source);
        byte[] blob = builder.ToBytes(source)!;
        ColumnPropertyBlock parsed = ColumnPropertyBlock.Parse(blob, outer)!;
        Assert.Equal("Café", parsed.FindTarget("A")!.GetTextValue("Description", outer));

        ColumnDefinition original = new("A", typeof(int)) { Description = "Café" };
        ColumnDefinition changed = original with { Description = "New café" };
        ColumnPropertyBlock rewritten = PersistedPropertyProjector.ProjectForRewrite(parsed, [original], [changed], static name => name, outer);
        byte[] bytes = rewritten.ToBytes(outer)!;
        Assert.Equal(blob[0..4], bytes[0..4]);
        Assert.Equal("New café", ColumnPropertyBlock.Parse(bytes, outer)!.FindTarget("A")!.GetTextValue("Description", outer));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    public void Rewrite_PreservesOpaqueTargetHeaders(DatabaseFormat databaseFormat)
    {
        var format = JetFormat.ForNewDatabase(databaseFormat);
        var builder = new ColumnPropertyBlockBuilder();
        builder.GetOrAddTarget("A").AddText("Description", "Old", format);
        byte[] blob = builder.ToBytes(format)!;
        int targetOffset = 4 + System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(blob.AsSpan(4)) + 6;
        byte[] opaque = [0xFF, 0x80, 0xAB, 0xCD];
        opaque.CopyTo(blob, targetOffset);
        ColumnPropertyBlock parsed = ColumnPropertyBlock.Parse(blob, format)!;
        Assert.Equal(blob, parsed.ToBytes(format));

        ColumnDefinition original = new("A", typeof(int)) { Description = "Old" };
        ColumnDefinition changed = new("LongerName", typeof(int)) { Description = "New" };
        ColumnPropertyBlock projected = PersistedPropertyProjector.ProjectForRewrite(parsed, [original], [changed], static _ => "LongerName", format);
        byte[] rewritten = projected.ToBytes(format)!;
        Assert.Equal(opaque, rewritten[targetOffset..(targetOffset + 4)]);
        Assert.Equal("New", ColumnPropertyBlock.Parse(rewritten, format)!.FindTarget("LongerName")!.GetTextValue("Description", format));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    public void Rename_UpdatesNativeTargetNameLength(DatabaseFormat databaseFormat)
    {
        var format = JetFormat.ForNewDatabase(databaseFormat);
        var builder = new ColumnPropertyBlockBuilder();
        builder.GetOrAddTarget("A").AddText("Description", "Keep", format);
        ColumnPropertyBlock parsed = ColumnPropertyBlock.Parse(builder.ToBytes(format), format)!;
        var renamed = ColumnPropertyBlockBuilder.FromBlock(parsed);
        renamed.RenameTarget("A", "LongerName");
        byte[] bytes = renamed.ToBytes(format)!;
        int targetOffset = 4 + System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(4)) + 6;
        Assert.Equal((uint)(6 + format.PropertyTextEncoding.GetByteCount("LongerName")), System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(targetOffset)));
        Assert.Equal("Keep", ColumnPropertyBlock.Parse(bytes, format)!.FindTarget("LongerName")!.GetTextValue("Description", format));
    }

    [Fact]
    public void Rewrite_PreservesNamePoolsChunkOrderAndEntryPadding()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        using var stream = new MemoryStream();
        WriteMagic(stream, mr2: true);
        WriteChunk(stream, 0xABCD, [0x13, 0x37]);
        WriteChunk(stream, 0x0080, BuildNamePoolPayload(["Unused", "Description", "Description"]));
        byte[] payload = BuildPropertyBlockPayload("A", [new SyntheticEntry(2, ColumnType.TextType, 0, Encoding.Unicode.GetBytes("Keep"))]);
        int entryOffset = 6 + Encoding.Unicode.GetByteCount("A");
        System.Buffers.Binary.BinaryPrimitives.WriteUInt16LittleEndian(payload.AsSpan(entryOffset), (ushort)(payload.Length - entryOffset + 3));
        byte[] padded = new byte[payload.Length + 3];
        payload.CopyTo(padded, 0);
        padded[^3] = 0xDE;
        padded[^2] = 0xAD;
        padded[^1] = 0xFF;
        WriteChunk(stream, 0x0001, padded);
        WriteChunk(stream, 0xBCDE, [0x42]);
        WriteChunk(stream, 0x0080, BuildNamePoolPayload(["Spare", "Required"]));
        WriteChunk(stream, 0x0002, BuildPropertyBlockPayload("B", [new SyntheticEntry(1, ColumnType.BooleanType, 1, [0xFF])]));
        byte[] original = stream.ToArray();
        ColumnPropertyBlock parsed = ColumnPropertyBlock.Parse(original, format)!;
        Assert.Equal(original, parsed.ToBytes(format));

        var builder = ColumnPropertyBlockBuilder.FromBlock(parsed);
        builder.RenameTarget("A", "Longer");
        builder.GetOrAddTarget("B").AddText("Description", "Added", format);
        ColumnPropertyBlock edited = ColumnPropertyBlock.Parse(builder.ToBytes(format), format)!;
        Assert.Equal(new byte[] { 0xDE, 0xAD, 0xFF }, edited.FindTarget("Longer")!.Find("Description")!.Padding);
        Assert.Equal("Keep", edited.FindTarget("Longer")!.GetTextValue("Description", format));
        Assert.Equal("Added", edited.FindTarget("B")!.GetTextValue("Description", format));
        Assert.Equal(["Unused", "Description", "Description"], Assert.IsType<string[]>(edited.SourceChunks[1].Names));
        Assert.Equal(["Spare", "Required", "Description"], Assert.IsType<string[]>(edited.SourceChunks[4].Names));
        Assert.Equal(original[..10], builder.ToBytes(format)![..10]);
        Assert.Equal((ushort)0xABCD, edited.SourceChunks[0].ChunkType);
        Assert.Equal(new byte[] { 0x13, 0x37 }, edited.UnknownChunks[0].Payload);
        Assert.Equal((ushort)0xBCDE, edited.SourceChunks[3].ChunkType);
        Assert.Equal("B"u8.ToArray(), edited.UnknownChunks[1].Payload);

        ColumnDefinition before = new("A", typeof(int)) { Description = "Keep" };
        ColumnDefinition after = new("Renamed", typeof(int)) { Description = "Changed" };
        ColumnPropertyBlock projected = PersistedPropertyProjector.ProjectForRewrite(parsed, [before], [after], static _ => "Renamed", format);
        Assert.Equal(new byte[] { 0xDE, 0xAD, 0xFF }, projected.FindTarget("Renamed")!.Find("Description")!.Padding);
        Assert.Equal("Changed", projected.FindTarget("Renamed")!.GetTextValue("Description", format));
        Assert.Equal(["Unused", "Description", "Description"], Assert.IsType<string[]>(projected.SourceChunks[1].Names));

        _ = builder.RemoveTarget("Longer");
        builder.GetOrAddTarget("New").AddByte("NewProperty", 23);
        ColumnPropertyBlock added = ColumnPropertyBlock.Parse(builder.ToBytes(format), format)!;
        Assert.Equal(["Unused", "Description", "Description"], Assert.IsType<string[]>(added.SourceChunks[1].Names));
        Assert.Null(added.FindTarget("Longer"));
        Assert.Equal(new byte[] { 23 }, added.FindTarget("New")!.Find("NewProperty")!.Value);
    }

    [Fact]
    public void RoundTrip_EmptyTargetBeforeNamePool_KeepsSourcePosition()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        using var stream = new MemoryStream();
        WriteMagic(stream, mr2: true);
        WriteChunk(stream, 0x0000, BuildPropertyBlockPayload(string.Empty, []));
        WriteChunk(stream, 0x0080, BuildNamePoolPayload(["Unused"]));
        byte[] original = stream.ToArray();
        ColumnPropertyBlock parsed = ColumnPropertyBlock.Parse(original, format)!;
        Assert.Equal(original, parsed.ToBytes(format));
        var edited = ColumnPropertyBlockBuilder.FromBlock(parsed);
        edited.GetOrAddTableTarget().AddByte("NewProperty", 7);
        Assert.Equal(new byte[] { 7 }, ColumnPropertyBlock.Parse(edited.ToBytes(format), format)!.FindTableTarget()!.Find("NewProperty")!.Value);
    }

    [Fact]
    public void Serialize_EntryPaddingCountsTowardStoredLengthLimit()
    {
        var builder = new ColumnPropertyBlockBuilder();
        builder.GetOrAddTarget("A").Entries.Add(new ColumnPropertyEntryBuilder
        {
            Name = "Opaque",
            DataType = ColumnType.ByteType,
            Value = [1],
            Padding = new byte[ushort.MaxValue - 8],
        });
        Assert.Throws<InvalidOperationException>(() => builder.ToBytes(JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb)));
    }

    [Fact]
    public void AddTarget_ReusesSingleSourceNamePool()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        var original = new ColumnPropertyBlockBuilder();
        original.GetOrAddTarget("A").AddByte("First", 1);
        var edited = ColumnPropertyBlockBuilder.FromBlock(ColumnPropertyBlock.Parse(original.ToBytes(format), format)!);
        edited.GetOrAddTarget("B").AddByte("Second", 2);
        ColumnPropertyBlock parsed = ColumnPropertyBlock.Parse(edited.ToBytes(format), format)!;
        Assert.Equal(3, parsed.SourceChunks.Count);
        Assert.Equal(["First", "Second"], Assert.IsType<string[]>(parsed.SourceChunks[0].Names));
        Assert.Equal("B", parsed.Targets[1].Name);
    }

    [Fact]
    public void RoundTrip_MagicOnly_KeepsPresentValue()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        byte[] blob = [(byte)'M', (byte)'R', (byte)'2', 0];
        Assert.Equal(blob, ColumnPropertyBlock.Parse(blob, format)!.ToBytes(format));
        Assert.Null(new ColumnPropertyBlockBuilder().ToBytes(format));
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
            ms.WriteByte((byte)e.DataType);
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

    private sealed record SyntheticEntry(int NameIndex, ColumnType DataType, byte DdlFlag, byte[] Value);

    private sealed record SyntheticBlock(string TargetName, ushort ChunkType, SyntheticEntry[] Entries);
}
