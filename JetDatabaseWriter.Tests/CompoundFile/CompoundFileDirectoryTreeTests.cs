namespace JetDatabaseWriter.Tests.CompoundFile;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.CompoundFile;
using Xunit;

/// <summary>CFB directories use the specification's name ordering and a balanced searchable tree.</summary>
public sealed class CompoundFileDirectoryTreeTests
{
    /// <summary>Length precedes case-insensitive code-unit order, and surrogate pairs retain binary order.</summary>
    /// <param name="officeCrypto">Whether to emit the version-four Office container.</param>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Directory_UsesNativeNameOrder_AndPreservesPayloads(bool officeCrypto)
    {
        string[] input = ["zzZ", "bB", "z", "\U00010428", "AA", "B", "\U00010400", "a", "\u00E8"];
        string[] expected = ["a", "B", "z", "\u00E8", "AA", "bB", "\U00010400", "\U00010428", "zzZ"];
        var entries = new List<KeyValuePair<string, byte[]>>(input.Length);
        for (int index = 0; index < input.Length; index++)
        {
            byte[] payload = new byte[officeCrypto ? 4096 : 1];
            payload[0] = checked((byte)index);
            entries.Add(new(input[index], payload));
        }

        byte[] file = officeCrypto ? CompoundFileWriter.BuildOfficeCrypto(entries) : CompoundFileWriter.Build(entries);
        Assert.Equal(expected, ReadTree(file, entries.Count));

        await using var stream = new MemoryStream(file);
        Dictionary<string, byte[]> recovered = await CompoundFileReader.ReadStreamsAsync(stream, TestContext.Current.CancellationToken);
        Assert.Equal(entries.Count, recovered.Count);
        foreach (KeyValuePair<string, byte[]> entry in entries)
        {
            Assert.Equal(entry.Value, recovered[entry.Key]);
        }
    }

    /// <summary>Every emitted entry is reachable once and worst-case lookup depth stays logarithmic.</summary>
    /// <param name="officeCrypto">Whether to emit the version-four Office container.</param>
    /// <param name="count">The number of stream entries.</param>
    [Theory]
    [InlineData(false, 1)]
    [InlineData(false, 2)]
    [InlineData(false, 3)]
    [InlineData(false, 4)]
    [InlineData(false, 17)]
    [InlineData(false, 255)]
    [InlineData(true, 1)]
    [InlineData(true, 2)]
    [InlineData(true, 3)]
    [InlineData(true, 4)]
    [InlineData(true, 17)]
    [InlineData(true, 255)]
    public void DirectoryTree_IsBalancedAndComplete(bool officeCrypto, int count)
    {
        var entries = new List<KeyValuePair<string, byte[]>>(count);
        string[] expected = new string[count];
        for (int index = count - 1; index >= 0; index--)
        {
            string name = $"Stream{index:D3}";
            expected[index] = name;
            entries.Add(new(name, []));
        }

        byte[] file = officeCrypto ? CompoundFileWriter.BuildOfficeCrypto(entries) : CompoundFileWriter.Build(entries);
        Assert.Equal(expected, ReadTree(file, count));
    }

    /// <summary>Names equivalent under native simple uppercase comparison cannot occupy separate siblings.</summary>
    /// <param name="first">The first name.</param>
    /// <param name="second">The equivalent spelling.</param>
    [Theory]
    [InlineData("Name", "Name")]
    [InlineData("Name", "nAME")]
    [InlineData("\u00E8", "\u00C8")]
    [InlineData("\u023A", "\u2C65")]
    [InlineData("\u023E", "\u2C66")]
    [InlineData("\u1FC3", "\u1FCC")]
    [InlineData("\u03C2", "\u03A3")]
    [InlineData("\u03C3", "\u03A3")]
    public void EquivalentNames_AreRejected(string first, string second)
    {
        KeyValuePair<string, byte[]>[] entries = [new(first, []), new(second, [])];
        _ = Assert.Throws<ArgumentException>(() => CompoundFileWriter.Build(entries));
        _ = Assert.Throws<ArgumentException>(() => CompoundFileWriter.BuildOfficeCrypto(entries));
    }

    /// <summary>Native Windows comparison uses the documented exceptional uppercase representatives.</summary>
    /// <param name="first">The name native structured storage sorts first.</param>
    /// <param name="second">The name native structured storage sorts second.</param>
    [Theory]
    [InlineData("\u0300", "\u023A")]
    [InlineData("\u0300", "\u2C65")]
    [InlineData("\u0300", "\u023E")]
    [InlineData("\u0300", "\u2C66")]
    [InlineData("\u1FC3", "\u1FC8")]
    [InlineData("\u1FCC", "\u1FC8")]
    [InlineData("I", "\u0131")]
    public void NativeUppercaseRepresentatives_PreserveOrder(string first, string second)
    {
        KeyValuePair<string, byte[]>[] entries = [new(second, []), new(first, [])];
        Assert.Equal([first, second], ReadTree(CompoundFileWriter.Build(entries), entries.Length));
        Assert.Equal([first, second], ReadTree(CompoundFileWriter.BuildOfficeCrypto(entries), entries.Length));
    }
    /// <summary>Names must be stored exactly and fit the native directory entry.</summary>
    /// <param name="name">An unrepresentable or forbidden name.</param>
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("A\0B")]
    [InlineData("A/B")]
    [InlineData("A\\B")]
    [InlineData("A:B")]
    [InlineData("A!B")]
    [InlineData("12345678901234567890123456789012")]
    public void InvalidNames_AreRejected(string? name)
    {
        KeyValuePair<string, byte[]>[] entries = [new(name!, [])];
        _ = Assert.Throws<ArgumentException>(() => CompoundFileWriter.Build(entries));
        _ = Assert.Throws<ArgumentException>(() => CompoundFileWriter.BuildOfficeCrypto(entries));
    }

    /// <summary>Invalid UTF-16 cannot be silently replaced before directory comparison.</summary>
    /// <param name="codeUnit">The unpaired surrogate.</param>
    /// <param name="surrounded">Whether ordinary code units precede and follow it.</param>
    [Theory]
    [InlineData(0xD800, false)]
    [InlineData(0xDC00, false)]
    [InlineData(0xD800, true)]
    [InlineData(0xDC00, true)]
    public void UnpairedSurrogates_AreRejected(int codeUnit, bool surrounded)
    {
        string name = new(checked((char)codeUnit), 1);
        if (surrounded)
        {
            name = "A" + name + "B";
        }

        KeyValuePair<string, byte[]>[] entries = [new(name, [])];
        _ = Assert.Throws<ArgumentException>(() => CompoundFileWriter.Build(entries));
        _ = Assert.Throws<ArgumentException>(() => CompoundFileWriter.BuildOfficeCrypto(entries));
    }

    private static List<string> ReadTree(byte[] file, int count)
    {
        int sectorSize = 1 << BinaryPrimitives.ReadUInt16LittleEndian(file.AsSpan(0x1E));
        int directory = checked(sectorSize * (checked((int)ReadUInt32(file, 0x30)) + 1));
        var seen = new HashSet<uint>();
        var names = new List<string>(count);
        int depth = Visit(ReadUInt32(file, directory + 0x4C));
        Assert.Equal(count, seen.Count);
        int maxDepth = 0;
        for (int nodes = count; nodes > 0; nodes >>= 1)
        {
            maxDepth++;
        }

        Assert.InRange(depth, 1, maxDepth);
        return names;

        int Visit(uint id)
        {
            if (id == uint.MaxValue)
            {
                return 0;
            }

            Assert.InRange(id, 1u, checked((uint)count));
            Assert.True(seen.Add(id), "A stream entry is aliased or cyclic.");
            int offset = checked(directory + (checked((int)id) * 128));
            Assert.Equal(2, file[offset + 0x42]);
            Assert.Equal(1, file[offset + 0x43]);
            Assert.Equal(uint.MaxValue, ReadUInt32(file, offset + 0x4C));
            int leftDepth = Visit(ReadUInt32(file, offset + 0x44));
            int length = BinaryPrimitives.ReadUInt16LittleEndian(file.AsSpan(offset + 0x40));
            Assert.InRange(length, 4, 64);
            Assert.Equal(0, BinaryPrimitives.ReadUInt16LittleEndian(file.AsSpan(offset + length - 2)));
            names.Add(JetFormat.DecodeUtf16LE(file.AsSpan(offset, length - 2)));
            int rightDepth = Visit(ReadUInt32(file, offset + 0x48));
            return 1 + Math.Max(leftDepth, rightDepth);
        }
    }

    private static uint ReadUInt32(byte[] file, int offset) =>
        BinaryPrimitives.ReadUInt32LittleEndian(file.AsSpan(offset));
}
