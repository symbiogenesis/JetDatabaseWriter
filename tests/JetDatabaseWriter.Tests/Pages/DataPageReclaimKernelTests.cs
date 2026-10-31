namespace JetDatabaseWriter.Tests.Pages;

using System;
using System.Buffers.Binary;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Verification;
using Xunit;

/// <summary>Physical compaction preserves complete page bytes, slot aliases and secure-erase behavior.</summary>
public sealed class DataPageReclaimKernelTests
{
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public void ReclaimMatchesIndependentPageImageForEverySlotAndErasePolicy(DatabaseFormat kind)
    {
        var format = JetFormat.ForNewDatabase(kind);
        foreach (int count in new[] { 1, 2, 16, 127, 255 })
        {
            byte[] source = CreatePage(format, count);
            for (int row = 0; row < count; row++)
            {
                foreach (bool erase in new[] { false, true })
                {
                    byte[] expected = ReclaimImage(format, source, row, erase);
                    byte[] actual = (byte[])source.Clone();
                    DataPageRows.ReclaimDeletedRowSpace(format, actual, row, erase);
                    Assert.Equal(expected, actual);
                }
            }
        }
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public void DuplicateUnorderedAndFlaggedOffsetsRetainTheirExistingPhysicalMeaning(DatabaseFormat kind)
    {
        var format = JetFormat.ForNewDatabase(kind);
        byte[] source = CreatePage(format, 8);
        int[] starts = [format.PageSize - 300, format.PageSize - 100, format.PageSize - 300, 0, format.PageSize - 200, format.PageSize, format.PageSize - 400, format.PageSize - 500];
        int[] flags = [0x8000, 0, 0xC000, 0x8000, 0x4000, 0xC000, 0x2000, 0];
        for (int index = 0; index < starts.Length; index++)
        {
            WriteWord(source, format.DataPage.RowsStart + (index * 2), starts[index] | flags[index]);
        }

        foreach (int row in new[] { 0, 1, 2, 4, 6, 7 })
        {
            foreach (bool erase in new[] { false, true })
            {
                byte[] actual = (byte[])source.Clone();
                DataPageRows.ReclaimDeletedRowSpace(format, actual, row, erase);
                Assert.Equal(ReclaimImage(format, source, row, erase), actual);
            }
        }
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public void RefusedSlotsAndOverlappingPayloadsLeaveTheWholePageUnchanged(DatabaseFormat kind)
    {
        var format = JetFormat.ForNewDatabase(kind);
        foreach (int row in new[] { int.MinValue, -1, 3, int.MaxValue })
        {
            byte[] actual = CreatePage(format, 3);
            byte[] before = (byte[])actual.Clone();
            Assert.Throws<JetCorruptDataException>(() => DataPageRows.ReclaimDeletedRowSpace(format, actual, row, clearFreedSpace: true));
            Assert.Equal(before, actual);
        }

        foreach (int invalid in new[] { 1, format.DataPage.RowsStart + 5 })
        {
            byte[] actual = CreatePage(format, 3);
            WriteWord(actual, format.DataPage.RowsStart + 2, invalid);
            byte[] before = (byte[])actual.Clone();
            Assert.Throws<JetCorruptDataException>(() => DataPageRows.ReclaimDeletedRowSpace(format, actual, 0, clearFreedSpace: true));
            Assert.Equal(before, actual);
        }
    }

    [Theory]
    [InlineData(0u, 8u, 10u, 0u)]
    [InlineData(uint.MaxValue, 8u, 10u, 0u)]
    [InlineData(2048u, 2u, 10u, 0u)]
    [InlineData(2048u, uint.MaxValue, 10u, 0u)]
    [InlineData(2048u, 8u, uint.MaxValue, 0u)]
    [InlineData(2048u, 8u, 10u, uint.MaxValue)]
    public void NativePreparationRefusesHostileWidthsWithoutMutation(uint pageSize, uint countOffset, uint rowsStart, uint rowIndex)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet3Mdb);
        byte[] page = CreatePage(format, 3);
        byte[] before = (byte[])page.Clone();
        DataPageKernel.PrepareReclaim(page, pageSize, countOffset, rowsStart, rowIndex, out uint rows, out uint first, out uint start, out uint end);
        Assert.Equal(0u, rows);
        Assert.Equal(0u, first);
        Assert.Equal(0u, start);
        Assert.Equal(0u, end);
        Assert.Equal(before, page);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public void ShortImagesAndOversizedDirectoriesRefuseBeforeAnyMutation(DatabaseFormat kind)
    {
        var format = JetFormat.ForNewDatabase(kind);
        byte[] source = CreatePage(format, 3);
        for (int length = 0; length < format.PageSize; length++)
        {
            byte[] page = source.AsSpan(0, length).ToArray();
            byte[] before = (byte[])page.Clone();
            Assert.Throws<JetCorruptDataException>(() => DataPageRows.ReclaimDeletedRowSpace(format, page, 0, clearFreedSpace: true));
            Assert.Equal(before, page);
        }

        foreach (int count in new[] { 0, 256, ushort.MaxValue })
        {
            byte[] page = (byte[])source.Clone();
            WriteWord(page, format.DataPage.NumRows, count);
            byte[] before = (byte[])page.Clone();
            Assert.Throws<JetCorruptDataException>(() => DataPageRows.ReclaimDeletedRowSpace(format, page, 0, clearFreedSpace: true));
            Assert.Equal(before, page);
        }
    }

    private static byte[] CreatePage(JetFormat format, int count)
    {
        byte[] result = new byte[format.PageSize + 17];
        for (int index = 0; index < result.Length; index++)
        {
            result[index] = (byte)((index * 37) & 255);
        }

        WriteWord(result, format.DataPage.NumRows, count);
        int available = format.PageSize - format.DataPage.RowsStart - (count * 2);
        int width = Math.Max(1, available / (count + 1));
        for (int index = 0; index < count; index++)
        {
            WriteWord(result, format.DataPage.RowsStart + (index * 2), (format.PageSize - ((index + 1) * width)) | ((index % 8) << 13));
        }

        return result;
    }

    private static byte[] ReclaimImage(JetFormat format, byte[] source, int selected, bool erase)
    {
        // Reconstruct the post-image from an immutable source. This oracle neither
        // performs an overlapping move nor calls the production slot/compaction code.
        int count = ReadWord(source, format.DataPage.NumRows);
        int[] offsets = new int[count];
        int first = format.PageSize;
        for (int index = 0; index < count; index++)
        {
            offsets[index] = ReadWord(source, format.DataPage.RowsStart + (index * 2));
            int position = offsets[index] & 0x1FFF;
            first = position > 0 ? Math.Min(first, position) : first;
        }

        int start = offsets[selected] & 0x1FFF;
        int end = format.PageSize;
        foreach (int raw in offsets)
        {
            int position = raw & 0x1FFF;
            end = position > start ? Math.Min(end, position) : end;
        }

        int width = end - start;
        byte[] result = (byte[])source.Clone();
        for (int position = first; position < end; position++)
        {
            if (position >= first + width)
            {
                result[position] = source[position - width];
            }
            else if (erase)
            {
                result[position] = 0;
            }
        }

        for (int index = 0; index < count; index++)
        {
            int position = offsets[index] & 0x1FFF;
            int updated = offsets[index];
            if (index == selected)
            {
                updated = 0xC000 | end;
            }
            else if (position >= first && position <= start)
            {
                updated = (offsets[index] & ~0x1FFF) | (position + width);
            }

            WriteWord(result, format.DataPage.RowsStart + (index * 2), updated);
        }

        WriteWord(result, 2, first + width - format.DataPage.RowsStart - (count * 2));
        return result;
    }

    private static int ReadWord(byte[] source, int offset) => BinaryPrimitives.ReadUInt16LittleEndian(source.AsSpan(offset, 2));

    private static void WriteWord(byte[] destination, int offset, int value) => BinaryPrimitives.WriteUInt16LittleEndian(destination.AsSpan(offset, 2), checked((ushort)value));
}
