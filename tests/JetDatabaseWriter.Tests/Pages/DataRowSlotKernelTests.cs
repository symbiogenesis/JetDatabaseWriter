namespace JetDatabaseWriter.Tests.Pages;

using System;
using System.Buffers.Binary;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Verification;
using Xunit;

/// <summary>Bounds of physical row slots must follow distinct offsets within the acquired logical page.</summary>
public sealed class DataRowSlotKernelTests
{
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public void ShortPageRefusesWithoutReadingMissingDirectoryBytes(DatabaseFormat kind)
    {
        var format = JetFormat.ForNewDatabase(kind);
        for (int length = 0; length < format.PageSize; length++)
        {
            byte[] page = new byte[length];
            if (length >= format.DataPage.RowsStart + 2)
            {
                BinaryPrimitives.WriteUInt16LittleEndian(page.AsSpan(format.DataPage.NumRows, 2), 1);
                BinaryPrimitives.WriteUInt16LittleEndian(page.AsSpan(format.DataPage.RowsStart, 2), checked((ushort)(format.DataPage.RowsStart + 2)));
            }

            Assert.False(DataPageRows.TryGetSlotBound(format, page, 0, out RowBound bound));
            Assert.Equal(default, bound);
        }
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public void UnorderedDuplicateAndFlaggedSlotsSelectNextDistinctPhysicalOffset(DatabaseFormat kind)
    {
        var format = JetFormat.ForNewDatabase(kind);
        int top = format.PageSize;
        int[] offsets = [top - 400, (top - 100) | 0x8000, (top - 400) | 0xC000, (top - 250) | 0x4000];
        byte[] page = BuildPage(format, offsets);
        Assert.True(DataPageRows.TryGetSlotBound(format, page, 0, out RowBound first));
        Assert.Equal(new RowBound(0, top - 400, 150), first);
        Assert.True(DataPageRows.TryGetSlotBound(format, page, 2, out RowBound duplicate));
        Assert.Equal(new RowBound(2, top - 400, 150), duplicate);
        Assert.True(DataPageRows.TryGetSlotBound(format, page, 1, out RowBound last));
        Assert.Equal(new RowBound(1, top - 100, 100), last);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public void InvalidIdentityAndDirectoryOverlapRefuseWithoutMutation(DatabaseFormat kind)
    {
        var format = JetFormat.ForNewDatabase(kind);
        byte[] page = BuildPage(format, [format.DataPage.RowsStart + 3, format.PageSize]);
        byte[] original = (byte[])page.Clone();
        foreach (int index in new[] { int.MinValue, -1, 0, 1, 2, int.MaxValue })
        {
            Assert.False(DataPageRows.TryGetSlotBound(format, page, index, out RowBound bound));
            Assert.Equal(default, bound);
            Assert.Equal(original, page);
        }
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public void SlotSelectionMatchesIndependentMinimumOracle(DatabaseFormat kind)
    {
        var format = JetFormat.ForNewDatabase(kind);
        var random = new Random(1871);
        for (int specimen = 0; specimen < 64; specimen++)
        {
            int[] raw = new int[32];
            int directoryEnd = format.DataPage.RowsStart + (raw.Length * 2);
            for (int index = 0; index < raw.Length; index++)
            {
#pragma warning disable CA5394 // Seeded specimen generation is reproducible and makes no security decisions.
                raw[index] = random.Next(0, format.PageSize + 1) | (random.Next(0, 8) << 13);
#pragma warning restore CA5394
            }

            byte[] page = BuildPage(format, raw);
            for (int index = 0; index < raw.Length; index++)
            {
                int start = raw[index] & 0x1FFF;
                bool valid = start >= directoryEnd && start < format.PageSize;
                Assert.Equal(valid, DataPageRows.TryGetSlotBound(format, page, index, out RowBound actual));
                if (valid)
                {
                    int end = format.PageSize;
                    foreach (int candidate in raw)
                    {
                        int offset = candidate & 0x1FFF;
                        end = offset > start ? Math.Min(end, offset) : end;
                    }

                    Assert.Equal(new RowBound(index, start, end - start), actual);
                }
                else
                {
                    Assert.Equal(default, actual);
                }
            }
        }
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public void OversizedPoolBufferCannotExtendLogicalDirectory(DatabaseFormat kind)
    {
        var format = JetFormat.ForNewDatabase(kind);
        byte[] page = new byte[131072];
        BinaryPrimitives.WriteUInt16LittleEndian(page.AsSpan(format.DataPage.NumRows, 2), ushort.MaxValue);
        int firstOutside = (format.PageSize - format.DataPage.RowsStart) / 2;
        BinaryPrimitives.WriteUInt16LittleEndian(page.AsSpan(format.DataPage.RowsStart + (firstOutside * 2), 2), 8191);
        Assert.False(DataPageRows.TryGetSlotBound(format, page, firstOutside, out RowBound bound));
        Assert.Equal(default, bound);
        Assert.False(DataPageRows.TryGetSlotBound(format, page, ushort.MaxValue - 1, out bound));
        Assert.Equal(default, bound);
    }

    [Theory]
    [InlineData(0u, 0u, 0u, 0u)]
    [InlineData(uint.MaxValue, 0u, 0u, 0u)]
    [InlineData(2048u, uint.MaxValue, 12u, 0u)]
    [InlineData(2048u, 2047u, 12u, 0u)]
    [InlineData(2048u, 8u, uint.MaxValue, 0u)]
    [InlineData(2048u, 8u, 12u, uint.MaxValue)]
    public void NativeBoundaryRefusesInvalidLayoutAndIdentity(uint pageSize, uint countOffset, uint rowsStart, uint rowIndex)
    {
        byte[] page = new byte[2048];
        RecoveryKernel.ReadDataRowSlot(page, pageSize, countOffset, rowsStart, rowIndex, out uint start, out uint end);
        Assert.Equal(0u, start);
        Assert.Equal(0u, end);
    }

    private static byte[] BuildPage(JetFormat format, int[] slots)
    {
        byte[] page = new byte[format.PageSize];
        BinaryPrimitives.WriteUInt16LittleEndian(page.AsSpan(format.DataPage.NumRows, 2), checked((ushort)slots.Length));
        for (int index = 0; index < slots.Length; index++)
        {
            BinaryPrimitives.WriteUInt16LittleEndian(page.AsSpan(format.DataPage.RowsStart + (index * 2), 2), checked((ushort)slots[index]));
        }

        return page;
    }
}
