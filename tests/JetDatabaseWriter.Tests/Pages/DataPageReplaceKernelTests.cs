namespace JetDatabaseWriter.Tests.Pages;

using System;
using System.Buffers.Binary;
using JetDatabaseWriter.Verification;
using Xunit;

/// <summary>Same-width row replacement has exact byte effects and refuses ambiguous identities.</summary>
public sealed class DataPageReplaceKernelTests
{
    /// <summary>Field patches copy exact payload bytes and change only the selected null or Boolean bit.</summary>
    [Fact]
    public void ColumnPatchPreservesAllUnassignedBytesAndMaskBits()
    {
        foreach (int width in new[] { 0, 1, 2, 4, 8, 12, 16 })
        {
            for (int bit = 0; bit < 8; bit++)
            {
                foreach (byte before in new byte[] { 0, 1, 0x55, 0xAA, 0xFF })
                {
                    foreach (byte after in new byte[] { 0, 1, 0x55, 0xAA, 0xFF })
                    {
                        byte[] row = new byte[40];
                        row.AsSpan().Fill(0x6B);
                        row[37] = before;
                        byte[] source = new byte[40];
                        source.AsSpan().Fill(0x39);
                        source[38] = after;
                        byte[] originalSource = (byte[])source.Clone();
                        byte[] expected = (byte[])row.Clone();
                        Array.Copy(source, 3, expected, 4, width);
                        int mask = 1 << bit;
                        expected[37] = (byte)((before & ~mask) | (after & mask));

                        Assert.True(DataPageKernel.PatchColumn(row, source, 4u, 3u, checked((uint)width), 30u, 29u, 37u, 38u, checked((uint)bit)));
                        Assert.Equal(expected, row);
                        Assert.Equal(originalSource, source);
                    }
                }
            }
        }
    }

    /// <summary>Alias, overflow, metadata overlap and invalid mask coordinates refuse before mutation.</summary>
    [Fact]
    public void ColumnPatchRefusalLeavesTheCompleteRowUnchanged()
    {
        foreach ((uint target, uint input, uint width, uint targetEnd, uint sourceEnd, uint mask, uint sourceMask, uint bit) in new[]
        {
            (uint.MaxValue, 3u, 4u, 30u, 29u, 37u, 38u, 0u),
            (4u, uint.MaxValue, 4u, 30u, 29u, 37u, 38u, 0u),
            (4u, 3u, uint.MaxValue, 30u, 29u, 37u, 38u, 0u),
            (4u, 3u, 4u, 38u, 29u, 37u, 38u, 0u),
            (4u, 3u, 4u, 30u, 39u, 37u, 38u, 0u),
            (4u, 3u, 4u, 30u, 29u, 40u, 38u, 0u),
            (4u, 3u, 4u, 30u, 29u, 37u, 40u, 0u),
            (4u, 3u, 4u, 30u, 29u, 37u, 38u, 8u),
            (28u, 3u, 4u, 30u, 29u, 37u, 38u, 0u),
        })
        {
            byte[] row = new byte[40];
            row.AsSpan().Fill(0x6B);
            byte[] before = (byte[])row.Clone();
            Assert.False(DataPageKernel.PatchColumn(row, new byte[40], target, input, width, targetEnd, sourceEnd, mask, sourceMask, bit));
            Assert.Equal(before, row);
        }

        byte[] aliased = new byte[40];
        byte[] original = (byte[])aliased.Clone();
        Assert.False(DataPageKernel.PatchColumn(aliased, aliased, 4u, 3u, 4u, 30u, 29u, 37u, 38u, 0u));
        Assert.Equal(original, aliased);
    }

    /// <summary>Every unselected page byte and every slot entry stays exact.</summary>
    /// <param name="pageSize">The physical page width.</param>
    [Theory]
    [InlineData(2048)]
    [InlineData(4096)]
    public void ReplacementChangesOnlyTheSelectedPhysicalRow(int pageSize)
    {
        for (int selected = 0; selected < 4; selected++)
        {
            byte[] page = CreatePage(pageSize);
            byte[] expected = (byte[])page.Clone();
            byte[] replacement = new byte[100];
            replacement.AsSpan().Fill(0x37);
            int start = pageSize - ((selected + 1) * 100);
            replacement.CopyTo(expected, start);

            Assert.True(DataPageKernel.ReplaceRow(page, checked((uint)pageSize), 8u, 10u, checked((uint)selected), checked((uint)start), replacement));
            Assert.Equal(expected, page);
        }
    }

    /// <summary>Stale coordinates, live aliases, flagged slots and changed widths cannot publish.</summary>
    [Fact]
    public void RefusedReplacementsLeaveEveryPageByteUnchanged()
    {
        foreach (int failure in new[] { 0, 1, 2, 3, 4, 5 })
        {
            byte[] page = CreatePage(2048);
            uint start = 1948u;
            byte[] replacement = new byte[100];
            if (failure == 0)
            {
                start--;
            }
            else if (failure == 1)
            {
                replacement = new byte[99];
            }
            else if (failure == 2)
            {
                BinaryPrimitives.WriteUInt16LittleEndian(page.AsSpan(12), 1948);
            }
            else if (failure == 3)
            {
                BinaryPrimitives.WriteUInt16LittleEndian(page.AsSpan(10), 0x879C);
            }
            else if (failure == 4)
            {
                BinaryPrimitives.WriteUInt16LittleEndian(page.AsSpan(12), 3);
            }
            else
            {
                replacement = page;
            }

            byte[] before = (byte[])page.Clone();
            Assert.False(DataPageKernel.ReplaceRow(page, 2048u, 8u, 10u, 0u, start, replacement));
            Assert.Equal(before, page);
        }
    }

    private static byte[] CreatePage(int pageSize)
    {
        byte[] page = new byte[pageSize];
        page.AsSpan().Fill(0xA5);
        BinaryPrimitives.WriteUInt16LittleEndian(page.AsSpan(8), 4);
        for (int index = 0; index < 4; index++)
        {
            BinaryPrimitives.WriteUInt16LittleEndian(page.AsSpan(10 + (index * 2)), checked((ushort)(pageSize - ((index + 1) * 100))));
        }

        return page;
    }
}
