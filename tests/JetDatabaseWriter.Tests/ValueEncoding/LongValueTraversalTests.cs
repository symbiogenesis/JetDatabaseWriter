namespace JetDatabaseWriter.Tests.ValueEncoding;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.LongValues;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Verification;
using Xunit;
using static JetDatabaseWriter.Schema.JetTypeInfo;

public sealed class LongValueTraversalTests
{
    [Theory]
    [InlineData(1)]
    [InlineData(8)]
    [InlineData(9)]
    [InlineData(17)]
    [InlineData(129)]
    public async Task CopyPayloadAsync_RefusesRepeatedIdentityBeforeReacquisition(int distinctRows)
    {
        int acquired = 0;
        int released = 0;
        byte[] page = new byte[5];
        await using var output = new MemoryStream();
        await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
            await LongValueStore.CopyPayloadAsync(
                LongValueDescriptor.Chained(distinctRows + 1, 256, 0),
                ReadOnlyMemory<byte>.Empty,
                output,
                64,
                distinctRows + 1,
                distinctRows + 1,
                (pointer, _) =>
                {
                    acquired++;
                    Assert.Equal(checked((uint)(acquired * 256)), pointer);
                    Wu32(page, 0, acquired == distinctRows ? 256u : checked((uint)((acquired + 1) * 256)));
                    page[4] = 42;
                    return new ValueTask<LvalRowLocation>(new LvalRowLocation(page, 0, page.Length, null)
                    {
                        Lease = PageLease.Own(page, page.Length, buffer =>
                        {
                            Assert.Same(page, buffer);
                            released++;
                        }),
                    });
                },
                TestContext.Current.CancellationToken));
        Assert.Equal(distinctRows, acquired);
        Assert.Equal(acquired, released);
        Assert.Equal(distinctRows, output.Length);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(3)]
    [InlineData(4)]
    public async Task CopyPayloadAsync_RefusesNoProgressRowAfterOneAcquisition(int rowSize)
    {
        int acquired = 0;
        int released = 0;
        byte[] page = new byte[8];
        await using var output = new MemoryStream();
        await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
            await LongValueStore.CopyPayloadAsync(
                LongValueDescriptor.Chained(100, 256, 0),
                ReadOnlyMemory<byte>.Empty,
                output,
                64,
                100,
                100,
                (_, _) =>
                {
                    acquired++;
                    return new ValueTask<LvalRowLocation>(new LvalRowLocation(page, 0, rowSize, null) { Lease = PageLease.Own(page, page.Length, _ => released++) });
                },
                TestContext.Current.CancellationToken));
        Assert.Equal(1, acquired);
        Assert.Equal(1, released);
        Assert.Equal(0, output.Length);
    }

    [Fact]
    public void VisitedSet_MatchesIndependentMembershipThroughCollisionsAndGrowth()
    {
        var expected = new HashSet<uint>();
        LongValueVisitedSet actual = default;
        for (uint index = 1; index <= 129; index++)
        {
            uint pointer = checked(index * 16777216);
            Assert.Equal(expected.Add(pointer), actual.Add(pointer));
            Assert.False(actual.Add(pointer));
        }

        for (uint index = 1; index <= 4097; index++)
        {
            uint pointer = checked(index * 65536);
            Assert.Equal(expected.Add(pointer), actual.Add(pointer));
            Assert.False(actual.Add(pointer));
        }

        foreach (uint pointer in expected)
        {
            Assert.False(actual.Add(pointer));
        }
    }

    [Fact]
    public void GeneratedRowCount_RefusesEveryInvalidSliceAndBoundsWideInputs()
    {
        for (int total = 0; total <= 11; total++)
        {
            for (int copied = 0; copied <= 12; copied++)
            {
                for (int start = 0; start <= 12; start++)
                {
                    for (int size = 0; size <= 12; size++)
                    {
                        uint expected = copied < total && size > 4 && start + size <= 11
                            ? checked((uint)Math.Min(size - 4, total - copied))
                            : 0;
                        Assert.Equal(expected, LongValueKernel.ChainPayloadCount(
                            checked((uint)total),
                            checked((uint)copied),
                            11,
                            checked((uint)start),
                            checked((uint)size),
                            4,
                            false));
                    }
                }
            }
        }

        Assert.Equal(0u, LongValueKernel.ChainPayloadCount(uint.MaxValue, 0, uint.MaxValue, uint.MaxValue, 5, 4, false));
        Assert.Equal(0u, LongValueKernel.NextVisitedCapacity(1073741824));
        Assert.Equal(0u, LongValueKernel.NextVisitedCapacity(uint.MaxValue));
    }
}
