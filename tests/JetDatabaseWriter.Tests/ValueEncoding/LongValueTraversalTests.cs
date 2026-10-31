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
    public void VisitedSet_ZeroIdentityDoesNotAllocateDefaultStorage()
    {
        LongValueVisitedSet entries = default;
        Assert.False(entries.Add(0));
        long before = GC.GetAllocatedBytesForCurrentThread();
        bool inserted = false;
        for (int attempt = 0; attempt < 64; attempt++)
        {
            inserted |= entries.Add(0);
        }

        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.False(inserted);
        Assert.Equal(0, allocated);
        Assert.True(entries.Add(1));
        Assert.False(entries.Add(1));
    }

    [Fact]
    public void VisitedSet_InsertionsAndDuplicatesBelowHalfLoadDoNotAllocate()
    {
        LongValueVisitedSet entries = default;
        Assert.True(entries.Add(1));
        Assert.False(entries.Add(1));
        long before = GC.GetAllocatedBytesForCurrentThread();
        bool inserted = true;
        bool duplicateInserted = false;
        for (uint pointer = 2; pointer <= 7; pointer++)
        {
            inserted &= entries.Add(pointer);
            duplicateInserted |= entries.Add(pointer);
        }

        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.True(inserted);
        Assert.False(duplicateInserted);
        Assert.Equal(0, allocated);
    }

    [Theory]
    [InlineData(8u)]
    [InlineData(16u)]
    [InlineData(32u)]
    [InlineData(64u)]
    [InlineData(256u)]
    public void VisitedSet_GrowsOnceAfterDistinctInsertionAtHalfLoad(uint threshold)
    {
        LongValueVisitedSet entries = default;
        for (uint pointer = 1; pointer <= threshold; pointer++)
        {
            Assert.True(entries.Add(pointer));
        }

        Assert.False(entries.Add(1));
        long before = GC.GetAllocatedBytesForCurrentThread();
        bool duplicateInserted = false;
        for (uint pointer = 1; pointer <= threshold; pointer++)
        {
            duplicateInserted |= entries.Add(pointer);
        }

        long duplicateAllocation = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.False(duplicateInserted);
        Assert.Equal(0, duplicateAllocation);
        before = GC.GetAllocatedBytesForCurrentThread();
        bool inserted = entries.Add(threshold + 1);
        long growthAllocation = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.True(inserted);
        Assert.InRange(growthAllocation, threshold * 16L, (threshold * 16L) + 64);
        before = GC.GetAllocatedBytesForCurrentThread();
        inserted = entries.Add(threshold + 2);
        long nextAllocation = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.True(inserted);
        Assert.Equal(0, nextAllocation);
        for (uint pointer = 1; pointer <= threshold + 2; pointer++)
        {
            Assert.False(entries.Add(pointer));
        }
    }

    [Fact]
    public void VisitedSet_DuplicatesAtHalfLoadDoNotAllocate()
    {
        LongValueVisitedSet entries = default;
        for (uint pointer = 1; pointer <= 8; pointer++)
        {
            Assert.True(entries.Add(pointer));
        }

        Assert.False(entries.Add(1));
        long before = GC.GetAllocatedBytesForCurrentThread();
        bool inserted = false;
        for (int attempt = 0; attempt < 64; attempt++)
        {
            inserted |= entries.Add(1);
        }

        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.False(inserted);
        Assert.Equal(0, allocated);
        Assert.True(entries.Add(9));
        for (uint pointer = 1; pointer <= 9; pointer++)
        {
            Assert.False(entries.Add(pointer));
        }
    }

    [Fact]
    public void GeneratedVisitedController_GrowthRequestRetainsSourceUntilCompletion()
    {
        uint[] source = new uint[16];
        for (uint pointer = 1; pointer <= 8; pointer++)
        {
            Assert.Equal((byte)2, LongValueKernel.AddVisited(source, pointer));
        }

        uint[] before = (uint[])source.Clone();
        for (int attempt = 0; attempt < 2; attempt++)
        {
            ulong prepared = LongValueKernel.PrepareVisitedAdd(source, 8, 9);
            Assert.Equal((byte)2, LongValueKernel.VisitedAddResult(prepared));
            Assert.True(LongValueKernel.VisitedAddHasHighFlag(prepared));
            Assert.False(LongValueKernel.VisitedAddIsExhausted(prepared));
            Assert.Equal(32u, LongValueKernel.VisitedAddPayload(prepared));
            Assert.Equal(before, source);
        }

        ulong duplicate = LongValueKernel.PrepareVisitedAdd(source, 8, 1);
        Assert.Equal(0UL, duplicate);
        Assert.Equal((byte)0, LongValueKernel.VisitedAddResult(duplicate));
        Assert.Equal(before, source);

        uint[] destination = new uint[32];
        uint nextCount = LongValueKernel.CompleteVisitedGrowth(source, destination, 8, 9);
        Assert.Equal(9u, nextCount);
        Assert.Equal(before, source);
        for (uint pointer = 1; pointer <= 9; pointer++)
        {
            Assert.True(LongValueKernel.ContainsVisited(destination, pointer));
        }

        Assert.False(LongValueKernel.ContainsVisited(destination, 10));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void GeneratedVisitedController_InitialAllocationHasExactSoleMembership(bool useNullStorage)
    {
        uint[]? source = useNullStorage ? null : [];
        ulong unchanged = LongValueKernel.PrepareVisitedAdd(source, 0, 0);
        Assert.Equal(0UL, unchanged);
        Assert.Equal((byte)0, LongValueKernel.VisitedAddResult(unchanged));
        for (int attempt = 0; attempt < 2; attempt++)
        {
            ulong prepared = LongValueKernel.PrepareVisitedAdd(source, 0, uint.MaxValue);
            Assert.Equal((byte)2, LongValueKernel.VisitedAddResult(prepared));
            Assert.True(LongValueKernel.VisitedAddHasHighFlag(prepared));
            Assert.False(LongValueKernel.VisitedAddIsExhausted(prepared));
            Assert.Equal(16u, LongValueKernel.VisitedAddPayload(prepared));
            Assert.True(source is null || source.Length == 0);
        }

        uint[] destination = new uint[16];
        Assert.Equal(1u, LongValueKernel.CompleteVisitedGrowth(source, destination, 0, uint.MaxValue));
        Assert.Single(Array.FindAll(destination, static value => value != 0));
        Assert.True(LongValueKernel.ContainsVisited(destination, uint.MaxValue));
        Assert.False(LongValueKernel.ContainsVisited(destination, 1));
        Assert.True(source is null || source.Length == 0);
    }

    [Fact]
    public void GeneratedVisitedController_ZeroOnInitializedTablePreservesRetainedCount()
    {
        uint[] source = new uint[16];
        for (uint pointer = 1; pointer <= 3; pointer++)
        {
            Assert.Equal((byte)2, LongValueKernel.AddVisited(source, pointer));
        }

        uint[] before = (uint[])source.Clone();
        Assert.Equal(0UL, LongValueKernel.PrepareVisitedAdd(source, 3, 0));
        Assert.Equal(before, source);
        ulong inserted = LongValueKernel.PrepareVisitedAdd(source, 3, 4);
        Assert.Equal(4UL, inserted);
        Assert.Equal((byte)1, LongValueKernel.VisitedAddResult(inserted));
        Assert.Equal(4u, LongValueKernel.VisitedAddPayload(inserted));
        for (uint pointer = 1; pointer <= 4; pointer++)
        {
            Assert.True(LongValueKernel.ContainsVisited(source, pointer));
        }
    }

    [Theory]
    [InlineData(0UL, (byte)0, 0u, false, false)]
    [InlineData(1UL, (byte)1, 1u, false, false)]
    [InlineData(0x0000_0000_FFFF_FFFFUL, (byte)1, uint.MaxValue, false, false)]
    [InlineData(0x8000_0000_0000_0001UL, (byte)2, 1u, true, false)]
    [InlineData(0x8000_0000_FFFF_FFFFUL, (byte)2, uint.MaxValue, true, false)]
    [InlineData(ulong.MaxValue, (byte)3, uint.MaxValue, true, true)]
    public void GeneratedVisitedController_DecisionModesHaveIndependentBoundaryValues(ulong decision, byte result, uint payload, bool highFlag, bool exhausted)
    {
        Assert.Equal(result, LongValueKernel.VisitedAddResult(decision));
        Assert.Equal(payload, LongValueKernel.VisitedAddPayload(decision));
        Assert.Equal(highFlag, LongValueKernel.VisitedAddHasHighFlag(decision));
        Assert.Equal(exhausted, LongValueKernel.VisitedAddIsExhausted(decision));
    }

    [Theory]
    [InlineData(1u, 1UL, 0x8000_0000_0000_0001UL)]
    [InlineData(uint.MaxValue, 0x0000_0000_FFFF_FFFFUL, 0x8000_0000_FFFF_FFFFUL)]
    public void GeneratedVisitedController_ConstructorsHaveIndependentBoundaryWords(uint payload, ulong inserted, ulong growth)
    {
        Assert.Equal(inserted, LongValueKernel.VisitedInsertedDecision(payload));
        Assert.Equal(growth, LongValueKernel.VisitedGrowthDecision(payload));
    }

    [Fact]
    public void GeneratedVisitedSet_DuplicateInsertionLeavesTableUnchanged()
    {
        uint[] entries = new uint[16];
        Assert.Equal((byte)2, LongValueKernel.AddVisited(entries, uint.MaxValue));
        uint[] before = (uint[])entries.Clone();
        Assert.Equal((byte)1, LongValueKernel.AddVisited(entries, uint.MaxValue));
        Assert.Equal(before, entries);
    }

    [Fact]
    public void GeneratedVisitedSet_PreservesWrappedClustersAndRehashMembership()
    {
        uint[] source = new uint[16];
        var expected = new HashSet<uint>();
        for (uint pointer = 1; expected.Count < source.Length; pointer++)
        {
            ulong hash = ((ulong)pointer * 2654435761) + (pointer / 256) + (pointer / 65536);
            if (hash % (ulong)source.Length != 15)
            {
                continue;
            }

            Assert.Equal((byte)2, LongValueKernel.AddVisited(source, pointer));
            expected.Add(pointer);
            foreach (uint member in expected)
            {
                Assert.True(LongValueKernel.ContainsVisited(source, member));
            }
        }

        uint[] before = (uint[])source.Clone();
        foreach (uint pointer in expected)
        {
            Assert.Equal((byte)1, LongValueKernel.AddVisited(source, pointer));
        }

        Assert.Equal(before, source);
        Assert.Equal((byte)0, LongValueKernel.AddVisited(source, uint.MaxValue));
        Assert.Equal(before, source);
        uint[] destination = new uint[33];
        LongValueKernel.RehashVisited(source, destination);
        Assert.Equal(before, source);
        for (uint pointer = 0; pointer < 1024; pointer++)
        {
            Assert.Equal(expected.Contains(pointer), LongValueKernel.ContainsVisited(destination, pointer));
        }

        Assert.False(LongValueKernel.ContainsVisited(destination, uint.MaxValue));
        Assert.Equal((byte)2, LongValueKernel.AddVisited(destination, uint.MaxValue));
        Assert.True(LongValueKernel.ContainsVisited(destination, uint.MaxValue));
    }

    [Fact]
    public void GeneratedVisitedSet_RehashesArbitraryDuplicateSourceIdentities()
    {
        uint[] source = [7, 0, 7, uint.MaxValue, 0, 7, 11, uint.MaxValue];
        uint[] before = (uint[])source.Clone();
        uint[] destination = new uint[17];
        LongValueKernel.RehashVisited(source, destination);
        Assert.Equal(before, source);
        Assert.Equal(3, Array.FindAll(destination, static value => value != 0).Length);
        foreach (uint pointer in new uint[] { 7, 11, uint.MaxValue })
        {
            Assert.True(LongValueKernel.ContainsVisited(destination, pointer));
            Assert.Equal((byte)1, LongValueKernel.AddVisited(destination, pointer));
        }

        Assert.False(LongValueKernel.ContainsVisited(destination, 1));
        Assert.Equal((byte)2, LongValueKernel.AddVisited(destination, 1));
    }

    [Fact]
    public void GeneratedVisitedSet_HandlesSingletonProbeAndEmptyRehash()
    {
        uint[] singleton = new uint[1];
        Assert.False(LongValueKernel.ContainsVisited(singleton, 0));
        Assert.False(LongValueKernel.ContainsVisited(singleton, 1));
        Assert.Equal((byte)2, LongValueKernel.AddVisited(singleton, uint.MaxValue));
        Assert.True(LongValueKernel.ContainsVisited(singleton, uint.MaxValue));
        Assert.Equal((byte)1, LongValueKernel.AddVisited(singleton, uint.MaxValue));
        Assert.Equal((byte)0, LongValueKernel.AddVisited(singleton, 1));
        Assert.Equal(uint.MaxValue, singleton[0]);
        uint[] empty = new uint[16];
        uint[] destination = new uint[32];
        LongValueKernel.RehashVisited(empty, destination);
        Assert.All(destination, value => Assert.Equal(0u, value));
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
