namespace JetDatabaseWriter.Tests.Pages.Paging;

using System;
using JetDatabaseWriter.Verification;
using Xunit;

/// <summary>Checks complete journal-content scans with independent byte-level authorization oracles.</summary>
public sealed class JournalContentKernelTests
{
    /// <summary>Mixed original, repeated after-images and replay zeros authorize exactly their own bytes.</summary>
    /// <param name="pageSize">The native page size.</param>
    /// <param name="recovering">Whether a durable replay marker admits removed-tail zeros.</param>
    [Theory]
    [InlineData(2048, false)]
    [InlineData(2048, true)]
    [InlineData(4096, false)]
    [InlineData(4096, true)]
    public void Authorization_MixedImages_MatchesIndependentOracle(int pageSize, bool recovering)
    {
        byte[] original = new byte[pageSize];
        byte[] current = new byte[pageSize];
        byte[] first = new byte[pageSize + 40];
        byte[] second = new byte[pageSize + 40];
        bool[] admitted = new bool[pageSize + 1];
        bool[] expected = new bool[pageSize + 1];
        admitted[^1] = true;
        expected[^1] = true;
        for (int index = 0; index < pageSize; index++)
        {
            original[index] = 11;
            first[index + 8] = 22;
            second[index + 8] = 33;
            current[index] = (index % 5) switch { 0 => 11, 1 => 22, 2 => 33, 3 => 0, _ => 44 };
            expected[index] = current[index] == 11 || (recovering && index >= pageSize / 2 && current[index] == 0);
        }

        Assert.Equal(1, RecoveryKernel.InitializeJournalAuthorization(current, original, admitted, (uint)pageSize, recovering, 4096, (ulong)(4096 + (pageSize / 2))));
        Assert.Equal(expected, admitted);
        Assert.False(RecoveryKernel.CheckJournalAuthorization(admitted, (uint)pageSize));
        foreach (byte[] record in new[] { first, second, first })
        {
            Assert.True(RecoveryKernel.IncludeJournalAfterImage(current, record, admitted, (uint)pageSize));
            for (int index = 0; index < pageSize; index++)
            {
                expected[index] |= current[index] == record[index + 8];
            }

            Assert.Equal(expected, admitted);
        }

        Assert.False(RecoveryKernel.CheckJournalAuthorization(admitted, (uint)pageSize));
        byte[] complete = new byte[pageSize + 8];
        current.CopyTo(complete, 8);
        Assert.True(RecoveryKernel.IncludeJournalAfterImage(current, complete, admitted, (uint)pageSize));
        Assert.True(RecoveryKernel.CheckJournalAuthorization(admitted, (uint)pageSize));
        Assert.Equal(2, RecoveryKernel.InitializeJournalAuthorization(original, original, admitted, (uint)pageSize, recovering, 4096, 0));
        Assert.True(RecoveryKernel.CheckJournalAuthorization(admitted, (uint)pageSize));
        Assert.True(admitted[^1]);
    }

    /// <summary>Invalid lengths refuse before changing any retained authorization state.</summary>
    [Fact]
    public void Authorization_InvalidRanges_PreserveScratchAndRefuse()
    {
        byte[] page = [1, 2, 3];
        bool[] admitted = [true, false, true];
        bool[] saved = (bool[])admitted.Clone();
        Assert.Equal(0, RecoveryKernel.InitializeJournalAuthorization(page, page, admitted, 4, false, 0, 0));
        Assert.Equal(0, RecoveryKernel.InitializeJournalAuthorization(page, page, admitted, 3, false, ulong.MaxValue, 0));
        Assert.False(RecoveryKernel.IncludeJournalAfterImage(page, new byte[10], admitted, 3));
        Assert.False(RecoveryKernel.IncludeJournalAfterImage(page, new byte[12], admitted, uint.MaxValue));
        Assert.Equal(saved, admitted);
        Assert.False(RecoveryKernel.CheckJournalAuthorization(admitted, 4));
        Assert.False(RecoveryKernel.CheckJournalZeroExtension(page, 2, 4));
        Assert.False(RecoveryKernel.CheckJournalZeroExtension(page, 3, 2));
    }

    /// <summary>Empty intervals succeed; every nonzero extension byte causes refusal without changing input.</summary>
    [Fact]
    public void ZeroExtension_EveryConflictAndEmptyInterval_AreExact()
    {
        byte[] page = new byte[17];
        page[0] = 99;
        Assert.True(RecoveryKernel.CheckJournalZeroExtension(page, 1, 17));
        Assert.True(RecoveryKernel.CheckJournalZeroExtension(page, 17, 17));
        Assert.True(RecoveryKernel.CheckJournalAuthorization([], 0));
        Assert.Equal(2, RecoveryKernel.InitializeJournalAuthorization([], [], [], 0, false, ulong.MaxValue, 0));
        for (int index = 1; index < page.Length; index++)
        {
            page[index] = 1;
            Assert.False(RecoveryKernel.CheckJournalZeroExtension(page, 1, 17));
            page[index] = 0;
        }

        Assert.Equal(99, page[0]);
    }

    /// <summary>Partial final pages leave scratch suffixes unchanged and reject every individual unauthorized byte.</summary>
    [Fact]
    public void Authorization_PartialPage_PreservesSuffixAndFindsEveryConflict()
    {
        byte[] original = new byte[31];
        byte[] current = new byte[31];
        bool[] admitted = new bool[31];
        for (uint count = 0; count <= 17; count++)
        {
            Array.Fill(admitted, true);
            Assert.Equal(2, RecoveryKernel.InitializeJournalAuthorization(current, original, admitted, count, false, 0, 0));
            for (int index = 0; index < count; index++)
            {
                admitted[index] = false;
                Assert.False(RecoveryKernel.CheckJournalAuthorization(admitted, count));
                admitted[index] = true;
            }

            for (int index = checked((int)count); index < admitted.Length; index++)
            {
                Assert.True(admitted[index]);
            }
        }

        Assert.Equal(new byte[31], original);
        Assert.Equal(new byte[31], current);
    }
}
