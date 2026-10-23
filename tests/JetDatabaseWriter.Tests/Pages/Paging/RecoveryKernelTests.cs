namespace JetDatabaseWriter.Tests.Pages.Paging;

using System;
using System.IO;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Verification;
using Xunit;

/// <summary>Checks the generated recovery boundary against independent bounded and edge-case oracles.</summary>
public sealed class RecoveryKernelTests
{
    /// <summary>Page offsets accept exactly supported complete aligned pages within the Access bound.</summary>
    [Fact]
    public void IsPageOffset_Boundaries_RequireCompleteAlignedPage()
    {
        uint[] pageSizes = [0, 1, 2048, 4096, uint.MaxValue];
        ulong[] offsets = [0, 1, 2048, 4096, 2147479552, 2147481600, 2147483648, ulong.MaxValue];
        foreach (uint pageSize in pageSizes)
        {
            foreach (ulong offset in offsets)
            {
                bool expected = pageSize is 2048 or 4096 && offset < 2147483648
                    && offset % pageSize == 0 && 2147483648 - offset >= pageSize;
                Assert.Equal(expected, RecoveryKernel.IsPageOffset(offset, pageSize));
            }
        }
    }

    /// <summary>Database lengths include the exact Access size limit and reject unsigned negative representations.</summary>
    [Fact]
    public void IsDatabaseLength_Boundaries_RequireAccessSizeBound()
    {
        Assert.True(RecoveryKernel.IsDatabaseLength(0));
        Assert.True(RecoveryKernel.IsDatabaseLength(2147483648));
        Assert.False(RecoveryKernel.IsDatabaseLength(2147483649));
        Assert.False(RecoveryKernel.IsDatabaseLength(ulong.MaxValue));
    }

    /// <summary>Truncate intents cannot authorize an unaligned or longer file.</summary>
    [Fact]
    public void IsTruncateLength_BoundedInputs_MatchesRetainedPrefix()
    {
        for (uint pageSize = 2048; pageSize <= 4096; pageSize *= 2)
        {
            for (ulong maximum = 0; maximum <= 12288; maximum += 1024)
            {
                for (ulong length = 0; length <= 16384; length += 1024)
                {
                    Assert.Equal(length <= maximum && length % pageSize == 0, RecoveryKernel.IsTruncateLength(length, pageSize, maximum));
                }
            }
        }

        Assert.False(RecoveryKernel.IsTruncateLength(0, 0, 0));
        Assert.False(RecoveryKernel.IsTruncateLength(ulong.MaxValue, 2048, 2147483648));
    }

    /// <summary>Exhaustive byte pairs and boundary addresses enforce original, zero-tail and exact after-image authorization.</summary>
    [Fact]
    public void ByteAuthorization_AllBytePairs_MatchesIndependentOracle()
    {
        ulong[] offsets = [0, 1, 4095, 4096, 4097, 2147483647];
        foreach (bool recovering in new[] { false, true })
        {
            foreach (ulong offset in offsets)
            {
                for (int actual = 0; actual <= byte.MaxValue; actual++)
                {
                    for (int original = 0; original <= byte.MaxValue; original++)
                    {
                        bool expected = actual == original;
                        if (recovering && offset >= 4096 && actual == 0)
                        {
                            expected = true;
                        }

                        bool initial = RecoveryKernel.OriginalOrZeroByte(recovering, offset, 4096, (byte)actual, (byte)original);
                        Assert.Equal(expected, initial);
                        Assert.Equal(expected || actual == 127, RecoveryKernel.IncludeAfterImageByte(initial, (byte)actual, 127));
                    }
                }
            }
        }
    }

    /// <summary>Real writer call sites retain signed validation before entering unsigned generated code.</summary>
    /// <param name="offset">An invalid signed page offset or retained length.</param>
    [Theory]
    [InlineData(-1L)]
    [InlineData(long.MinValue)]
    [InlineData(1L)]
    [InlineData(long.MaxValue)]
    public void JournalCallSites_InvalidSignedRanges_RefuseWithoutRecording(long offset)
    {
        string path = Path.Combine(Path.GetTempPath(), "jdw-kernel-" + Guid.NewGuid().ToString("N") + ".mdb");
        File.WriteAllBytes(path, new byte[4096]);
        try
        {
            using var database = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
            using var journal = PersistentRollbackJournal.Create(database, 2048);
            long preparedLength = new FileInfo(path + ".jdw-journal").Length;
            Assert.Throws<JetOperationException>(() => journal.RecordBeforeWrite(offset, new byte[2048]));
            Assert.Throws<JetOperationException>(() => journal.RecordBeforeTruncate(offset));
            Assert.Equal(preparedLength, new FileInfo(path + ".jdw-journal").Length);
        }
        finally
        {
            File.Delete(path);
            File.Delete(path + ".jdw-journal");
        }
    }
}
