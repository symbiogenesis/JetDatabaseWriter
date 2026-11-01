namespace JetDatabaseWriter.Tests.Pages.Paging;

using JetDatabaseWriter.Verification;
using Xunit;

/// <summary>Checks row-read refusal and trailer geometry against independent native-width oracles.</summary>
public sealed class RowReadAdmissionKernelTests
{
    /// <summary>Readable rows succeed; unreadable rows omit only under explicit lenience.</summary>
    /// <param name="readable">Whether acquisition produced a complete row.</param>
    /// <param name="strict">Whether omission is forbidden.</param>
    /// <param name="expected">Zero for read, one for refusal, two for omission.</param>
    [Theory]
    [InlineData(true, true, 0U)]
    [InlineData(true, false, 0U)]
    [InlineData(false, true, 1U)]
    [InlineData(false, false, 2U)]
    public void Disposition_RequiresExplicitLenience(bool readable, bool strict, uint expected)
        => Assert.Equal(expected, RecoveryKernel.RowReadDisposition(readable, strict));

    /// <summary>The observable omission total cannot wrap or become negative.</summary>
    [Fact]
    public void OmissionCount_SaturatesAtSignedLimit()
    {
        Assert.Equal(1UL, RecoveryKernel.NextOmittedRowCount(0));
        Assert.Equal((ulong)long.MaxValue, RecoveryKernel.NextOmittedRowCount(long.MaxValue - 1));
        Assert.Equal((ulong)long.MaxValue, RecoveryKernel.NextOmittedRowCount(long.MaxValue));
        Assert.Equal((ulong)long.MaxValue, RecoveryKernel.NextOmittedRowCount(ulong.MaxValue));
    }

    /// <summary>Mask and count locations agree with widened signed geometry across native boundaries.</summary>
    [Fact]
    public void HeaderGeometry_MatchesIndependentOracle()
    {
        uint[] sizes = [0, 1, 2, 3, 8, 255, 2048, 4096, int.MaxValue, uint.MaxValue];
        uint[] widths = [0, 1, 2, 3, uint.MaxValue];
        uint[] columns = [0, 1, 7, 8, 9, 255, 65535, 65536, uint.MaxValue];
        foreach (uint size in sizes)
        {
            foreach (uint width in widths)
            {
                foreach (uint count in columns)
                {
                    long mask = size - (((long)count + 7) / 8);
                    bool valid = width is 1 or 2 && count is > 0 and <= 65535 && size <= int.MaxValue && mask >= width;
                    Assert.Equal(valid ? (uint)mask : 0U, RecoveryKernel.RowNullMaskStart(size, width, count));
                }

                foreach (uint countWidth in widths)
                {
                    long position = (long)size - countWidth;
                    bool valid = width is 1 or 2 && countWidth is 1 or 2 && position >= width;
                    Assert.Equal(valid ? (uint)position : 0U, RecoveryKernel.RowVariableCountStart(size, width, countWidth));
                }
            }
        }
    }

    /// <summary>Variable metadata is admitted only when its complete widened size leaves the row header intact.</summary>
    [Fact]
    public void VariableGeometry_MatchesIndependentOracle()
    {
        uint[] sizes = [0, 1, 2, 3, 8, 255, 2048, 4096, int.MaxValue, uint.MaxValue];
        uint[] widths = [0, 1, 2, 3, uint.MaxValue];
        uint[] counts = [0, 1, 255, 65535, 65536, uint.MaxValue];
        uint[] jumpCounts = [0, 1, 15, uint.MaxValue];
        foreach (uint size in sizes)
        {
            foreach (uint header in widths)
            {
                foreach (uint width in widths)
                {
                    foreach (uint end in widths)
                    {
                        foreach (uint count in counts)
                        {
                            foreach (uint jumps in jumpCounts)
                            {
                                ulong metadata = jumps + ((ulong)count * width) + end;
                                bool valid = header is 1 or 2 && width is 1 or 2 && end is 1 or 2 && count <= 65535
                                    && metadata <= size && size - metadata >= header;
                                Assert.Equal(valid ? (uint)(size - metadata) : 0U, RecoveryKernel.RowVariableDataEnd(size, header, count, width, jumps, end));
                            }
                        }
                    }
                }
            }
        }
    }
}
