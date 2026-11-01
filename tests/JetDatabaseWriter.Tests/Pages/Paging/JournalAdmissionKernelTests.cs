namespace JetDatabaseWriter.Tests.Pages.Paging;

using System;
using JetDatabaseWriter.Verification;
using Xunit;

/// <summary>Checks journal admission against independent signed-record and length-history oracles.</summary>
public sealed class JournalAdmissionKernelTests
{
    private const ulong MaximumLength = 2147483648;

    /// <summary>Every record class uses only its relevant evidence and preserves state when it refuses.</summary>
    /// <param name="pageSize">The physical page size.</param>
    [Theory]
    [InlineData(2048U)]
    [InlineData(4096U)]
    public void Advance_RecordClassesAndEvidence_MatchIndependentOracle(uint pageSize)
    {
        ulong originalLength = (pageSize * 3UL) + 17;
        ulong maximum = pageSize * 5UL;
        AdmissionState[] states =
        [
            new(originalLength, originalLength, false, false),
            new(pageSize, maximum, false, false),
            new(pageSize, maximum, true, false),
            new(originalLength, maximum, false, false),
        ];
        ulong[] currentLengths = [0, pageSize, pageSize + 1UL, originalLength, maximum, maximum + 1, MaximumLength];
        ulong[] truncateLengths = [0, pageSize, pageSize + 1UL, originalLength, maximum, maximum + 1, MaximumLength, ulong.MaxValue];
        long[] tags =
        [
            long.MinValue, long.MinValue + pageSize, long.MinValue + (pageSize * 3L),
            long.MinValue + (pageSize * 4L), long.MinValue + 1, -4, -3, -2, -1,
            0, 1, pageSize, pageSize * 4L, 2147483648L - pageSize, 2147483648L, long.MaxValue,
        ];
        foreach (AdmissionState state in states)
        {
            foreach (ulong currentLength in currentLengths)
            {
                foreach (long tag in tags)
                {
                    foreach (ulong truncateLength in truncateLengths)
                    {
                        for (int flags = 0; flags < 32; flags++)
                        {
                            var input = new AdmissionInput(tag, truncateLength, (flags & 1) != 0, (flags & 2) != 0, (flags & 4) != 0, (flags & 8) != 0);
                            AdmissionState initial = state with { Authorized = (flags & 16) != 0 };
                            Assert.Equal(ExpectedAdvance(pageSize, originalLength, currentLength, initial, input), Advance(pageSize, originalLength, currentLength, initial, input));
                        }
                    }
                }
            }
        }
    }

    /// <summary>Malformed native-width inputs cannot borrow authority from any record or valid payload.</summary>
    [Fact]
    public void Advance_InvalidState_RefusesWithoutChangingOutputs()
    {
        const ulong originalLength = 8192;
        var initial = new AdmissionState(originalLength, originalLength, false, true);
        (uint PageSize, ulong Original, ulong Current, AdmissionState State)[] cases =
        [
            (0, originalLength, originalLength, initial),
            (1, originalLength, originalLength, initial),
            (uint.MaxValue, originalLength, originalLength, initial),
            (2048, MaximumLength + 1, originalLength, initial),
            (2048, ulong.MaxValue, originalLength, initial),
            (2048, originalLength, MaximumLength + 1, initial),
            (2048, originalLength, ulong.MaxValue, initial),
            (2048, originalLength, originalLength, initial with { Minimum = originalLength + 1 }),
            (2048, originalLength, originalLength, initial with { Maximum = originalLength - 1 }),
            (2048, originalLength, originalLength, initial with { Maximum = MaximumLength + 1 }),
            (2048, originalLength, originalLength, initial with { Maximum = ulong.MaxValue }),
            (2048, originalLength, originalLength, initial with { Recovering = true }),
        ];
        foreach ((uint pageSize, ulong original, ulong current, AdmissionState state) in cases)
        {
            foreach (long tag in new long[] { -1, -3, -2, long.MinValue, 0 })
            {
                var input = new AdmissionInput(tag, 0, true, false, true, true);
                Assert.Equal(((byte)0, state), Advance(pageSize, original, current, state, input));
            }
        }
    }

    /// <summary>A final decision bypasses historical length admission only after every input is well formed.</summary>
    [Fact]
    public void Complete_BoundaryStates_MatchIndependentOracle()
    {
        ulong[] lengths = [0, 1, 2048, 4097, 8192, MaximumLength, MaximumLength + 1, ulong.MaxValue];
        foreach (ulong originalLength in lengths)
        {
            foreach (ulong currentLength in lengths)
            {
                foreach (ulong minimumLength in lengths)
                {
                    foreach (ulong maximumLength in lengths)
                    {
                        for (int flags = 0; flags < 8; flags++)
                        {
                            bool recovering = (flags & 1) != 0;
                            bool authorized = (flags & 2) != 0;
                            bool committed = (flags & 4) != 0;
                            bool valid = originalLength <= MaximumLength && currentLength <= MaximumLength && maximumLength <= MaximumLength
                                && minimumLength <= originalLength && originalLength <= maximumLength && (!recovering || minimumLength < originalLength);
                            bool expected = valid && (committed || (minimumLength <= currentLength && currentLength <= maximumLength && (authorized || recovering)));
                            Assert.Equal(expected, RecoveryKernel.CompleteJournalAdmission(originalLength, currentLength, minimumLength, maximumLength, recovering, authorized, committed));
                        }
                    }
                }
            }
        }
    }

    /// <summary>Later truncation and regrowth retain the earliest removed tail and exact length authorization.</summary>
    /// <param name="pageSize">The physical page size.</param>
    [Theory]
    [InlineData(2048U)]
    [InlineData(4096U)]
    public void Advance_TruncateRegrowAndReplay_RetainsHistory(uint pageSize)
    {
        ulong originalLength = (pageSize * 3UL) + 17;
        ulong currentLength = pageSize + 1UL;
        var initial = new AdmissionState(originalLength, originalLength, false, false);
        AdmissionState state = initial;
        for (long offset = pageSize; (ulong)offset < originalLength; offset += pageSize)
        {
            (byte kind, state) = Advance(pageSize, originalLength, currentLength, state, new AdmissionInput(long.MinValue + offset, 0, false, false, false, true));
            Assert.Equal(4, kind);
            Assert.Equal(initial, state);
        }

        (byte truncate, state) = Advance(pageSize, originalLength, currentLength, state, new AdmissionInput(-2, pageSize, false, false, true, false));
        Assert.Equal(3, truncate);
        Assert.Equal(new AdmissionState(pageSize, originalLength, false, false), state);
        Assert.False(RecoveryKernel.CompleteJournalAdmission(originalLength, currentLength, state.Minimum, state.Maximum, state.Recovering, state.Authorized, false));

        (byte write, state) = Advance(pageSize, originalLength, currentLength, state, new AdmissionInput(pageSize, 0, false, true, false, false));
        Assert.Equal(5, write);
        Assert.Equal(new AdmissionState(pageSize, originalLength, false, true), state);
        Assert.True(RecoveryKernel.CompleteJournalAdmission(originalLength, currentLength, state.Minimum, state.Maximum, state.Recovering, state.Authorized, false));

        (byte laterTruncate, state) = Advance(pageSize, originalLength, currentLength, state, new AdmissionInput(-2, pageSize * 2UL, false, false, true, false));
        Assert.Equal(3, laterTruncate);
        Assert.Equal(new AdmissionState(pageSize, originalLength, false, true), state);
        (byte extension, state) = Advance(pageSize, originalLength, currentLength, state, new AdmissionInput(pageSize * 4L, 0, false, false, false, false));
        Assert.Equal(5, extension);
        Assert.Equal(new AdmissionState(pageSize, pageSize * 5UL, false, true), state);
        (byte extendedTruncate, state) = Advance(pageSize, originalLength, currentLength, state, new AdmissionInput(-2, pageSize * 5UL, false, false, true, false));
        Assert.Equal(3, extendedTruncate);
        Assert.Equal(new AdmissionState(pageSize, pageSize * 5UL, false, true), state);
        (byte replay, state) = Advance(pageSize, originalLength, currentLength, state, new AdmissionInput(-3, 0, false, false, false, false));
        Assert.Equal(2, replay);
        Assert.Equal(new AdmissionState(pageSize, pageSize * 5UL, true, true), state);

        foreach (AdmissionInput input in new AdmissionInput[]
        {
            new(pageSize, 0, true, true, true, true),
            new(-2, 0, true, true, true, true),
            new(long.MinValue, 0, true, false, true, true),
            new(-1, 0, true, false, false, false),
        })
        {
            Assert.Equal(((byte)0, state), Advance(pageSize, originalLength, currentLength, state, input));
        }

        Assert.Equal(((byte)2, state), Advance(pageSize, originalLength, currentLength, state, new AdmissionInput(-3, 0, false, false, false, false)));
        Assert.Equal(((byte)1, state), Advance(pageSize, originalLength, currentLength, state, new AdmissionInput(-1, 0, true, false, false, true)));
    }

    private static (byte Kind, AdmissionState State) Advance(uint pageSize, ulong originalLength, ulong currentLength, AdmissionState state, AdmissionInput input)
    {
        RecoveryKernel.AdvanceJournalAdmission(
            pageSize,
            originalLength,
            currentLength,
            state.Minimum,
            state.Maximum,
            state.Recovering,
            state.Authorized,
            unchecked((ulong)input.Tag),
            input.TruncateLength,
            input.Final,
            input.CapturePresent,
            input.TailCovered,
            input.PayloadValid,
            out byte kind,
            out ulong minimum,
            out ulong maximum,
            out bool recovering,
            out bool authorized);
        return (kind, new AdmissionState(minimum, maximum, recovering, authorized));
    }

    private static (byte Kind, AdmissionState State) ExpectedAdvance(uint pageSize, ulong originalLength, ulong currentLength, AdmissionState state, AdmissionInput input)
    {
        if (input.Tag == -1)
        {
            return (input.Final && input.PayloadValid ? (byte)1 : (byte)0, state);
        }

        if (input.Tag == -3)
        {
            return state.Minimum < originalLength ? ((byte)2, state with { Recovering = true }) : ((byte)0, state);
        }

        if (state.Recovering)
        {
            return (0, state);
        }

        if (input.Tag == -2)
        {
            return input.TruncateLength <= state.Maximum && input.TruncateLength % pageSize == 0 && input.TailCovered
                ? ((byte)3, state with { Minimum = Math.Min(state.Minimum, input.TruncateLength), Authorized = state.Authorized || currentLength == input.TruncateLength })
                : ((byte)0, state);
        }

        if (input.Tag < 0)
        {
            ulong offset = unchecked((ulong)(input.Tag - long.MinValue));
            return offset < originalLength && offset % pageSize == 0 && !input.CapturePresent && input.PayloadValid ? ((byte)4, state) : ((byte)0, state);
        }

        ulong writeOffset = (ulong)input.Tag;
        if (writeOffset >= MaximumLength || writeOffset % pageSize != 0 || MaximumLength - writeOffset < pageSize || (writeOffset < originalLength && !input.CapturePresent))
        {
            return (0, state);
        }

        return (5, state with { Maximum = Math.Max(state.Maximum, writeOffset + pageSize), Authorized = state.Authorized || (writeOffset < currentLength && currentLength <= writeOffset + pageSize) });
    }

    private readonly record struct AdmissionState(ulong Minimum, ulong Maximum, bool Recovering, bool Authorized);

    private readonly record struct AdmissionInput(long Tag, ulong TruncateLength, bool Final, bool CapturePresent, bool TailCovered, bool PayloadValid);
}
