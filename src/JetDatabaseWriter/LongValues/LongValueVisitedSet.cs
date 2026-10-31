namespace JetDatabaseWriter.LongValues;

using System.Runtime.CompilerServices;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Verification;

/// <summary>A zero-sentinel row-identity set with bounded geometric storage.</summary>
internal struct LongValueVisitedSet
{
    private uint[]? entries;
    private uint count;

    /// <summary>Adds a nonzero identity once, refusing impossible array growth.</summary>
    /// <param name="pointer">The nonzero external-row identity.</param>
    /// <returns>Whether the identity was absent.</returns>
    /// <exception cref="JetLimitationException">The bounded tracking capacity is exhausted.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal bool Add(uint pointer)
    {
        uint[] current = this.entries ?? [];
        LongValueKernel.PrepareVisitedAdd(current, this.count, pointer, out byte result, out uint nextCount, out uint capacity);
        if (result == 0)
        {
            return false;
        }

        if (result == 2)
        {
            // Allocation supplies distinct, zero-filled storage to verified control.
            uint[] next = new uint[checked((int)capacity)];
            nextCount = LongValueKernel.CompleteVisitedGrowth(current, next, this.count, pointer);
            this.entries = next;
        }
        else if (result == 3)
        {
            throw new JetLimitationException(JetErrorCode.ValueTooLarge, "The long-value row-identity tracking capacity is exhausted.");
        }

        this.count = nextCount;
        return true;
    }
}
