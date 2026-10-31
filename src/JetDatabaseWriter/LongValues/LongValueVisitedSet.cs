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
        ulong decision = LongValueKernel.PrepareVisitedAdd(this.entries, this.count, pointer);
        if (decision == 0)
        {
            return false;
        }

        uint nextCount;
        if (LongValueKernel.VisitedAddHasHighFlag(decision))
        {
            if (LongValueKernel.VisitedAddIsExhausted(decision))
            {
                throw new JetLimitationException(JetErrorCode.ValueTooLarge, "The long-value row-identity tracking capacity is exhausted.");
            }

            // Allocation supplies distinct, zero-filled storage to verified control.
            uint capacity = LongValueKernel.VisitedAddPayload(decision);
            uint[] next = new uint[checked((int)capacity)];
            nextCount = LongValueKernel.CompleteVisitedGrowth(this.entries, next, this.count, pointer);
            this.entries = next;
        }
        else
        {
            nextCount = LongValueKernel.VisitedAddPayload(decision);
        }

        this.count = nextCount;
        return true;
    }
}
