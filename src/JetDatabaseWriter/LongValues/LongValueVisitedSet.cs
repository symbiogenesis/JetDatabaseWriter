namespace JetDatabaseWriter.LongValues;

using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Verification;

/// <summary>A zero-sentinel row-identity set with bounded geometric storage.</summary>
internal struct LongValueVisitedSet
{
    private uint[]? entries;
    private int count;

    /// <summary>Adds a nonzero identity once, refusing impossible array growth.</summary>
    /// <param name="pointer">The nonzero external-row identity.</param>
    /// <returns>Whether the identity was absent.</returns>
    /// <exception cref="JetLimitationException">The bounded tracking capacity is exhausted.</exception>
    internal bool Add(uint pointer)
    {
        if (pointer == 0 || (this.entries is not null && Contains(this.entries, pointer)))
        {
            return false;
        }

        if (this.entries is null || this.count >= this.entries.Length / 2)
        {
            uint capacity = LongValueKernel.NextVisitedCapacity(checked((uint)(this.entries?.Length ?? 0)));
            if (capacity == 0)
            {
                throw new JetLimitationException(JetErrorCode.ValueTooLarge, "The long-value row-identity tracking capacity is exhausted.");
            }

            uint[] next = new uint[checked((int)capacity)];
            if (this.entries is not null)
            {
                foreach (uint previous in this.entries)
                {
                    if (previous != 0)
                    {
                        Insert(next, previous);
                    }
                }
            }

            this.entries = next;
        }

        Insert(this.entries, pointer);
        this.count++;
        return true;
    }

    private static bool Contains(uint[] entries, uint pointer)
    {
        uint capacity = checked((uint)entries.Length);
        uint slot = LongValueKernel.VisitedSlot(pointer, capacity);
        for (int remaining = entries.Length; remaining > 0; remaining--)
        {
            uint candidate = entries[checked((int)slot)];
            if (candidate == 0)
            {
                return false;
            }

            if (candidate == pointer)
            {
                return true;
            }

            slot = LongValueKernel.NextVisitedSlot(slot, capacity);
        }

        return false;
    }

    private static void Insert(uint[] entries, uint pointer)
    {
        uint capacity = checked((uint)entries.Length);
        uint slot = LongValueKernel.VisitedSlot(pointer, capacity);
        for (int remaining = entries.Length; remaining > 0; remaining--)
        {
            if (entries[checked((int)slot)] == 0)
            {
                entries[checked((int)slot)] = pointer;
                return;
            }

            slot = LongValueKernel.NextVisitedSlot(slot, capacity);
        }

        throw new JetOperationException(JetErrorCode.InvalidOperationState, "The long-value row-identity table is unexpectedly full.");
    }
}
