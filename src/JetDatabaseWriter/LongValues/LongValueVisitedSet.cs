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
    /// <exception cref="JetOperationException">The table violates its insertion capacity invariant.</exception>
    internal bool Add(uint pointer)
    {
        if (pointer == 0)
        {
            return false;
        }

        if (this.entries is null || this.count >= this.entries.Length / 2)
        {
            // A duplicate at the growth boundary must not allocate a new table.
            if (this.entries is not null && LongValueKernel.ContainsVisited(this.entries, pointer))
            {
                return false;
            }

            uint capacity = LongValueKernel.NextVisitedCapacity(checked((uint)(this.entries?.Length ?? 0)));
            if (capacity == 0)
            {
                throw new JetLimitationException(JetErrorCode.ValueTooLarge, "The long-value row-identity tracking capacity is exhausted.");
            }

            uint[] next = new uint[checked((int)capacity)];
            if (this.entries is not null)
            {
                LongValueKernel.RehashVisited(this.entries, next);
            }

            this.entries = next;
        }

        byte added = LongValueKernel.AddVisited(this.entries, pointer);
        if (added == 1)
        {
            return false;
        }

        if (added != 2)
        {
            throw new JetOperationException(JetErrorCode.InvalidOperationState, "The long-value row-identity table is unexpectedly full.");
        }

        this.count++;
        return true;
    }
}
