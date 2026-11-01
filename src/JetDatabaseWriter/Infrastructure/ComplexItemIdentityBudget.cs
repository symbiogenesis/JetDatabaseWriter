namespace JetDatabaseWriter.Infrastructure;

using System.Threading;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Verification;

/// <summary>Bounds examined complex-item identity records and their retained scalar inventories.</summary>
/// <param name="limit">The maximum number of examined records.</param>
internal sealed class ComplexItemIdentityBudget(int limit)
{
    private int observed;

    /// <summary>Admits one scalar record before its identity/location is retained.</summary>
    /// <exception cref="JetLimitationException">The named operation allowance is exhausted.</exception>
    internal void Observe()
    {
        int previous;
        do
        {
            previous = Volatile.Read(ref this.observed);
            if (!RecoveryKernel.IsByteRange((ulong)limit, (ulong)previous, 1))
            {
                throw new JetLimitationException(JetErrorCode.ComplexItemIdentityBudgetExceeded, "The complex-item identity inspection budget for this reader operation was exceeded.");
            }
        }
        while (Interlocked.CompareExchange(ref this.observed, previous + 1, previous) != previous);
    }
}
