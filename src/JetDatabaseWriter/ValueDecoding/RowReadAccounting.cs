namespace JetDatabaseWriter.ValueDecoding;

using System.Threading;
using Kernel = JetDatabaseWriter.Verification.RecoveryKernel;

/// <summary>Retains bounded omission accounting shared by one reader and its authorized linked sources.</summary>
internal sealed class RowReadAccounting
{
    private long omittedRowCount;

    /// <summary>Gets the cumulative number of unreadable rows omitted by explicitly lenient reads.</summary>
    internal long OmittedRowCount => Volatile.Read(ref this.omittedRowCount);

    /// <summary>Publishes one omission without wrapping the observable signed count.</summary>
    internal void RecordOmission()
    {
        long observed;
        long next;
        do
        {
            observed = Volatile.Read(ref this.omittedRowCount);
            next = (long)Kernel.NextOmittedRowCount((ulong)observed);
        }
        while (Interlocked.CompareExchange(ref this.omittedRowCount, next, observed) != observed);
    }
}
