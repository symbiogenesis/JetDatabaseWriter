namespace JetDatabaseWriter.Encryption;

using System;
using System.Threading;
using JetDatabaseWriter.Exceptions;

/// <summary>Limits cumulative password hashing across a reader and its linked sources.</summary>
internal sealed class EncryptionWorkBudget
{
    private long remaining;

    /// <summary>Initializes a new instance of the <see cref="EncryptionWorkBudget"/> class.</summary>
    /// <param name="maximum">The maximum cumulative password-hash iteration count.</param>
    /// <exception cref="ArgumentOutOfRangeException">The maximum is negative.</exception>
    internal EncryptionWorkBudget(long maximum)
    {
#if NET8_0_OR_GREATER
        ArgumentOutOfRangeException.ThrowIfNegative(maximum);
#else
        if (maximum < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maximum));
        }
#endif

        this.remaining = maximum;
    }

    /// <summary>Reserves the iterations before password derivation begins.</summary>
    /// <param name="spinCount">The validated descriptor's iteration count.</param>
    /// <exception cref="JetLimitationException">The aggregate password work exceeds the budget.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The iteration count is negative.</exception>
    internal void Charge(int spinCount)
    {
#if NET8_0_OR_GREATER
        ArgumentOutOfRangeException.ThrowIfNegative(spinCount);
#else
        if (spinCount < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(spinCount));
        }
#endif

        while (true)
        {
            long available = Interlocked.Read(ref this.remaining);
            if (spinCount > available)
            {
                throw new JetLimitationException(JetErrorCode.ValueTooLarge, "Password hashing exceeds MaxTotalEncryptionSpinCount for this reader and its linked sources.");
            }

            if (Interlocked.CompareExchange(ref this.remaining, available - spinCount, available) == available)
            {
                return;
            }
        }
    }
}
