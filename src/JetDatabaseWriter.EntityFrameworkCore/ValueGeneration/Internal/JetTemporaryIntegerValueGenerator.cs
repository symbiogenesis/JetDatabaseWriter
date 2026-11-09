namespace JetDatabaseWriter.EntityFrameworkCore.ValueGeneration.Internal;

using System;
using System.Globalization;
using System.Threading;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.ValueGeneration;

/// <summary>
/// Generates unique temporary integer key values for entities added to a <see cref="Microsoft.EntityFrameworkCore.DbContext"/>
/// before <c>SaveChangesAsync</c> assigns the permanent JET AutoNumber value.
/// </summary>
/// <typeparam name="TValue">The integer key type.</typeparam>
public sealed class JetTemporaryIntegerValueGenerator<TValue> : ValueGenerator<TValue>
    where TValue : struct
{
    private long step;

    /// <inheritdoc/>
    public override bool GeneratesTemporaryValues => true;

    /// <inheritdoc/>
    public override TValue Next(EntityEntry entry)
    {
        ArgumentNullException.ThrowIfNull(entry);

        ulong index = unchecked((ulong)Interlocked.Increment(ref this.step) - 1UL);
        if (typeof(TValue) == typeof(int))
        {
            int intValue = int.MinValue + 1000 + (int)(index % 2_147_482_648UL);
            return (TValue)(object)intValue;
        }

        if (typeof(TValue) == typeof(long))
        {
            long longValue = int.MinValue + 1000L + (long)(index % 2_147_482_648UL);
            return (TValue)(object)longValue;
        }

        if (typeof(TValue) == typeof(short))
        {
            short shortValue = (short)(short.MinValue + 100 + (int)(index % 32_668UL));
            return (TValue)(object)shortValue;
        }

        if (typeof(TValue) == typeof(byte))
        {
            byte byteValue = (byte)((index % 250UL) + 1UL);
            return (TValue)(object)byteValue;
        }

        long fallbackValue = -1L - (long)(index % 30_000UL);
        return (TValue)Convert.ChangeType(fallbackValue, typeof(TValue), CultureInfo.InvariantCulture);
    }
}
