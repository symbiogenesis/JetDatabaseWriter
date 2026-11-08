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
    private long current = int.MinValue + 1000;

    /// <inheritdoc/>
    public override bool GeneratesTemporaryValues => true;

    /// <inheritdoc/>
    public override TValue Next(EntityEntry entry)
    {
        ArgumentNullException.ThrowIfNull(entry);

        long nextValue = Interlocked.Increment(ref this.current);
        if (typeof(TValue) == typeof(int))
        {
            return (TValue)(object)unchecked((int)nextValue);
        }

        if (typeof(TValue) == typeof(long))
        {
            return (TValue)(object)nextValue;
        }

        if (typeof(TValue) == typeof(short))
        {
            short shortValue = unchecked((short)((nextValue % 30000) - 30000));
            return (TValue)(object)shortValue;
        }

        if (typeof(TValue) == typeof(byte))
        {
            byte byteValue = unchecked((byte)((Math.Abs(nextValue) % 250) + 1));
            return (TValue)(object)byteValue;
        }

        return (TValue)Convert.ChangeType(nextValue, typeof(TValue), CultureInfo.InvariantCulture);
    }
}
