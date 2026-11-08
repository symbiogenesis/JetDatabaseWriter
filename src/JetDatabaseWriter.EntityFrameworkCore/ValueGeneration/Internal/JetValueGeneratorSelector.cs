namespace JetDatabaseWriter.EntityFrameworkCore.ValueGeneration.Internal;

using System;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.ValueGeneration;

/// <summary>
/// Selects <see cref="ValueGenerator"/> instances for properties configured with value generation on add.
/// </summary>
/// <param name="dependencies">Parameter object containing dependencies for this service.</param>
public sealed class JetValueGeneratorSelector(ValueGeneratorSelectorDependencies dependencies) : ValueGeneratorSelector(dependencies)
{
    /// <inheritdoc/>
    protected override ValueGenerator? FindForType(IProperty property, ITypeBase typeBase, Type clrType)
    {
        ArgumentNullException.ThrowIfNull(property);
        ArgumentNullException.ThrowIfNull(typeBase);
        ArgumentNullException.ThrowIfNull(clrType);

        Type unwrapped = Nullable.GetUnderlyingType(clrType) ?? clrType;
        if (unwrapped == typeof(int))
        {
            return new JetTemporaryIntegerValueGenerator<int>();
        }

        if (unwrapped == typeof(long))
        {
            return new JetTemporaryIntegerValueGenerator<long>();
        }

        if (unwrapped == typeof(short))
        {
            return new JetTemporaryIntegerValueGenerator<short>();
        }

        if (unwrapped == typeof(byte))
        {
            return new JetTemporaryIntegerValueGenerator<byte>();
        }

        return base.FindForType(property, typeBase, clrType);
    }
}
