namespace JetDatabaseWriter.Schema.Expressions;

using System;
using System.Globalization;

/// <summary>Retains the native Currency Variant subtype within expression evaluation.</summary>
/// <param name="units">The signed count of ten-thousandths.</param>
internal readonly struct AccessCurrency(long units)
{
    internal decimal Value => units / 10000m;

    internal static AccessCurrency FromDecimal(decimal value)
        => new(checked((long)Math.Round(value * 10000m, MidpointRounding.ToEven)));

    internal static AccessCurrency FromDouble(double value)
        => new(checked((long)Math.Round(AccessVariantOperators.Finite(value * 10000d), MidpointRounding.ToEven)));

    internal double ToDouble() => units / 10000d;

    public override string ToString() => this.Value.ToString("0.####", CultureInfo.InvariantCulture);
}
