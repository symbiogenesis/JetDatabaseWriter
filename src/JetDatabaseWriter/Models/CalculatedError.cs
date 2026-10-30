namespace JetDatabaseWriter.Models;

using System;
using System.Globalization;

/// <summary>A native cached calculation failure, distinct from database Null.</summary>
public sealed class CalculatedError : IEquatable<CalculatedError>
{
    internal CalculatedError(int nativeCode, int envelopeLength = 39)
    {
        this.NativeCode = nativeCode;
        this.EnvelopeLength = envelopeLength;
    }

    /// <summary>Gets the signed native calculation status stored in the cached value.</summary>
    public int NativeCode { get; }

    internal int EnvelopeLength { get; }

    /// <inheritdoc/>
    public bool Equals(CalculatedError? other) => other is not null && this.NativeCode == other.NativeCode;

    /// <inheritdoc/>
    public override bool Equals(object? obj) => obj is CalculatedError other && this.Equals(other);

    /// <inheritdoc/>
    public override int GetHashCode() => this.NativeCode;

    /// <summary>Returns an explicit error marker containing the native status.</summary>
    /// <returns>The error marker.</returns>
    public override string ToString() => "#Error(" + this.NativeCode.ToString(CultureInfo.InvariantCulture) + ")";
}
