namespace JetDatabaseWriter.Models;

using System;
using System.Globalization;

/// <summary>A calculation failure or unavailable cached result, distinct from database Null.</summary>
public sealed class CalculatedError : IEquatable<CalculatedError>
{
    internal CalculatedError(int? nativeCode, int envelopeLength = 39)
    {
        this.NativeCode = nativeCode;
        this.EnvelopeLength = envelopeLength;
    }

    /// <summary>Gets the signed cached native status, or null when the row has no cached result.</summary>
    public int? NativeCode { get; }

    internal static CalculatedError Uncached { get; } = new(null, envelopeLength: 0);

    internal int EnvelopeLength { get; }

    /// <inheritdoc/>
    public bool Equals(CalculatedError? other) => other is not null && this.NativeCode == other.NativeCode;

    /// <inheritdoc/>
    public override bool Equals(object? obj) => obj is CalculatedError other && this.Equals(other);

    /// <inheritdoc/>
    public override int GetHashCode() => this.NativeCode.GetHashCode();

    /// <summary>Returns an explicit error marker containing the native status.</summary>
    /// <returns>The error marker.</returns>
    public override string ToString() => this.NativeCode is int code ? "#Error(" + code.ToString(CultureInfo.InvariantCulture) + ")" : "#Error(Uncached)";
}
