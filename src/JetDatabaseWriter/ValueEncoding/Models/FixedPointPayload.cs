namespace JetDatabaseWriter.ValueEncoding.Models;

/// <summary>
/// Metadata produced while shaping a decimal into a fixed-point NUMERIC
/// payload magnitude.
/// </summary>
internal readonly struct FixedPointPayload
{
    /// <summary>
    /// Initializes a new instance of the <see cref="FixedPointPayload"/> struct.
    /// </summary>
    /// <param name="negative">Whether the original value was negative.</param>
    /// <param name="naturalScale">The decimal scale before target-scale rescaling.</param>
    /// <param name="digitCount">The base-10 digit count after target-scale rescaling.</param>
    /// <param name="magnitudeByteCount">The unsigned little-endian byte count after target-scale rescaling.</param>
    internal FixedPointPayload(bool negative, int naturalScale, int digitCount, int magnitudeByteCount)
    {
        this.Negative = negative;
        this.NaturalScale = naturalScale;
        this.DigitCount = digitCount;
        this.MagnitudeByteCount = magnitudeByteCount;
    }

    /// <summary>
    /// Gets a value indicating whether the original value was negative.
    /// </summary>
    internal bool Negative { get; }

    /// <summary>
    /// Gets the decimal scale before target-scale rescaling.
    /// </summary>
    internal int NaturalScale { get; }

    /// <summary>
    /// Gets the base-10 digit count after target-scale rescaling.
    /// </summary>
    internal int DigitCount { get; }

    /// <summary>
    /// Gets the unsigned little-endian byte count after target-scale rescaling.
    /// </summary>
    internal int MagnitudeByteCount { get; }
}
