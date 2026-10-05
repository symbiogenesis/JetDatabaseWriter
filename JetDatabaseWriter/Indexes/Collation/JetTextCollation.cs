namespace JetDatabaseWriter.Indexes.Collation;

using System;

/// <summary>Compares expression text with the database's Access sort order.</summary>
/// <param name="sortOrder">The database sort order.</param>
internal sealed class JetTextCollation(TextSortOrder sortOrder)
{
    /// <summary>Gets the default collation used by standalone expression tests.</summary>
    internal static JetTextCollation GeneralLegacy { get; } = new(new TextSortOrder(0x0409, 0, true));

    /// <summary>Compares complete text without trimming spaces.</summary>
    /// <param name="left">The left value.</param>
    /// <param name="right">The right value.</param>
    /// <returns>The relative order.</returns>
    internal int Compare(string left, string right)
        => IndexPageCodec.CompareKeyBytes(this.EncodeComparisonKey(left), this.EncodeComparisonKey(right));

    /// <summary>Encodes a complete comparison key without stored-index limits.</summary>
    /// <param name="text">The text.</param>
    /// <param name="trimTrailingSpaces">Whether to trim trailing spaces for joins.</param>
    /// <exception cref="NotSupportedException">The database sort order is unsupported.</exception>
    internal byte[] EncodeComparisonKey(string text, bool trimTrailingSpaces = false)
    {
        if (!sortOrder.IsSupported)
        {
            throw new NotSupportedException($"Text sort order 0x{sortOrder.Value:X4}, version {sortOrder.Version}, is not supported.");
        }

        if (sortOrder.Value == 0 || (sortOrder.HasVersion && sortOrder.Version == 0))
        {
            return GeneralLegacyTextIndexEncoder.EncodeComparisonKey(text, trimTrailingSpaces);
        }

        return !sortOrder.HasVersion
            ? General97TextIndexEncoder.EncodeComparisonKey(text, trimTrailingSpaces)
            : GeneralTextIndexEncoder.EncodeComparisonKey(text, trimTrailingSpaces);
    }
}
