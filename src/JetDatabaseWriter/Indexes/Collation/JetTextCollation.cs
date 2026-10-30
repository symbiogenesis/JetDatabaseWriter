namespace JetDatabaseWriter.Indexes.Collation;

using System;
using JetDatabaseWriter.Exceptions;

/// <summary>Compares expression text with the database's Access sort order.</summary>
/// <param name="sortOrder">The database sort order.</param>
internal sealed class JetTextCollation(TextSortOrder sortOrder)
{
    /// <summary>Gets the default collation used by standalone expression tests.</summary>
    internal static JetTextCollation GeneralLegacy { get; } = new(new TextSortOrder(0x0409, 0, true));

    /// <summary>Compares expression text with native trailing-space equivalence.</summary>
    /// <param name="left">The left value.</param>
    /// <param name="right">The right value.</param>
    /// <returns>The relative order.</returns>
    internal int Compare(string left, string right)
        => IndexPageCodec.CompareKeyBytes(this.EncodeComparisonKey(left, trimTrailingSpaces: true), this.EncodeComparisonKey(right, trimTrailingSpaces: true));

    /// <summary>Checks whether appending Text can still complete the requested primary-weight stream.</summary>
    /// <param name="prefix">The complete comparison key of the candidate prefix.</param>
    /// <param name="target">The requested complete comparison key.</param>
    /// <returns>Whether the candidate can still match after adding code units.</returns>
    internal bool CanExtendComparisonKey(ReadOnlySpan<byte> prefix, ReadOnlySpan<byte> target)
    {
        if (sortOrder.Value != 0 && !sortOrder.HasVersion)
        {
            return true;
        }

        int prefixEnd = prefix.IndexOf(GeneralLegacyTextIndexEncoder.EndText);
        int targetEnd = target.IndexOf(GeneralLegacyTextIndexEncoder.EndText);
        if (prefixEnd < 1 || targetEnd < 1)
        {
            return true;
        }

        ReadOnlySpan<byte> primaryPrefix = prefix[1..prefixEnd];
        ReadOnlySpan<byte> primaryTarget = target[1..targetEnd];
        return primaryPrefix.Length <= primaryTarget.Length && primaryTarget.StartsWith(primaryPrefix);
    }

    /// <summary>Encodes a complete comparison key without stored-index limits.</summary>
    /// <param name="text">The text.</param>
    /// <param name="trimTrailingSpaces">Whether to omit trailing spaces from the comparison key.</param>
    /// <exception cref="JetNotSupportedException">The database sort order is unsupported.</exception>
    internal byte[] EncodeComparisonKey(string text, bool trimTrailingSpaces = false)
    {
        if (!sortOrder.IsSupported)
        {
            throw new JetNotSupportedException(JetErrorCode.UnsupportedTextCollation, $"Text sort order 0x{sortOrder.Value:X4}, version {sortOrder.Version}, is not supported.", new JetErrorInfo { Reason = "Unsupported text sort order" });
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
