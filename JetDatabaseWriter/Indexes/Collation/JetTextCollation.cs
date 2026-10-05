namespace JetDatabaseWriter.Indexes.Collation;

using System;

/// <summary>Compares expression text with the database's Access sort order.</summary>
/// <param name="sortOrder">The database sort order.</param>
internal sealed class JetTextCollation(TextSortOrder sortOrder)
{
    /// <summary>Gets the default collation used by standalone expression tests.</summary>
    internal static JetTextCollation GeneralLegacy { get; } = new(new TextSortOrder(0x0409, 0, true));

    /// <summary>Compares text in bounded sort-key windows without trimming spaces.</summary>
    /// <param name="left">The left value.</param>
    /// <param name="right">The right value.</param>
    /// <returns>The relative order.</returns>
    internal int Compare(string left, string right)
    {
        const int window = Constants.IndexTextEncoding.MaxTextIndexCharLength;
        int offset = 0;
        while (offset < left.Length || offset < right.Length)
        {
            string leftWindow = offset < left.Length ? left.Substring(offset, Math.Min(window, left.Length - offset)) : string.Empty;
            string rightWindow = offset < right.Length ? right.Substring(offset, Math.Min(window, right.Length - offset)) : string.Empty;
            byte[] leftKey = this.Encode(leftWindow);
            byte[] rightKey = this.Encode(rightWindow);
            int comparison = IndexPageCodec.CompareKeyBytes(leftKey, rightKey);
            if (comparison != 0)
            {
                return comparison;
            }

            offset += window;
        }

        return 0;
    }

    private byte[] Encode(string text)
    {
        if (!sortOrder.IsSupported)
        {
            throw new NotSupportedException($"Text sort order 0x{sortOrder.Value:X4}, version {sortOrder.Version}, is not supported.");
        }

        if (sortOrder.Value == 0 || (sortOrder.HasVersion && sortOrder.Version == 0))
        {
            return GeneralLegacyTextIndexEncoder.Encode(text, true, trimTrailingSpaces: false);
        }

        return !sortOrder.HasVersion
            ? General97TextIndexEncoder.Encode(text, true, trimTrailingSpaces: false)
            : GeneralTextIndexEncoder.EncodeComparisonWindow(text);
    }
}
