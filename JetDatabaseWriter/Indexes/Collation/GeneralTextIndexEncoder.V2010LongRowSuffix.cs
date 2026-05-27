namespace JetDatabaseWriter.Indexes.Collation;

using System;

internal static partial class GeneralTextIndexEncoder
{
    private const int V2010SuffixAlphabetLength = 65;
    private const int V2010SuffixSpaceIndex = 0;
    private const int V2010SuffixBoundaryPreviousIndex = 253;
    private const int V2010SuffixBoundaryIndex = 254;
    private const int V2010SuffixMinimumTextLength = 255;

    private static ReadOnlySpan<byte> AscendingAuxiliaryRemainder =>
    [
        0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x0E, 0x02, 0x02,
        0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x0E, 0x01, 0x01,
        0x01, 0x81, 0x5F, 0x06, 0x82, 0x81, 0x9B, 0x06, 0x82, 0x00,
    ];

    private static ReadOnlySpan<byte> AscendingRow12Remainder => [0x01, 0x81, 0x5F, 0x06, 0x82, 0x81, 0x9B, 0x06, 0x82, 0x00];

    private static ReadOnlySpan<byte> AscendingRow10Remainder => [0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x0E, 0x01, 0x01, 0x01, 0x80, 0x07, 0x06, 0x82, 0x00];

    private static ReadOnlySpan<byte> AscendingRow11Remainder =>
    [
        0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02,
        0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x0E, 0x01,
        0x01, 0x01, 0x80, 0x13, 0x06, 0x82, 0x00,
    ];

    private static ReadOnlySpan<byte> DescendingAuxiliaryRemainder =>
    [
        0xFD, 0xFD, 0xFD, 0xFD, 0xFD, 0xFD, 0xFD, 0xFD, 0xFD, 0xFD, 0xF1, 0xFD, 0xFD,
        0xFD, 0xFD, 0xFD, 0xFD, 0xFD, 0xFD, 0xFD, 0xFD, 0xFD, 0xFD, 0xF1, 0xFE, 0xFE,
        0xFE, 0x7E, 0xA0, 0xF9, 0x7D, 0x7E, 0x64, 0xF9, 0x7D, 0xFF, 0x00,
    ];

    private static ReadOnlySpan<byte> DescendingRow12Remainder => [0xFE, 0x7E, 0xA0, 0xF9, 0x7D, 0x7E, 0x64, 0xF9, 0x7D, 0xFF, 0x00];

    private static ReadOnlySpan<byte> DescendingRow10Remainder => [0xFD, 0xFD, 0xFD, 0xFD, 0xFD, 0xFD, 0xFD, 0xFD, 0xFD, 0xFD, 0xFD, 0xF1, 0xFE, 0xFE, 0xFE, 0x7F, 0xF8, 0xF9, 0x7D, 0xFF, 0x00];

    private static ReadOnlySpan<byte> DescendingRow11Remainder =>
    [
        0xFD, 0xFD, 0xFD, 0xFD, 0xFD, 0xFD, 0xFD, 0xFD, 0xFD, 0xFD, 0xFD, 0xFD, 0xFD,
        0xFD, 0xFD, 0xFD, 0xFD, 0xFD, 0xFD, 0xFD, 0xFD, 0xFD, 0xFD, 0xFD, 0xF1, 0xFE,
        0xFE, 0xFE, 0x7F, 0xEC, 0xF9, 0x7D, 0xFF, 0x00,
    ];

    private static readonly ushort[] ZeroSuffixContributions = new ushort[V2010SuffixAlphabetLength];

    private static readonly ushort[] AscendingPlainRowContributions =
    [
        0x0000, 0x0000, 0x9F80, 0xA380, 0xE001, 0xBF82, 0x9402, 0xEC02, 0x5802, 0xC003, 0xAF83, 0x9383, 0x8B85,
        0x7C04, 0xE806, 0x1806, 0x3386, 0x9C0A, 0xA00A, 0x7C0B, 0xDF8B, 0xA78B, 0x8008, 0xF808, 0xD388, 0xC408,
        0x1F88, 0x0000, 0x9F80, 0xA380, 0xE001, 0xBF82, 0x9402, 0xEC02, 0x5802, 0xC003, 0xAF83, 0x9383, 0x8B85,
        0x7C04, 0xE806, 0x1806, 0x3386, 0x9C0A, 0xA00A, 0x7C0B, 0xDF8B, 0xA78B, 0x8008, 0xF808, 0xD388, 0xC408,
        0x1F88, 0x0781, 0xDC01, 0xE001, 0xF781, 0x9801, 0x8F81, 0xB381, 0xA401, 0xA802, 0xBF82, 0x7B85, 0x1780,
    ];

    private static readonly ushort[] AscendingPlainColumnContributions =
    [
        0x2824, 0x0000, 0x3980, 0x3380, 0x5000, 0xC980, 0xC600, 0xD200, 0xE400, 0xA000, 0xB180, 0xBB80, 0xBF81,
        0xEA01, 0x2C01, 0x0401, 0x0B81, 0x3A03, 0x3003, 0x6A03, 0x5983, 0x4D83, 0xC003, 0xD403, 0xDB83, 0xDE03,
        0xF983, 0x0000, 0x3980, 0x3380, 0x5000, 0xC980, 0xC600, 0xD200, 0xE400, 0xA000, 0xB180, 0xBB80, 0xBF81,
        0xEA01, 0x2C01, 0x0401, 0x0B81, 0x3A03, 0x3003, 0x6A03, 0x5983, 0x4D83, 0xC003, 0xD403, 0xDB83, 0xDE03,
        0xF983, 0x7D8A, 0x5A0A, 0x500A, 0x558A, 0x440A, 0x418A, 0x4B8A, 0x4E0A, 0xCC0A, 0xC98A, 0x97B7, 0x0594,
    ];

    private static readonly ushort[] AscendingPlainBoundarySpaceSuffixes =
    [
        0x0000, 0x0F81, 0x3601, 0x3C01, 0x5F81, 0xC601, 0xC981, 0xDD81, 0xEB81, 0xAF81, 0xBE01, 0xB401, 0xB000,
        0xE580, 0x2380, 0x0B80, 0x0400, 0x3582, 0x3F82, 0x6582, 0x5602, 0x4202, 0xCF82, 0xDB82, 0xD402, 0xD182,
        0xF602, 0x0F81, 0x3601, 0x3C01, 0x5F81, 0xC601, 0xC981, 0xDD81, 0xEB81, 0xAF81, 0xBE01, 0xB401, 0xB000,
        0xE580, 0x2380, 0x0B80, 0x0400, 0x3582, 0x3F82, 0x6582, 0x5602, 0x4202, 0xCF82, 0xDB82, 0xD402, 0xD182,
        0xF602, 0x7201, 0x5581, 0x5F81, 0x5A01, 0x4B81, 0x4E01, 0x4401, 0x4181, 0xC381, 0xC601, 0x9800, 0x0A01,
    ];

    private static readonly ushort[] AscendingRow10ColumnContributions =
    [
        0x88E4, 0x0000, 0x41CB, 0xC943, 0x5BC4, 0xA907, 0x5A77, 0x4FE6, 0xFD5D, 0xB308, 0xDD61, 0x55E9, 0x7DAD,
        0x5D91, 0x07E6, 0x2844, 0xDB34, 0xCA77, 0x42FF, 0x91B3, 0x58F0, 0x4D61, 0xAA33, 0xBFA2, 0x4CD2, 0x372A,
        0xEBF8, 0x0000, 0x41CB, 0xC943, 0x5BC4, 0xA907, 0x5A77, 0x4FE6, 0xFD5D, 0xB308, 0xDD61, 0x55E9, 0x7DAD,
        0x5D91, 0x07E6, 0x2844, 0xDB34, 0xCA77, 0x42FF, 0x91B3, 0x58F0, 0x4D61, 0xAA33, 0xBFA2, 0x4CD2, 0x372A,
        0xEBF8, 0x0F9E, 0xD34C, 0x5BC4, 0x203C, 0x4E55, 0x35AD, 0xBD25, 0xC6DD, 0xD2FF, 0xA907, 0x520F, 0x7BF8,
    ];

    private static readonly ushort[] AscendingRow11ColumnContributions =
    [
        0xD812, 0x0000, 0xD783, 0x6383, 0xA005, 0x378D, 0xEC0D, 0x840C, 0x880F, 0x400B, 0x478A, 0xF38A, 0xBB98,
        0x741D, 0x9810, 0x4812, 0x9392, 0xD435, 0x6035, 0x7430, 0x17B3, 0x7FB2, 0x803B, 0xE83A, 0x33BA, 0x5C3A,
        0x57B8, 0x0000, 0xD783, 0x6383, 0xA005, 0x378D, 0xEC0D, 0x840C, 0x880F, 0x400B, 0x478A, 0xF38A, 0xBB98,
        0x741D, 0x9810, 0x4812, 0x9392, 0xD435, 0x6035, 0x7430, 0x17B3, 0x7FB2, 0x803B, 0xE83A, 0x33BA, 0x5C3A,
        0x57B8, 0x1F87, 0x1405, 0xA005, 0xCF85, 0xC804, 0xA784, 0x1384, 0x7C04, 0x580D, 0x378D, 0x6B9A, 0x6F80,
    ];

    private static readonly ushort[] DescendingPlainRowContributions =
    [
        0x0000, 0x0000, 0x031C, 0x0320, 0x0560, 0x0CBC, 0x0F14, 0x0F6C, 0x0FD8, 0x0AC0, 0x092C, 0x0910, 0x1D08,
        0x1BFC, 0x14E8, 0x1418, 0x17B0, 0x3C9C, 0x3CA0, 0x39FC, 0x3ADC, 0x3AA4, 0x3300, 0x3378, 0x30D0, 0x3344,
        0x301C, 0x0000, 0x031C, 0x0320, 0x0560, 0x0CBC, 0x0F14, 0x0F6C, 0x0FD8, 0x0AC0, 0x092C, 0x0910, 0x1D08,
        0x1BFC, 0x14E8, 0x1418, 0x17B0, 0x3C9C, 0x3CA0, 0x39FC, 0x3ADC, 0x3AA4, 0x3300, 0x3378, 0x30D0, 0x3344,
        0x301C, 0x0604, 0x055C, 0x0560, 0x06F4, 0x0518, 0x068C, 0x06B0, 0x0524, 0x0F28, 0x0CBC, 0x1DF8, 0x0394,
    ];

    private static readonly ushort[] DescendingPlainColumnContributions =
    [
        0xFC28, 0x0000, 0x03BA, 0x03B0, 0x0050, 0x034A, 0x00C6, 0x00D2, 0x00E4, 0x00A0, 0x0332, 0x0338, 0x06BC,
        0x056A, 0x05AC, 0x0584, 0x0608, 0x0A3A, 0x0A30, 0x0A6A, 0x09DA, 0x09CE, 0x0AC0, 0x0AD4, 0x0958, 0x0ADE,
        0x097A, 0x0000, 0x03BA, 0x03B0, 0x0050, 0x034A, 0x00C6, 0x00D2, 0x00E4, 0x00A0, 0x0332, 0x0338, 0x06BC,
        0x056A, 0x05AC, 0x0584, 0x0608, 0x0A3A, 0x0A30, 0x0A6A, 0x09DA, 0x09CE, 0x0AC0, 0x0AD4, 0x0958, 0x0ADE,
        0x097A, 0x3FFE, 0x3C5A, 0x3C50, 0x3FD6, 0x3C44, 0x3FC2, 0x3FC8, 0x3C4E, 0x3CCC, 0x3F4A, 0xB294, 0x7B86,
    ];

    private static readonly ushort[] DescendingPlainBoundarySpaceSuffixes =
    [
        0xFF00, 0x0B73, 0x08C9, 0x08C3, 0x0B23, 0x0839, 0x0BB5, 0x0BA1, 0x0B97, 0x0BD3, 0x0841, 0x084B, 0x0DCF,
        0x0E19, 0x0EDF, 0x0EF7, 0x0D7B, 0x0149, 0x0143, 0x0119, 0x02A9, 0x02BD, 0x01B3, 0x01A7, 0x022B, 0x01AD,
        0x0209, 0x0B73, 0x08C9, 0x08C3, 0x0B23, 0x0839, 0x0BB5, 0x0BA1, 0x0B97, 0x0BD3, 0x0841, 0x084B, 0x0DCF,
        0x0E19, 0x0EDF, 0x0EF7, 0x0D7B, 0x0149, 0x0143, 0x0119, 0x02A9, 0x02BD, 0x01B3, 0x01A7, 0x022B, 0x01AD,
        0x0209, 0x088D, 0x0B29, 0x0B23, 0x08A5, 0x0B37, 0x08B1, 0x08BB, 0x0B3D, 0x0BBF, 0x0839, 0x0DE7, 0x08F5,
    ];

    private static readonly ushort[] DescendingRow10ColumnContributions =
    [
        0x8052, 0x0000, 0xB9C3, 0x8948, 0x9BD9, 0x1129, 0x325B, 0x57CD, 0xCD7C, 0x3333, 0x455C, 0x75D7, 0xEDFE,
        0x65DE, 0x5785, 0x9829, 0xBB5B, 0x32CB, 0x0240, 0xA912, 0x205A, 0x45CC, 0xAAAA, 0xCF3C, 0xEC4E, 0xFFB7,
        0x1369, 0x0000, 0xB9C3, 0x8948, 0x9BD9, 0x1129, 0x325B, 0x57CD, 0xCD7C, 0x3333, 0x455C, 0x75D7, 0xEDFE,
        0x65DE, 0x5785, 0x9829, 0xBB5B, 0x32CB, 0x0240, 0xA912, 0x205A, 0x45CC, 0xAAAA, 0xCF3C, 0xEC4E, 0xFFB7,
        0x1369, 0x478C, 0xAB52, 0x9BD9, 0x8820, 0xFE4F, 0xEDB6, 0xDD3D, 0xCEC4, 0x02D0, 0x1129, 0x2252, 0x13F9,
    ];

    private static readonly ushort[] DescendingRow11ColumnContributions =
    [
        0x90D8, 0x0000, 0x0954, 0x09E0, 0x1EA0, 0x2E34, 0x2D6C, 0x2884, 0x2288, 0x39C0, 0x3FC4, 0x3F70, 0x5338,
        0x4E74, 0x6318, 0x6C48, 0x6F10, 0xBED4, 0xBE60, 0xA074, 0xA994, 0xAC7C, 0x9900, 0x9CE8, 0x9FB0, 0x9C5C,
        0x9054, 0x0000, 0x0954, 0x09E0, 0x1EA0, 0x2E34, 0x2D6C, 0x2884, 0x2288, 0x39C0, 0x3FC4, 0x3F70, 0x5338,
        0x4E74, 0x6318, 0x6C48, 0x6F10, 0xBED4, 0xBE60, 0xA074, 0xA994, 0xAC7C, 0x9900, 0x9CE8, 0x9FB0, 0x9C5C,
        0x9054, 0x121C, 0x1E14, 0x1EA0, 0x1D4C, 0x1B48, 0x18A4, 0x1810, 0x1BFC, 0x2DD8, 0x2E34, 0x5C68, 0x03EC,
    ];

    private static readonly V2010LongRowSuffixTable AscendingPlainSuffixTable = new(
        0x27A5,
        AscendingPlainRowContributions,
        AscendingPlainColumnContributions,
        AscendingPlainBoundarySpaceSuffixes,
        tripleSpaceSuffix: null,
        hasBoundarySpaceForPreviousSpace: false);

    private static readonly V2010LongRowSuffixTable AscendingAuxiliarySuffixTable = new(
        0x338E,
        ZeroSuffixContributions,
        CreateSpaceOnlyTable(0xDF52),
        CreateSpaceAndOtherTable(0x3404, 0xECDC),
        tripleSpaceSuffix: null,
        hasBoundarySpaceForPreviousSpace: true);

    private static readonly V2010LongRowSuffixTable AscendingRow12SuffixTable = new(
        0x1DAC,
        ZeroSuffixContributions,
        CreateSpaceOnlyTable(0x7466),
        CreateSpaceAndOtherTable(0x7F53, 0x69CA),
        tripleSpaceSuffix: 0x1D58,
        hasBoundarySpaceForPreviousSpace: true);

    private static readonly V2010LongRowSuffixTable AscendingRow10SuffixTable = new(
        0x9E8D,
        ZeroSuffixContributions,
        AscendingRow10ColumnContributions,
        CreateSpaceAndOtherTable(0x164D, 0x1669),
        tripleSpaceSuffix: 0x173E,
        hasBoundarySpaceForPreviousSpace: true);

    private static readonly V2010LongRowSuffixTable AscendingRow11SuffixTable = new(
        0xE5A7,
        ZeroSuffixContributions,
        AscendingRow11ColumnContributions,
        CreateSpaceAndOtherTable(0x1A3B, 0x3DB5),
        tripleSpaceSuffix: 0x9593,
        hasBoundarySpaceForPreviousSpace: true);

    private static readonly V2010LongRowSuffixTable DescendingPlainSuffixTable = new(
        0xF75B,
        DescendingPlainRowContributions,
        DescendingPlainColumnContributions,
        DescendingPlainBoundarySpaceSuffixes,
        tripleSpaceSuffix: null,
        hasBoundarySpaceForPreviousSpace: true);

    private static readonly V2010LongRowSuffixTable DescendingAuxiliarySuffixTable = new(
        0x2A1F,
        ZeroSuffixContributions,
        CreateSpaceOnlyTable(0xE0D6),
        CreateSpaceAndOtherTable(0x1AEF, 0xCAC9),
        tripleSpaceSuffix: null,
        hasBoundarySpaceForPreviousSpace: true);

    private static readonly V2010LongRowSuffixTable DescendingRow12SuffixTable = new(
        0xC1A1,
        ZeroSuffixContributions,
        CreateSpaceOnlyTable(0x7315),
        CreateSpaceAndOtherTable(0xEBF1, 0xB2B4),
        tripleSpaceSuffix: 0xD2EF,
        hasBoundarySpaceForPreviousSpace: true);

    private static readonly V2010LongRowSuffixTable DescendingRow10SuffixTable = new(
        0xDF12,
        ZeroSuffixContributions,
        DescendingRow10ColumnContributions,
        CreateSpaceAndOtherTable(0xA364, 0x5F40),
        tripleSpaceSuffix: 0x79E8,
        hasBoundarySpaceForPreviousSpace: true);

    private static readonly V2010LongRowSuffixTable DescendingRow11SuffixTable = new(
        0x0819,
        ZeroSuffixContributions,
        DescendingRow11ColumnContributions,
        CreateSpaceAndOtherTable(0x9565, 0x98C1),
        tripleSpaceSuffix: 0xC303,
        hasBoundarySpaceForPreviousSpace: true);

    private static ushort? TryComputeV2010LongRowSuffix(string text, bool ascending, byte[] fullEntry)
    {
        if (text.Length < V2010SuffixMinimumTextLength
            || fullEntry.Length <= GeneralLegacyTextIndexEncoder.MaxEntryLengthGeneralV2010)
        {
            return null;
        }

        char previousBoundaryChar = text[V2010SuffixBoundaryPreviousIndex];
        char boundaryChar = text[V2010SuffixBoundaryIndex];
        int previousBoundaryIndex = GetV2010SuffixAlphabetIndex(previousBoundaryChar);
        int boundaryIndex = GetV2010SuffixAlphabetIndex(boundaryChar);
        if (previousBoundaryIndex < 0 || boundaryIndex < 0)
        {
            return null;
        }

        var table = TryGetV2010LongRowSuffixTable(
            text,
            fullEntry,
            ascending,
            previousBoundaryChar,
            boundaryChar);
        if (table is null)
        {
            return null;
        }

        return TryLookupV2010LongRowSuffix(
            table,
            text[V2010SuffixBoundaryPreviousIndex - 1],
            previousBoundaryIndex,
            boundaryIndex);
    }

    private static V2010LongRowSuffixTable? TryGetV2010LongRowSuffixTable(
        string text,
        byte[] fullEntry,
        bool ascending,
        char previousBoundaryChar,
        char boundaryChar)
    {
        var remainderTable = ascending
            ? TryGetAscendingRemainderTable(fullEntry)
            : TryGetDescendingRemainderTable(fullEntry);
        if (remainderTable is not null)
        {
            return remainderTable;
        }

        return IsPlainV2010DaoContext(text, previousBoundaryChar, boundaryChar)
            ? ascending ? AscendingPlainSuffixTable : DescendingPlainSuffixTable
            : null;
    }

    private static V2010LongRowSuffixTable? TryGetAscendingRemainderTable(byte[] fullEntry)
    {
        if (MatchesRemainder(fullEntry, AscendingAuxiliaryRemainder))
        {
            return AscendingAuxiliarySuffixTable;
        }

        if (MatchesRemainder(fullEntry, AscendingRow10Remainder))
        {
            return AscendingRow10SuffixTable;
        }

        if (MatchesRemainder(fullEntry, AscendingRow11Remainder))
        {
            return AscendingRow11SuffixTable;
        }

        return MatchesRemainder(fullEntry, AscendingRow12Remainder)
            ? AscendingRow12SuffixTable
            : null;
    }

    private static V2010LongRowSuffixTable? TryGetDescendingRemainderTable(byte[] fullEntry)
    {
        if (MatchesRemainder(fullEntry, DescendingAuxiliaryRemainder))
        {
            return DescendingAuxiliarySuffixTable;
        }

        if (MatchesRemainder(fullEntry, DescendingRow10Remainder))
        {
            return DescendingRow10SuffixTable;
        }

        if (MatchesRemainder(fullEntry, DescendingRow11Remainder))
        {
            return DescendingRow11SuffixTable;
        }

        return MatchesRemainder(fullEntry, DescendingRow12Remainder)
            ? DescendingRow12SuffixTable
            : null;
    }

    private static ushort? TryLookupV2010LongRowSuffix(
        V2010LongRowSuffixTable table,
        char precedingBoundaryChar,
        int previousBoundaryIndex,
        int boundaryIndex)
    {
        if (boundaryIndex == V2010SuffixSpaceIndex)
        {
            if (previousBoundaryIndex == V2010SuffixSpaceIndex)
            {
                if (precedingBoundaryChar == ' ')
                {
                    return table.TripleSpaceSuffix;
                }

                if (!table.HasBoundarySpaceForPreviousSpace)
                {
                    return null;
                }
            }

            return table.BoundarySpaceSuffixes[previousBoundaryIndex];
        }

        return (ushort)(table.BaseSuffix
            ^ table.RowContributions[previousBoundaryIndex]
            ^ table.ColumnContributions[boundaryIndex]);
    }

    private static bool MatchesRemainder(byte[] fullEntry, ReadOnlySpan<byte> remainder)
    {
        const int remainderStart = GeneralLegacyTextIndexEncoder.MaxEntryLengthGeneralV2010;
        return fullEntry.Length == remainderStart + remainder.Length
            && fullEntry.AsSpan(remainderStart, remainder.Length).SequenceEqual(remainder);
    }

    private static bool IsPlainV2010DaoContext(
        string text,
        char previousBoundaryChar,
        char boundaryChar)
    {
        for (int index = 0; index < text.Length; index++)
        {
            if (index == V2010SuffixBoundaryPreviousIndex || index == V2010SuffixBoundaryIndex)
            {
                continue;
            }

            if (index == V2010SuffixBoundaryPreviousIndex - 1
                && previousBoundaryChar == ' '
                && boundaryChar == ' '
                && text[index] != ' '
                && GetV2010SuffixAlphabetIndex(text[index]) >= 0)
            {
                continue;
            }

            if (text[index] != 'a')
            {
                return false;
            }
        }

        return true;
    }

    private static int GetV2010SuffixAlphabetIndex(char value)
        => value switch
        {
            ' ' => 0,
            >= 'a' and <= 'z' => value - 'a' + 1,
            >= 'A' and <= 'Z' => value - 'A' + 27,
            >= '0' and <= '9' => value - '0' + 53,
            '_' => 63,
            '+' => 64,
            _ => -1,
        };

    private static ushort[] CreateSpaceOnlyTable(ushort spaceValue) => CreateSpaceAndOtherTable(spaceValue, 0);

    private static ushort[] CreateSpaceAndOtherTable(ushort spaceValue, ushort otherValue)
    {
        ushort[] values = new ushort[V2010SuffixAlphabetLength];
        Array.Fill(values, otherValue);
        values[V2010SuffixSpaceIndex] = spaceValue;

        return values;
    }

    private sealed class V2010LongRowSuffixTable(
        ushort baseSuffix,
        ushort[] rowContributions,
        ushort[] columnContributions,
        ushort[] boundarySpaceSuffixes,
        ushort? tripleSpaceSuffix,
        bool hasBoundarySpaceForPreviousSpace)
    {
        public ushort BaseSuffix { get; } = baseSuffix;

        public ushort[] RowContributions { get; } = rowContributions;

        public ushort[] ColumnContributions { get; } = columnContributions;

        public ushort[] BoundarySpaceSuffixes { get; } = boundarySpaceSuffixes;

        public ushort? TripleSpaceSuffix { get; } = tripleSpaceSuffix;

        public bool HasBoundarySpaceForPreviousSpace { get; } = hasBoundarySpaceForPreviousSpace;
    }
}
