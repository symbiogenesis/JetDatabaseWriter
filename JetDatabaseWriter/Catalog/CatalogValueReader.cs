namespace JetDatabaseWriter.Catalog;

using System.Globalization;

internal static class CatalogValueReader
{
    public static string GetStringOrEmpty(string[] row, int index)
        => GetStringOrDefault(row, index, string.Empty);

    public static string GetStringOrDefault(string[] row, int index, string defaultValue)
        => (uint)index < (uint)row.Length ? row[index] : defaultValue;

    public static bool TryParseInt32(string value, out int parsed)
        => int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out parsed);

    public static bool TryParseInt32(string[] row, int index, out int parsed)
        => TryParseInt32(GetStringOrEmpty(row, index), out parsed);

    public static bool TryParseInt64(string value, out long parsed)
        => long.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out parsed);

    public static bool TryParseInt64(string[] row, int index, out long parsed)
        => TryParseInt64(GetStringOrEmpty(row, index), out parsed);

    public static int ParseInt32OrZero(string value)
        => TryParseInt32(value, out int parsed) ? parsed : 0;

    public static int ParseInt32OrZero(string[] row, int index)
        => TryParseInt32(row, index, out int parsed) ? parsed : 0;

    public static long ParseInt64OrZero(string value)
        => TryParseInt64(value, out long parsed) ? parsed : 0L;

    public static long ParseInt64OrZero(string[] row, int index)
        => TryParseInt64(row, index, out long parsed) ? parsed : 0L;

    public static long TdefPageFromId(long id) => id & Constants.SystemObjects.TdefPageMask;

    public static int TdefPageFromId(int id) => id & Constants.SystemObjects.TdefPageMask;
}
