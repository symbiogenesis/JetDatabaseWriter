namespace JetDatabaseWriter.DelimitedText;

using System;
using System.Collections.Generic;
using System.Globalization;

internal static class DelimitedTextColumnNames
{
    internal static string[] Normalize(IReadOnlyList<string> rawColumnNames)
    {
        var usedNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var nextSuffixByBaseName = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        string[] columnNames = new string[rawColumnNames.Count];
        for (int i = 0; i < rawColumnNames.Count; i++)
        {
            string baseName = string.IsNullOrWhiteSpace(rawColumnNames[i]) ? $"F{i + 1}" : rawColumnNames[i].Trim();

            if (usedNames.Add(baseName))
            {
                columnNames[i] = baseName;
                continue;
            }

            int suffix = nextSuffixByBaseName.TryGetValue(baseName, out int nextSuffix) ? nextSuffix : 2;
            string candidate;
            do
            {
                candidate = baseName + suffix.ToString(CultureInfo.InvariantCulture);
                suffix++;
            }
            while (!usedNames.Add(candidate));

            nextSuffixByBaseName[baseName] = suffix;
            columnNames[i] = candidate;
        }

        return columnNames;
    }

    internal static string[] CreateGenerated(int columnCount)
    {
        string[] columnNames = new string[columnCount];
        for (int i = 0; i < columnNames.Length; i++)
        {
            columnNames[i] = $"F{i + 1}";
        }

        return columnNames;
    }
}
