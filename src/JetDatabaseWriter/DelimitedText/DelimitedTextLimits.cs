namespace JetDatabaseWriter.DelimitedText;

internal readonly record struct DelimitedTextLimits(
    int MaxRecordLength,
    int MaxFieldLength,
    int MaxColumnCount,
    string MaxRecordLengthOptionName,
    string MaxFieldLengthOptionName,
    string MaxColumnCountOptionName);
