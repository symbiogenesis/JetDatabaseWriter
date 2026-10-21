namespace JetDatabaseWriter.ValueDecoding.Models;

internal readonly record struct LongValueRef(int Start, int Len, bool IsOle);
