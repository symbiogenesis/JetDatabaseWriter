namespace JetDatabaseWriter.Schema.Models;

using System;

/// <summary>Nonfatal defects retained by the tolerant TDEF codec for consumer-specific bail decisions.</summary>
[Flags]
internal enum TDefParseIssues
{
    None = 0,
    InvalidRealIndexCount = 1,
    InvalidLogicalIndexCount = 2,
    TruncatedColumnNames = 4,
    TruncatedIndexDescriptors = 8,
    TruncatedIndexNames = 16,
}
