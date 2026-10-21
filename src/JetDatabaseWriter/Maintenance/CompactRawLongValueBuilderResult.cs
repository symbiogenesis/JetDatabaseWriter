namespace JetDatabaseWriter.Maintenance;

using System;

/// <summary>One complete private raw LVAL allocation and its retargeted original column slice.</summary>
/// <param name="Column">The entire original column, with only its defined external first pointer replaced.</param>
/// <param name="FirstPageNumber">The destination EOF before staging; inline or null external values append nothing.</param>
/// <param name="PageCount">The appended LVAL-page count.</param>
/// <param name="RowCount">The copied external-row count, including rows beyond the declared payload.</param>
/// <param name="RawBytes">The complete column and external-row bytes, including trailing storage.</param>
internal sealed record CompactRawLongValueBuilderResult(ReadOnlyMemory<byte> Column, long FirstPageNumber, long PageCount, long RowCount, long RawBytes)
{
    /// <summary>Gets a value indicating whether the destination database has been validated; always false.</summary>
    public bool IsValidated { get; }

    /// <summary>Gets a value indicating whether incoming references and exclusive ownership have been proven; always false.</summary>
    public bool OwnershipProven { get; }
}
