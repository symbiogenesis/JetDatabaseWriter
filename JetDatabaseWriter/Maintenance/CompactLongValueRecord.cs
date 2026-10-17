namespace JetDatabaseWriter.Maintenance;

using System;

/// <summary>A borrowed raw long-value observation that retains its storage identity.</summary>
internal sealed record CompactLongValueRecord
{
    /// <summary>Gets the observation kind: column, reference, external row or terminal.</summary>
    public string Kind { get; init; } = string.Empty;

    /// <summary>Gets the complete original column slice or external row; valid only until the awaited visitor returns.</summary>
    public ReadOnlyMemory<byte> RawBytes { get; init; }

    /// <summary>Gets the physical page containing an external row or the target of a reference.</summary>
    public long PageNumber { get; init; }

    /// <summary>Gets the encoded external row pointer being observed.</summary>
    public uint? Pointer { get; init; }

    /// <summary>Gets the defined next-row pointer stored in a chained row, including a terminal zero.</summary>
    public uint? NextPointer { get; init; }

    /// <summary>Gets the external row's physical offset on its page.</summary>
    public int RowStart { get; init; }

    /// <summary>Gets the payload's offset within the raw column or row bytes.</summary>
    public int PayloadOffset { get; init; }

    /// <summary>Gets the number of bytes in this slice covered by the descriptor's declared payload.</summary>
    public int DeclaredPayloadBytes { get; init; }

    /// <summary>Gets the bytes after the declared payload in this slice, without asserting they are padding or disposable.</summary>
    public int TrailingBytes { get; init; }

    /// <summary>Gets the explicit observation coverage, where applicable.</summary>
    public string? Coverage { get; init; }

    /// <summary>Gets a value indicating whether incoming references and exclusive ownership have been proven.</summary>
    public bool OwnershipProven { get; }

    /// <summary>Gets a value indicating whether the observed column and external storage must retain their identity.</summary>
    public bool IsPinned { get; } = true;
}
