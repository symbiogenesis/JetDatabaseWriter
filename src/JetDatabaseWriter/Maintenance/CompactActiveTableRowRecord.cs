namespace JetDatabaseWriter.Maintenance;

using System;
using JetDatabaseWriter.Pages.Models;

/// <summary>Preserves unassigned rows and reports successful scoped membership without releasing original storage.</summary>
internal sealed record CompactActiveTableRowRecord
{
    /// <summary>Gets unassigned-row or active-membership-terminal.</summary>
    public string Kind { get; init; } = string.Empty;

    /// <summary>Gets the original physical table owner.</summary>
    public long OwnerTDefPage { get; init; }

    /// <summary>Gets an unassigned original header and resolved payload location.</summary>
    public RowLocation? Location { get; init; }

    /// <summary>Gets the borrowed complete row bytes, valid only until the awaited observer completes.</summary>
    public ReadOnlyMemory<byte> RawBytes { get; init; }

    /// <summary>Gets successful scoped counts on the terminal record.</summary>
    public CompactActiveTableResult? Result { get; init; }

    /// <summary>Gets a value indicating whether original storage remains pinned; always true.</summary>
    public bool IsPinned { get; } = true;
}
