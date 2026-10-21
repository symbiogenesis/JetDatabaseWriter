namespace JetDatabaseWriter.Schema.Models;

using System;
using System.Collections.Generic;
using JetDatabaseWriter.Indexes.Models;

/// <summary>An immutable physical index descriptor, preserving uninterpreted descriptor bytes.</summary>
internal sealed class TDefIndexDescriptor
{
    internal TDefIndexDescriptor(RealIdxSlot slot, uint rootPage, List<KeyColumn> columns, ReadOnlySpan<byte> bytes)
    {
        this.Slot = slot;
        this.RootPage = rootPage;
        this.Columns = Array.AsReadOnly(columns.ToArray());
        this.RawDescriptor = Array.AsReadOnly(bytes.ToArray());
    }

    internal RealIdxSlot Slot { get; }

    internal uint RootPage { get; }

    internal IReadOnlyList<KeyColumn> Columns { get; }

    internal IReadOnlyList<byte> RawDescriptor { get; }
}
