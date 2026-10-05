namespace JetDatabaseWriter.Schema.Models;

using System;
using System.Collections.Generic;
using JetDatabaseWriter.Indexes.Models;

/// <summary>A logical index or relationship entry and its original uninterpreted bytes.</summary>
internal sealed class TDefLogicalIndexDescriptor
{
    internal TDefLogicalIndexDescriptor(LogicalIdxEntry entry, string name, ReadOnlySpan<byte> bytes)
    {
        this.Entry = entry;
        this.Name = name;
        this.RawDescriptor = Array.AsReadOnly(bytes.ToArray());
    }

    internal LogicalIdxEntry Entry { get; }

    internal string Name { get; }

    internal IReadOnlyList<byte> RawDescriptor { get; }
}
