namespace JetDatabaseWriter.Schema.Models;

using System;
using System.Collections.Generic;
using System.Linq;
using JetDatabaseWriter.Indexes.Models;

/// <summary>An owned immutable projection of one logical table definition, including uninterpreted bytes.</summary>
internal sealed class TDefImage
{
    private readonly byte[] bytes;

    internal TDefImage(byte[] bytes, TDefHeader header, List<ColumnInfo> columns, bool hasDeletedColumns, TDefParseIssues issues, IndexSectionAnchors? indexSection, List<TDefIndexDescriptor> realIndexes, List<TDefLogicalIndexDescriptor> logicalIndexes)
    {
        this.bytes = bytes.AsSpan().ToArray();
        this.Header = header;
        this.Columns = Array.AsReadOnly(columns.ToArray());
        this.HasDeletedColumns = hasDeletedColumns;
        this.Issues = issues;
        this.IndexSection = indexSection;
        this.RealIndexes = Array.AsReadOnly(realIndexes.ToArray());
        this.LogicalIndexes = Array.AsReadOnly(logicalIndexes.ToArray());
    }

    internal TDefHeader Header { get; }

    internal IReadOnlyList<ColumnInfo> Columns { get; }

    internal bool HasDeletedColumns { get; }

    internal TDefParseIssues Issues { get; }

    internal IndexSectionAnchors? IndexSection { get; }

    internal IReadOnlyList<TDefIndexDescriptor> RealIndexes { get; }

    internal IReadOnlyList<TDefLogicalIndexDescriptor> LogicalIndexes { get; }

    /// <summary>Returns an independent copy, preserving unknown fields for later model edits.</summary>
    internal byte[] CopyBytes() => this.bytes.AsSpan().ToArray();
}
