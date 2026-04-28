namespace JetDatabaseWriter.Internal.Helpers;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using JetDatabaseWriter.Core;
using JetDatabaseWriter.Internal;
using JetDatabaseWriter.Internal.Builders;
using JetDatabaseWriter.Internal.Models;
using JetDatabaseWriter.Models;

/// <summary>
/// State-free index-related helpers extracted from <see cref="AccessWriter"/>.
/// Holds the schema-validation, FK composite-key encoding, and B-tree
/// split / descent helpers that depend only on their inputs (no
/// <see cref="AccessBase"/> instance state).
/// </summary>
internal static class IndexHelpers
{
    // Mirror of AccessBase column-type discriminators used by the schema
    // validators here. Kept private to avoid expanding the visibility of
    // the AccessBase constants for one consumer.
    private const byte T_OLE = 0x0B;
    private const byte T_ATTACHMENT = 0x11;
    private const byte T_COMPLEX = 0x12;

    /// <summary>
    /// Returns <paramref name="baseName"/> if no entry in <paramref name="existing"/>
    /// already uses it (case-insensitive); otherwise appends "_1", "_2", … until
    /// an unused name is found.
    /// </summary>
    public static string MakeUniqueLogicalIdxName(string baseName, IReadOnlyList<string> existing)
    {
        var taken = new HashSet<string>(existing, StringComparer.OrdinalIgnoreCase);
        if (!taken.Contains(baseName))
        {
            return baseName;
        }

        for (int i = 1; i < int.MaxValue; i++)
        {
            string candidate = baseName + "_" + i.ToString(CultureInfo.InvariantCulture);
            if (!taken.Contains(candidate))
            {
                return candidate;
            }
        }

        return baseName;
    }

    /// <summary>
    /// Returns <see langword="true"/> when the 10-slot col_map at the
    /// real-idx physical descriptor located at offset <paramref name="phys"/>
    /// in <paramref name="td"/> exactly matches <paramref name="columnNumbers"/>
    /// (used slots agree, unused slots are 0xFFFF). Returns <see langword="false"/>
    /// when the descriptor would extend past <paramref name="td"/>'s end.
    /// </summary>
    public static bool RealIdxColMapMatches(byte[] td, int phys, int[] columnNumbers)
    {
        if (phys + 52 > td.Length)
        {
            return false;
        }

        for (int slot = 0; slot < 10; slot++)
        {
            int so = phys + 4 + (slot * 3);
            int cn = BinaryPrimitives.ReadUInt16LittleEndian(td.AsSpan(so, 2));
            if (slot < columnNumbers.Length)
            {
                if (cn != columnNumbers[slot])
                {
                    return false;
                }
            }
            else if (cn != 0xFFFF)
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// synthesizes a primary-key <see cref="IndexDefinition"/> from any
    /// <see cref="ColumnDefinition.IsPrimaryKey"/> flags set on the supplied
    /// columns and forces those columns to <see cref="ColumnDefinition.IsNullable"/>
    /// = <c>false</c> on the returned column list (the JET TDEF flag bit
    /// <c>FLAG_NULL_ALLOWED 0x02</c> is cleared, matching Access semantics
    /// for PK columns). Mixing the column-level shortcut with an explicit
    /// PK <see cref="IndexDefinition"/> in the same call throws
    /// <see cref="ArgumentException"/>.
    /// </summary>
    public static (IReadOnlyList<ColumnDefinition> Columns, IReadOnlyList<IndexDefinition> Indexes) ApplyPrimaryKeyShortcut(
        IReadOnlyList<ColumnDefinition> columns,
        IReadOnlyList<IndexDefinition> indexes)
    {
        bool anyColumnPk = false;
        foreach (ColumnDefinition c in columns)
        {
            if (c.IsPrimaryKey)
            {
                anyColumnPk = true;
                break;
            }
        }

        bool anyIndexPk = false;
        foreach (IndexDefinition idx in indexes)
        {
            if (idx.IsPrimaryKey)
            {
                anyIndexPk = true;
                break;
            }
        }

        if (anyColumnPk && anyIndexPk)
        {
            throw new ArgumentException(
                "Primary key declared both via ColumnDefinition.IsPrimaryKey and an explicit IndexDefinition.IsPrimaryKey. Use one or the other.");
        }

        // Force PK key columns (whether declared via column flag OR an explicit
        // PK IndexDefinition) to non-nullable on the emitted TDEF.
        HashSet<string>? pkColumnNames = null;
        if (anyColumnPk)
        {
            pkColumnNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var pkColList = new List<string>();
            foreach (ColumnDefinition c in columns)
            {
                if (c.IsPrimaryKey)
                {
                    pkColumnNames.Add(c.Name);
                    pkColList.Add(c.Name);
                }
            }

            var newIndexes = new List<IndexDefinition>(indexes.Count + 1);
            newIndexes.AddRange(indexes);
            newIndexes.Add(new IndexDefinition("PrimaryKey", pkColList) { IsPrimaryKey = true });
            indexes = newIndexes;
        }
        else if (anyIndexPk)
        {
            pkColumnNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (IndexDefinition idx in indexes)
            {
                if (idx.IsPrimaryKey)
                {
                    foreach (string col in idx.Columns)
                    {
                        pkColumnNames.Add(col);
                    }
                }
            }
        }

        if (pkColumnNames is not null)
        {
            var newCols = new ColumnDefinition[columns.Count];
            for (int i = 0; i < columns.Count; i++)
            {
                ColumnDefinition c = columns[i];
                if (pkColumnNames.Contains(c.Name) && c.IsNullable)
                {
                    c = c with { IsNullable = false };
                }

                newCols[i] = c;
            }

            columns = newCols;
        }

        return (columns, indexes);
    }

    /// <summary>
    /// Validates the user-declared <paramref name="indexes"/> against
    /// <paramref name="tableDef"/> and returns the resolved per-index
    /// column-number / direction tuples consumed by the TDEF emitter.
    /// </summary>
    public static List<ResolvedIndex> ResolveIndexes(IReadOnlyList<IndexDefinition> indexes, TableDef tableDef)
    {
        if (indexes.Count == 0)
        {
            return [];
        }

        var result = new List<ResolvedIndex>(indexes.Count);
        var seenNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        bool sawPk = false;
        for (int i = 0; i < indexes.Count; i++)
        {
            IndexDefinition def = indexes[i];
            if (string.IsNullOrEmpty(def.Name))
            {
                throw new ArgumentException($"IndexDefinition at position {i} has an empty name.", nameof(indexes));
            }

            if (!seenNames.Add(def.Name))
            {
                throw new ArgumentException($"Duplicate index name '{def.Name}'.", nameof(indexes));
            }

            if (def.Columns.Count == 0)
            {
                throw new ArgumentException($"IndexDefinition '{def.Name}' must reference at least one column.", nameof(indexes));
            }

            // The JET col_map carries up to 10 columns per index (§3.1).
            if (def.Columns.Count > 10)
            {
                throw new NotSupportedException($"IndexDefinition '{def.Name}' has {def.Columns.Count} columns; the JET col_map supports at most 10.");
            }

            // Multi-column non-PK indexes are accepted; the maintenance loop
            // encodes per-column and concatenates to form the composite key.
            if (def.IsPrimaryKey)
            {
                if (sawPk)
                {
                    throw new ArgumentException("Only one primary-key index is permitted per table.", nameof(indexes));
                }

                sawPk = true;
            }

            var seenCols = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var colNums = new int[def.Columns.Count];
            for (int k = 0; k < def.Columns.Count; k++)
            {
                string columnName = def.Columns[k];
                if (!seenCols.Add(columnName))
                {
                    throw new ArgumentException($"IndexDefinition '{def.Name}' references column '{columnName}' more than once.", nameof(indexes));
                }

                ColumnInfo column = tableDef.FindColumn(columnName)
                    ?? throw new ArgumentException($"IndexDefinition '{def.Name}' references unknown column '{columnName}'.", nameof(indexes));

                // match Microsoft Access — neither the UI
                // nor the engine permits CREATE INDEX over OLE Object,
                // Attachment, or Multi-Value (Complex) columns. Previously
                // these silently fell through to a "schema-only" path that
                // emitted a stale empty leaf and let Access rebuild on
                // Compact & Repair; that fallback masked a programmer error
                // and disagreed with Access semantics. Reject with a clear
                // diagnostic so callers correct the index definition.
                if (column.Type is T_OLE or T_ATTACHMENT or T_COMPLEX)
                {
                    string typeName = column.Type switch
                    {
                        T_OLE => "OLE Object",
                        T_ATTACHMENT => "Attachment",
                        T_COMPLEX => "Multi-Value (Complex)",
                        _ => $"0x{column.Type:X2}",
                    };
                    throw new NotSupportedException(
                        $"IndexDefinition '{def.Name}' references column '{columnName}' whose type is {typeName}; "
                        + "Microsoft Access does not permit indexes on this column type.");
                }

                colNums[k] = column.ColNum;
            }

            // per-column ascending direction. DescendingColumns is a
            // case-insensitive subset of Columns; any entry that does not
            // appear in Columns is rejected.
            bool[] ascending = new bool[def.Columns.Count];
            for (int k = 0; k < ascending.Length; k++)
            {
                ascending[k] = true;
            }

            if (def.DescendingColumns is { Count: > 0 } descendingList)
            {
                foreach (string descName in descendingList)
                {
                    if (string.IsNullOrEmpty(descName))
                    {
                        throw new ArgumentException($"IndexDefinition '{def.Name}' has an empty entry in DescendingColumns.", nameof(indexes));
                    }

                    int matchIndex = -1;
                    for (int k = 0; k < def.Columns.Count; k++)
                    {
                        if (string.Equals(def.Columns[k], descName, StringComparison.OrdinalIgnoreCase))
                        {
                            matchIndex = k;
                            break;
                        }
                    }

                    if (matchIndex < 0)
                    {
                        throw new ArgumentException(
                            $"IndexDefinition '{def.Name}' lists '{descName}' in DescendingColumns but the column is not in Columns.",
                            nameof(indexes));
                    }

                    ascending[matchIndex] = false;
                }
            }

            // PKs are implicitly unique (signalled by index_type=0x01, not the
            // flag bit per §3.1). User-set IsUnique on a PK is silently subsumed.
            bool isUnique = def.IsPrimaryKey || def.IsUnique;

            result.Add(new ResolvedIndex(def.Name, colNums, ascending, def.IsPrimaryKey, isUnique));
        }

        return result;
    }

    /// <summary>
    /// Default <c>RewriteTableAsync</c> index projection: forwards every
    /// surviving Normal / PrimaryKey index whose key columns are still present
    /// (case-insensitive name match) in the rebuilt schema. FK indexes are
    /// not forwarded today.
    /// </summary>
    public static List<IndexDefinition> DefaultIndexProjection(IReadOnlyList<IndexMetadata> existing, IReadOnlyList<ColumnDefinition> newDefs)
    {
        var result = new List<IndexDefinition>(existing.Count);
        var newColumnNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (ColumnDefinition c in newDefs)
        {
            newColumnNames.Add(c.Name);
        }

        foreach (IndexMetadata idx in existing)
        {
            if (idx.Kind != IndexKind.Normal && idx.Kind != IndexKind.PrimaryKey)
            {
                continue;
            }

            if (idx.Columns.Count == 0)
            {
                continue;
            }

            bool allSurvive = true;
            foreach (IndexColumnReference ic in idx.Columns)
            {
                if (string.IsNullOrEmpty(ic.Name) || !newColumnNames.Contains(ic.Name))
                {
                    allSurvive = false;
                    break;
                }
            }

            if (!allSurvive)
            {
                continue;
            }

            var pkCols = new string[idx.Columns.Count];
            var descendingCols = new List<string>();
            for (int i = 0; i < idx.Columns.Count; i++)
            {
                pkCols[i] = idx.Columns[i].Name;
                if (!idx.Columns[i].IsAscending)
                {
                    descendingCols.Add(idx.Columns[i].Name);
                }
            }

            if (idx.Kind == IndexKind.PrimaryKey)
            {
                result.Add(new IndexDefinition(idx.Name, pkCols)
                {
                    IsPrimaryKey = true,
                    DescendingColumns = descendingCols,
                });
            }
            else
            {
                result.Add(new IndexDefinition(idx.Name, pkCols)
                {
                    IsUnique = idx.IsUnique,
                    DescendingColumns = descendingCols,
                });
            }
        }

        return result;
    }

    /// <summary>
    /// Builds a canonical, type-tolerant string key from <paramref name="row"/>
    /// for the columns listed in <paramref name="columnIndexes"/>. Returns
    /// <see langword="null"/> when any component is <see cref="DBNull"/> /
    /// <see langword="null"/> — Access treats a partial-null FK tuple as
    /// unconstrained (the row is allowed even if no parent matches).
    /// </summary>
    public static string? BuildCompositeKey(object?[] row, int[] columnIndexes)
    {
        var sb = new StringBuilder();
        for (int i = 0; i < columnIndexes.Length; i++)
        {
            int idx = columnIndexes[i];
            if (idx < 0 || idx >= row.Length)
            {
                return null;
            }

            object? v = row[idx];
            if (v == null || v is DBNull)
            {
                return null;
            }

            sb.Append('|');
            AppendNormalized(sb, v);
        }

        return sb.ToString();
    }

    private static void AppendNormalized(StringBuilder sb, object value)
    {
        switch (value)
        {
            case string s:
                // Access string equality is case-insensitive in JET — match that.
                sb.Append('S').Append(':').Append(s.ToUpperInvariant());
                break;
            case Guid g:
                sb.Append('G').Append(':').Append(g.ToString("N"));
                break;
            case byte[] b:
                sb.Append('B').Append(':').Append(Convert.ToBase64String(b));
                break;
            case DateTime dt:
                sb.Append('D').Append(':').Append(dt.ToUniversalTime().Ticks.ToString(CultureInfo.InvariantCulture));
                break;
            case bool bl:
                sb.Append('?').Append(':').Append(bl ? '1' : '0');
                break;
            case IConvertible c:
                // Numeric-ish: normalize through decimal for cross-width equality
                // (e.g. user passes int 5 against a long parent column).
                try
                {
                    decimal d = c.ToDecimal(CultureInfo.InvariantCulture);
                    sb.Append('N').Append(':').Append(d.ToString(CultureInfo.InvariantCulture));
                }
                catch (Exception ex) when (ex is FormatException || ex is InvalidCastException || ex is OverflowException)
                {
                    sb.Append('X').Append(':').Append(value.ToString() ?? string.Empty);
                }

                break;
            default:
                sb.Append('X').Append(':').Append(value.ToString() ?? string.Empty);
                break;
        }
    }

    /// <summary>
    /// Encodes the composite seek key for a single FK-side row using the
    /// parent-side column types and the per-column ascending flags captured
    /// at index resolution. Returns <see langword="null"/> when any column
    /// is null (Access permits partial-null FK tuples — caller already
    /// short-circuited on this path) or when the encoder rejects any value.
    /// </summary>
    public static byte[]? TryEncodeSeekKey(ParentSeekIndex idx, object[] values)
    {
        var pieces = new byte[idx.KeyColumns.Count][];
        int total = 0;
        try
        {
            for (int i = 0; i < idx.KeyColumns.Count; i++)
            {
                ParentSeekKeyColumn col = idx.KeyColumns[i];
                if (col.ForeignColumnIndex < 0 || col.ForeignColumnIndex >= values.Length)
                {
                    return null;
                }

                object? v = values[col.ForeignColumnIndex];
                if (v is DBNull)
                {
                    v = null;
                }

                if (v == null)
                {
                    // BuildCompositeKey already rejected partial-null tuples,
                    // but be defensive — encoding a null key entry still
                    // produces a well-formed flag-only block.
                    pieces[i] = IndexKeyEncoder.EncodeEntry(col.ColumnType, null, col.Ascending);
                }
                else
                {
                    pieces[i] = IndexKeyEncoder.EncodeEntry(col.ColumnType, v, col.Ascending);
                }

                total += pieces[i].Length;
            }
        }
        catch (Exception ex) when (ex is NotSupportedException || ex is ArgumentException || ex is OverflowException)
        {
            return null;
        }

        byte[] composite = new byte[total];
        int offset = 0;
        for (int i = 0; i < pieces.Length; i++)
        {
            Buffer.BlockCopy(pieces[i], 0, composite, offset, pieces[i].Length);
            offset += pieces[i].Length;
        }

        return composite;
    }

    /// <summary>
    /// Encodes a composite seek key for the child (FK-side) index using the
    /// supplied parent-PK column values (in relationship-PK declaration
    /// order). Returns <see langword="null"/> when any value is null
    /// (cascade callers already short-circuit on partial-null parent keys)
    /// or when the encoder rejects any value.
    /// </summary>
    public static byte[]? TryEncodeChildSeekKey(ChildSeekIndex idx, object?[] parentPkValues)
    {
        if (parentPkValues.Length != idx.KeyColumns.Count)
        {
            return null;
        }

        var pieces = new byte[idx.KeyColumns.Count][];
        int total = 0;
        try
        {
            for (int i = 0; i < idx.KeyColumns.Count; i++)
            {
                ChildSeekKeyColumn col = idx.KeyColumns[i];
                object? v = parentPkValues[i];
                if (v is DBNull)
                {
                    v = null;
                }

                if (v == null)
                {
                    return null;
                }

                pieces[i] = IndexKeyEncoder.EncodeEntry(col.ColumnType, v, col.Ascending);
                total += pieces[i].Length;
            }
        }
        catch (Exception ex) when (ex is NotSupportedException || ex is ArgumentException || ex is OverflowException)
        {
            return null;
        }

        byte[] composite = new byte[total];
        int offset = 0;
        for (int i = 0; i < pieces.Length; i++)
        {
            Buffer.BlockCopy(pieces[i], 0, composite, offset, pieces[i].Length);
            offset += pieces[i].Length;
        }

        return composite;
    }

    /// <summary>
    /// Lexicographic byte-array compare matching the JET index-key sort order
    /// (every byte in <see cref="IndexKeyEncoder"/>'s output is already
    /// inverted on descending columns, so unsigned compare is correct).
    /// </summary>
    public static int CompareKeyBytes(byte[] a, byte[] b)
    {
        int n = Math.Min(a.Length, b.Length);
        for (int i = 0; i < n; i++)
        {
            int diff = a[i] - b[i];
            if (diff != 0)
            {
                return diff;
            }
        }

        return a.Length - b.Length;
    }

    /// <summary>
    /// Returns the index of the first intermediate entry whose summary key
    /// sorts &gt;= <paramref name="searchKey"/>, or <c>-1</c> when every
    /// summary is &lt; searchKey (the descent would have to follow
    /// <c>tail_page</c>, which the surgical path rejects).
    /// </summary>
    public static int SelectChildIndexFromDecoded(
        List<IndexLeafIncremental.DecodedIntermediateEntry> entries,
        byte[] searchKey)
    {
        for (int i = 0; i < entries.Count; i++)
        {
            if (CompareKeyBytes(searchKey, entries[i].Key) <= 0)
            {
                return i;
            }
        }

        return -1;
    }

    /// <summary>
    /// greedy left-fill N-way split of a spliced leaf
    /// entry list. Walks the input once, packing entries into a fresh page
    /// until the next entry would overflow, then opens a new page and
    /// continues. Returns <see langword="null"/> only when a single entry
    /// is too large to fit on any page (encoded key + 4-byte slot offset
    /// exceeds the per-page payload area) or when the input would not
    /// actually require a split. The returned list always has
    /// <c>Count &gt;= 2</c> on success; every page is non-empty.
    /// </summary>
    public static List<List<IndexLeafPageBuilder.LeafEntry>>? TryGreedySplitLeafInN(
        IndexLeafPageBuilder.LeafPageLayout layout,
        int pageSize,
        List<IndexLeafPageBuilder.LeafEntry> entries)
    {
        int payloadArea = pageSize - layout.FirstEntryOffset;
        if (entries.Count < 2)
        {
            return null;
        }

        var pages = new List<List<IndexLeafPageBuilder.LeafEntry>>();
        var current = new List<IndexLeafPageBuilder.LeafEntry>();
        int currentSize = 0;

        for (int i = 0; i < entries.Count; i++)
        {
            int len = entries[i].EncodedKey.Length + 4;
            if (len > payloadArea)
            {
                // A single entry's encoded key + slot offset exceeds an
                // entire page's payload area — no split arrangement helps.
                return null;
            }

            if (currentSize + len > payloadArea && current.Count > 0)
            {
                pages.Add(current);
                current = new List<IndexLeafPageBuilder.LeafEntry>();
                currentSize = 0;
            }

            current.Add(entries[i]);
            currentSize += len;
        }

        if (current.Count > 0)
        {
            pages.Add(current);
        }

        if (pages.Count < 2)
        {
            // Whole thing fits on one page — caller should have used the
            // in-place rewrite instead of asking for a split. Bail
            // so the call site falls through cleanly.
            return null;
        }

        return pages;
    }

    /// <summary>
    /// greedy left-fill N-way split of an
    /// INTERMEDIATE page's entry list. Each candidate page is validated by
    /// <see cref="IndexBTreeBuilder.TryBuildIntermediatePage"/> so the
    /// per-page byte budget — including the §4.4 prefix-compression savings
    /// the simpler leaf splitter cannot model — is respected exactly.
    /// </summary>
    public static List<List<(byte[] Key, long DataPage, byte DataRow, long ChildPage)>>? TryGreedySplitIntermediateInN(
        IndexLeafPageBuilder.LeafPageLayout layout,
        int pageSize,
        long parentTdefPage,
        List<(byte[] Key, long DataPage, byte DataRow, long ChildPage)> entries)
    {
        if (entries.Count < 2)
        {
            return null;
        }

        var pages = new List<List<(byte[], long, byte, long)>>();
        int i = 0;
        while (i < entries.Count)
        {
            // Grow [i, end) until either (a) end == entries.Count and the
            // remainder fits, or (b) the next extension overflows. Linear
            // probe is fine — TryBuildIntermediatePage is O(slice size) and
            // the total work across all probes is O(N²) where N is the
            // intermediate's entry count (typically ≤ a few hundred).
            int end = i + 1;
            byte[]? lastFit = IndexBTreeBuilder.TryBuildIntermediatePage(
                layout, pageSize, parentTdefPage, entries.GetRange(i, 1), prevPage: 0, nextPage: 0, tailPage: 0);
            if (lastFit is null)
            {
                // A single entry won't fit — degenerate, bail.
                return null;
            }

            while (end < entries.Count)
            {
                var probe = entries.GetRange(i, end - i + 1);
                byte[]? probeBytes = IndexBTreeBuilder.TryBuildIntermediatePage(
                    layout, pageSize, parentTdefPage, probe, prevPage: 0, nextPage: 0, tailPage: 0);
                if (probeBytes is null)
                {
                    break;
                }

                end++;
            }

            pages.Add(entries.GetRange(i, end - i));
            i = end;
        }

        if (pages.Count < 2)
        {
            return null;
        }

        return pages;
    }

    /// <summary>
    /// Re-walks a captured <paramref name="path"/> with a different search
    /// key, returning <see langword="true"/> only when every level picks the
    /// same child entry that was originally followed. Used to confirm that
    /// every change in the batch lands on the same leaf as the first one
    /// (surgical mutation requires this; multi-leaf change-sets bail to
    /// the bulk path).
    /// </summary>
    public static bool ConfirmKeyTargetsSamePath(List<DescentStep> path, byte[] searchKey)
    {
        for (int level = 0; level < path.Count; level++)
        {
            DescentStep step = path[level];
            int idx = SelectChildIndexFromDecoded(step.Entries, searchKey);
            if (idx != step.TakenIndex)
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Appends <paramref name="op"/> to the per-page op list keyed by
    /// <paramref name="pageNumber"/> in <paramref name="ops"/>, allocating
    /// the list lazily when this is the first op for that page.
    /// </summary>
    public static void AddIntermediateOp(
        Dictionary<long, List<IntermediateOp>> ops,
        long pageNumber,
        IntermediateOp op)
    {
        if (!ops.TryGetValue(pageNumber, out List<IntermediateOp>? list))
        {
            list = [];
            ops[pageNumber] = list;
        }

        list.Add(op);
    }

    /// <summary>
    /// Apply <paramref name="ops"/> to <paramref name="original"/> producing
    /// the post-mutation intermediate entry list. Ops are sorted by
    /// (OriginalIndex, declaration order); each entry index in
    /// <paramref name="original"/> is consumed at most once by a Replace.
    /// </summary>
    public static List<(byte[] Key, long DataPage, byte DataRow, long ChildPage)> ApplyIntermediateOps(
        List<IndexLeafIncremental.DecodedIntermediateEntry> original,
        List<IntermediateOp> ops)
    {
        // Stable sort by OriginalIndex; declaration order preserved within ties.
        var indexed = new (IntermediateOp Op, int Order)[ops.Count];
        for (int i = 0; i < ops.Count; i++)
        {
            indexed[i] = (ops[i], i);
        }

        Array.Sort(indexed, static (a, b) =>
        {
            int c = a.Op.OriginalIndex - b.Op.OriginalIndex;
            return c != 0 ? c : a.Order - b.Order;
        });

        var result = new List<(byte[], long, byte, long)>(original.Count + ops.Count);
        int opCursor = 0;
        for (int origIdx = 0; origIdx < original.Count; origIdx++)
        {
            // Consume any ops at this original index.
            bool replaced = false;
            bool removed = false;
            while (opCursor < indexed.Length && indexed[opCursor].Op.OriginalIndex == origIdx)
            {
                IntermediateOp op = indexed[opCursor].Op;
                opCursor++;
                switch (op.Type)
                {
                    case IntermediateOpType.Replace:
                        if (removed)
                        {
                            // Can't replace something that was already removed.
                            // Defensive — caller guards against this combination.
                            return [];
                        }

                        result.Add((op.NewKey, op.NewDataPage, op.NewDataRow, op.NewChildPage));
                        replaced = true;
                        break;

                    case IntermediateOpType.Remove:
                        if (replaced)
                        {
                            return [];
                        }

                        removed = true;
                        break;

                    case IntermediateOpType.InsertAfter:
                        // Insert after the (possibly replaced) original entry.
                        // If the original was removed, insert in its place.
                        if (!replaced && !removed)
                        {
                            IndexLeafIncremental.DecodedIntermediateEntry e = original[origIdx];
                            result.Add((e.Key, e.DataPage, e.DataRow, e.ChildPage));
                            replaced = true;
                        }

                        result.Add((op.NewKey, op.NewDataPage, op.NewDataRow, op.NewChildPage));
                        break;
                }
            }

            if (!replaced && !removed)
            {
                IndexLeafIncremental.DecodedIntermediateEntry e = original[origIdx];
                result.Add((e.Key, e.DataPage, e.DataRow, e.ChildPage));
            }
        }

        // Any ops past the final original index would be invalid (no
        // OriginalIndex == original.Count is meaningful). Drop them
        // defensively — caller treats this as a bail by checking the result
        // count vs. expected.
        return result;
    }
}
