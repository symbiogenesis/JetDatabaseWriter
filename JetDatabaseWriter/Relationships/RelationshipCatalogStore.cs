namespace JetDatabaseWriter.Relationships;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tables;
using static JetDatabaseWriter.Enums.ColumnType;

/// <summary>
/// <c>MSysRelationships</c> row emission, loading, and rewrites, and the
/// writer session's cache of the enforced relationships that every insert,
/// update and delete checks.
/// </summary>
/// <remarks>
/// The cache is kept until <see cref="Invalidate"/> or until the table
/// catalog's <see cref="TableCatalog.Generation"/> moves on, which every
/// catalog write, rollback and failed commit does. It relies on two rules:
/// every write to <c>MSysRelationships</c> goes through
/// <see cref="AppendRelationshipRowsAsync"/> or <see cref="RewriteRowsAsync"/>,
/// which drop it; and no other process writes the file while the writer has
/// it open (see <c>docs/design/concurrency-and-lock-ordering.md</c>).
/// </remarks>
/// <param name="db">The database page I/O and format context.</param>
/// <param name="indexes">Inserts and rewrites system-table rows with index maintenance.</param>
/// <param name="catalogRows">Locates the <c>MSysRelationships</c> table.</param>
/// <param name="snapshots">Reads decoded <c>MSysRelationships</c> rows for enforcement.</param>
/// <param name="tableCatalog">The writer's table catalog, whose <see cref="TableCatalog.Generation"/> keys the cache.</param>
internal sealed class RelationshipCatalogStore(
    DatabaseFile db,
    IndexMaintainer indexes,
    CatalogRowReader catalogRows,
    TableSnapshotReader snapshots,
    TableCatalog tableCatalog)
{
    /// <summary>The enforced relationships last loaded, or <see langword="null"/> before the first load and after <see cref="Invalidate"/>.</summary>
    private volatile EnforcedRelationshipSet? enforced;

    /// <summary>The number of <see cref="Invalidate"/> calls so far, so a load that overlaps one is not cached.</summary>
    private int version;

    /// <summary>
    /// Appends one <c>MSysRelationships</c> row per key column of
    /// <paramref name="relationship"/>, and drops the cached enforced
    /// relationships, even when the append fails partway.
    /// </summary>
    /// <param name="msysRelTdefPage">The <c>MSysRelationships</c> TDEF page.</param>
    /// <param name="msysRelDef">The <c>MSysRelationships</c> table definition.</param>
    /// <param name="relationship">The relationship to append.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    public async ValueTask AppendRelationshipRowsAsync(
        long msysRelTdefPage,
        TableDef msysRelDef,
        RelationshipDefinition relationship,
        CancellationToken cancellationToken)
    {
        try
        {
            uint grbit = 0;
            if (!relationship.EnforceReferentialIntegrity)
            {
                grbit |= Constants.RelationshipFlags.NoRefIntegrity;
            }

            if (relationship.CascadeUpdates)
            {
                grbit |= Constants.RelationshipFlags.CascadeUpdates;
            }

            if (relationship.CascadeDeletes)
            {
                grbit |= Constants.RelationshipFlags.CascadeDeletes;
            }

            int ccolumn = relationship.PrimaryColumns.Count;
            int grbitInt = unchecked((int)grbit);

            for (int column = 0; column < ccolumn; column++)
            {
                object[] values = msysRelDef.CreateNullValueRow();

                msysRelDef.SetValueByName(values, "ccolumn", ccolumn);
                msysRelDef.SetValueByName(values, "grbit", grbitInt);
                msysRelDef.SetValueByName(values, "icolumn", column);
                msysRelDef.SetValueByName(values, "szColumn", relationship.ForeignColumns[column]);
                msysRelDef.SetValueByName(values, "szObject", relationship.ForeignTable);
                msysRelDef.SetValueByName(values, "szReferencedColumn", relationship.PrimaryColumns[column]);
                msysRelDef.SetValueByName(values, "szReferencedObject", relationship.PrimaryTable);
                msysRelDef.SetValueByName(values, "szRelationship", relationship.Name);

                await indexes.InsertSystemRowAndMaintainAsync(
                    msysRelTdefPage,
                    msysRelDef,
                    Constants.SystemTableNames.Relationships,
                    values,
                    cancellationToken: cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            this.Invalidate();
        }
    }

    public async ValueTask<List<RelationshipRowSnapshot>> CollectRowsAsync(
        long msysRelTdefPage,
        TableDef msysRelDef,
        Func<string, bool> namePredicate,
        CancellationToken cancellationToken)
    {
        var results = new List<RelationshipRowSnapshot>();
        ColumnInfo? nameCol = msysRelDef.FindColumn("szRelationship");
        ColumnInfo? objCol = msysRelDef.FindColumn("szObject");
        ColumnInfo? refObjCol = msysRelDef.FindColumn("szReferencedObject");
        ColumnInfo? colCol = msysRelDef.FindColumn("szColumn");
        ColumnInfo? refColCol = msysRelDef.FindColumn("szReferencedColumn");
        ColumnInfo? icolCol = msysRelDef.FindColumn("icolumn");
        ColumnInfo? ccolCol = msysRelDef.FindColumn("ccolumn");
        ColumnInfo? grbitCol = msysRelDef.FindColumn("grbit");
        if (nameCol == null || objCol == null || refObjCol == null || colCol == null
            || refColCol == null || icolCol == null || ccolCol == null || grbitCol == null)
        {
            return results;
        }

        await db.ForEachLiveTableRowAsync(
            msysRelTdefPage,
            (row, _) =>
            {
                byte[] page = row.Page;
                RowLocation location = row.Location;
                string name = db.DecodeSimpleColumnValue(page, location.RowStart, location.RowSize, nameCol);
                if (string.IsNullOrEmpty(name) || !namePredicate(name))
                {
                    return new ValueTask<bool>(true);
                }

                object[] values = new object[msysRelDef.Columns.Count];
                for (int column = 0; column < values.Length; column++)
                {
                    ColumnInfo tableColumn = msysRelDef.Columns[column];
                    string raw = db.DecodeSimpleColumnValue(page, location.RowStart, location.RowSize, tableColumn);
                    values[column] = string.IsNullOrEmpty(raw)
                        ? DBNull.Value
                        : tableColumn.Type switch
                        {
                            LongIntegerType => CatalogValueReader.ParseInt32OrZero(raw),
                            BigIntType => CatalogValueReader.ParseInt64OrZero(raw),
                            IntegerType => (short)CatalogValueReader.ParseInt32OrZero(raw),
                            ByteType => (byte)CatalogValueReader.ParseInt32OrZero(raw),
                            ColumnType.BooleanType or
                            ColumnType.MoneyType or
                            ColumnType.FloatType or
                            ColumnType.DoubleType or
                            ColumnType.DateTimeType or
                            ColumnType.BinaryType or
                            ColumnType.TextType or
                            ColumnType.OleType or
                            ColumnType.MemoType or
                            ColumnType.GuidType or
                            ColumnType.NumericType or
                            ColumnType.AttachmentType or
                            ColumnType.ComplexType or
                            ColumnType.DateTimeExtendedType or
                            _ => raw,
                        };
                }

                results.Add(new RelationshipRowSnapshot(
                    location,
                    name,
                    db.DecodeSimpleColumnValue(page, location.RowStart, location.RowSize, objCol),
                    db.DecodeSimpleColumnValue(page, location.RowStart, location.RowSize, refObjCol),
                    db.DecodeSimpleColumnValue(page, location.RowStart, location.RowSize, colCol),
                    db.DecodeSimpleColumnValue(page, location.RowStart, location.RowSize, refColCol),
                    CatalogValueReader.ParseInt32OrZero(db.DecodeSimpleColumnValue(page, location.RowStart, location.RowSize, icolCol)),
                    CatalogValueReader.ParseInt32OrZero(db.DecodeSimpleColumnValue(page, location.RowStart, location.RowSize, ccolCol)),
                    CatalogValueReader.ParseInt32OrZero(db.DecodeSimpleColumnValue(page, location.RowStart, location.RowSize, grbitCol)),
                    values));
                return new ValueTask<bool>(true);
            },
            cancellationToken).ConfigureAwait(false);

        return results;
    }

    /// <summary>
    /// Replaces every <c>MSysRelationships</c> row with <paramref name="rows"/>,
    /// and drops the cached enforced relationships, even when the rewrite
    /// fails partway.
    /// </summary>
    /// <param name="msysRelTdefPage">The <c>MSysRelationships</c> TDEF page.</param>
    /// <param name="msysRelDef">The <c>MSysRelationships</c> table definition.</param>
    /// <param name="rows">The rows the table holds afterwards.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    public async ValueTask RewriteRowsAsync(
        long msysRelTdefPage,
        TableDef msysRelDef,
        IReadOnlyList<object[]> rows,
        CancellationToken cancellationToken)
    {
        try
        {
            await indexes.RewriteSystemTableRowsAsync(
                msysRelTdefPage,
                msysRelDef,
                Constants.SystemTableNames.Relationships,
                rows,
                cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            this.Invalidate();
        }
    }

    /// <summary>
    /// Returns the relationships that enforce referential integrity. The set
    /// is loaded from <c>MSysRelationships</c> once and reused until a
    /// relationship row is written through this class or the table catalog's
    /// <see cref="TableCatalog.Generation"/> moves on (any catalog write, a
    /// rollback or a failed commit). A database with no
    /// <c>MSysRelationships</c> table caches the empty set.
    /// </summary>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    public async ValueTask<IReadOnlyList<FkRelationship>> GetEnforcedRelationshipsAsync(CancellationToken cancellationToken)
    {
        int catalogGeneration = tableCatalog.Generation;
        int storeVersion = Volatile.Read(ref this.version);
        EnforcedRelationshipSet? cached = this.enforced;
        if (cached is not null && cached.CatalogGeneration == catalogGeneration && cached.StoreVersion == storeVersion)
        {
            return cached.Relationships;
        }

        FkRelationship[] loaded = await this.LoadEnforcedRelationshipsAsync(cancellationToken).ConfigureAwait(false);

        // A load that overlapped a catalog change or a relationship write may
        // describe either side of it, so it is returned but not kept.
        if (tableCatalog.Generation == catalogGeneration && Volatile.Read(ref this.version) == storeVersion)
        {
            this.enforced = new EnforcedRelationshipSet(catalogGeneration, storeVersion, loaded);
        }

        return loaded;
    }

    public async ValueTask<HashSet<string>> ReadExistingRelationshipNamesAsync(
        long msysRelTdefPage,
        TableDef msysRelDef,
        CancellationToken cancellationToken)
    {
        var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        ColumnInfo? nameCol = msysRelDef.FindColumn("szRelationship");
        if (nameCol == null)
        {
            return names;
        }

        await db.ForEachLiveTableRowAsync(
            msysRelTdefPage,
            (row, _) =>
            {
                string name = db.DecodeSimpleColumnValue(row.Page, row.Location.RowStart, row.Location.RowSize, nameCol);
                if (!string.IsNullOrEmpty(name))
                {
                    names.Add(name);
                }

                return new ValueTask<bool>(true);
            },
            cancellationToken).ConfigureAwait(false);

        return names;
    }

    /// <summary>
    /// Drops the cached enforced relationships, so the next
    /// <see cref="GetEnforcedRelationshipsAsync"/> reads <c>MSysRelationships</c> again.
    /// </summary>
    internal void Invalidate()
    {
        _ = Interlocked.Increment(ref this.version);
        this.enforced = null;
    }

    private static int RelationshipCatalogInt32(object? value)
    {
        if (value is null or DBNull)
        {
            return 0;
        }

        try
        {
            return Convert.ToInt32(value, CultureInfo.InvariantCulture);
        }
        catch (Exception ex) when (ex is FormatException or InvalidCastException or OverflowException)
        {
            return 0;
        }
    }

    /// <summary>
    /// Returns the value at <paramref name="ordinal"/> of a decoded row, or
    /// <see langword="null"/> when the column is missing or the cell is null.
    /// </summary>
    /// <param name="values">The decoded row.</param>
    /// <param name="ordinal">The column's index, or -1 when the table has no such column.</param>
    private static object? CellAt(object[] values, int ordinal)
        => ordinal >= 0 && ordinal < values.Length && values[ordinal] is not DBNull ? values[ordinal] : null;

    /// <summary>Returns the text at <paramref name="ordinal"/> of a decoded row, or an empty string when there is none.</summary>
    /// <param name="values">The decoded row.</param>
    /// <param name="ordinal">The column's index, or -1 when the table has no such column.</param>
    private static string TextAt(object[] values, int ordinal)
        => CellAt(values, ordinal)?.ToString() ?? string.Empty;

    /// <summary>
    /// Reads the relationships that enforce referential integrity from
    /// <c>MSysRelationships</c>: the rows are grouped by name and ordered by
    /// <c>icolumn</c>, and a group is left out when its <c>grbit</c> has
    /// <c>NoRefIntegrity</c>, its row count differs from <c>ccolumn</c>, or a
    /// table or column name is empty. The table is found with one
    /// <c>MSysObjects</c> walk and its rows are read by page, through the
    /// writer's <see cref="DatabaseFile"/>, so an active transaction's pending
    /// writes are visible.
    /// </summary>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private async ValueTask<FkRelationship[]> LoadEnforcedRelationshipsAsync(CancellationToken cancellationToken)
    {
        long page = await catalogRows.FindSystemTableTdefPageAsync(Constants.SystemTableNames.Relationships, cancellationToken).ConfigureAwait(false);
        if (page <= 0)
        {
            return [];
        }

        TableDef? definition = await db.ReadTableDefAsync(page, cancellationToken).ConfigureAwait(false);
        int nameOrdinal = definition?.FindColumnIndex("szRelationship") ?? -1;
        if (definition is null || nameOrdinal < 0)
        {
            return [];
        }

        int grbitOrdinal = definition.FindColumnIndex("grbit");
        int ccolumnOrdinal = definition.FindColumnIndex("ccolumn");
        int icolumnOrdinal = definition.FindColumnIndex("icolumn");
        int objectOrdinal = definition.FindColumnIndex("szObject");
        int columnOrdinal = definition.FindColumnIndex("szColumn");
        int referencedObjectOrdinal = definition.FindColumnIndex("szReferencedObject");
        int referencedColumnOrdinal = definition.FindColumnIndex("szReferencedColumn");

        var groups = new Dictionary<string, List<object[]>>(StringComparer.OrdinalIgnoreCase);
        foreach (LocatedRow row in await snapshots.ReadRowsAsync(page, cancellationToken).ConfigureAwait(false))
        {
            string name = TextAt(row.Values, nameOrdinal);
            if (name.Length == 0)
            {
                continue;
            }

            if (!groups.TryGetValue(name, out List<object[]>? list))
            {
                list = [];
                groups[name] = list;
            }

            list.Add(row.Values);
        }

        var result = new List<FkRelationship>(groups.Count);
        foreach (KeyValuePair<string, List<object[]>> group in groups)
        {
            List<object[]> rows = group.Value;
            rows.Sort((left, right) => RelationshipCatalogInt32(CellAt(left, icolumnOrdinal))
                .CompareTo(RelationshipCatalogInt32(CellAt(right, icolumnOrdinal))));

            object[] head = rows[0];
            int grbit = RelationshipCatalogInt32(CellAt(head, grbitOrdinal));
            if ((grbit & Constants.RelationshipFlags.NoRefIntegrity) != 0)
            {
                continue;
            }

            string primaryTable = TextAt(head, referencedObjectOrdinal);
            string foreignTable = TextAt(head, objectOrdinal);
            if (primaryTable.Length == 0 || foreignTable.Length == 0)
            {
                continue;
            }

            int declaredColumnCount = RelationshipCatalogInt32(CellAt(head, ccolumnOrdinal));
            if (declaredColumnCount <= 0 || declaredColumnCount != rows.Count)
            {
                continue;
            }

            string[] primaryColumns = new string[rows.Count];
            string[] foreignColumns = new string[rows.Count];
            bool malformedColumns = false;
            for (int column = 0; column < rows.Count; column++)
            {
                primaryColumns[column] = TextAt(rows[column], referencedColumnOrdinal);
                foreignColumns[column] = TextAt(rows[column], columnOrdinal);
                if (primaryColumns[column].Length == 0 || foreignColumns[column].Length == 0)
                {
                    malformedColumns = true;
                }
            }

            if (malformedColumns)
            {
                continue;
            }

            result.Add(new FkRelationship(
                group.Key,
                primaryTable,
                primaryColumns,
                foreignTable,
                foreignColumns,
                (grbit & Constants.RelationshipFlags.CascadeUpdates) != 0,
                (grbit & Constants.RelationshipFlags.CascadeDeletes) != 0));
        }

        return [.. result];
    }

    /// <summary>The enforced relationships loaded under one catalog generation and store version.</summary>
    /// <param name="CatalogGeneration">The <see cref="TableCatalog.Generation"/> the set was loaded under.</param>
    /// <param name="StoreVersion">The store's invalidation count the set was loaded under.</param>
    /// <param name="Relationships">The relationships.</param>
    private sealed record EnforcedRelationshipSet(int CatalogGeneration, int StoreVersion, FkRelationship[] Relationships);
}
