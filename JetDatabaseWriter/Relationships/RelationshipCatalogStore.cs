namespace JetDatabaseWriter.Relationships;

using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema.Models;
using static JetDatabaseWriter.Constants.ColumnTypes;

internal sealed class RelationshipCatalogStore(AccessWriter writer)
{
    public async ValueTask AppendRelationshipRowsAsync(
        long msysRelTdefPage,
        TableDef msysRelDef,
        RelationshipDefinition relationship,
        CancellationToken cancellationToken)
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

            await writer.InsertSystemRowAndMaintainAsync(
                msysRelTdefPage,
                msysRelDef,
                Constants.SystemTableNames.Relationships,
                values,
                cancellationToken: cancellationToken).ConfigureAwait(false);
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

        long total = writer._stream.Length / writer._pgSz;
        for (long pageNumber = 3; pageNumber < total; pageNumber++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            byte[] page = await writer.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                if (page[0] != Constants.PageTypes.Data)
                {
                    continue;
                }

                if (AccessBase.Ri32(page, writer._dataPage.TDefOff) != msysRelTdefPage)
                {
                    continue;
                }

                foreach (RowLocation row in writer.EnumerateLiveRowLocations(pageNumber, page))
                {
                    string name = writer.DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, nameCol);
                    if (string.IsNullOrEmpty(name) || !namePredicate(name))
                    {
                        continue;
                    }

                    var values = new object[msysRelDef.Columns.Count];
                    for (int column = 0; column < values.Length; column++)
                    {
                        ColumnInfo tableColumn = msysRelDef.Columns[column];
                        string raw = writer.DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, tableColumn);
                        values[column] = string.IsNullOrEmpty(raw)
                            ? DBNull.Value
                            : tableColumn.Type switch
                            {
                                T_LONG => CatalogValueReader.ParseInt32OrZero(raw),
                                T_INT => (short)CatalogValueReader.ParseInt32OrZero(raw),
                                T_BYTE => (byte)CatalogValueReader.ParseInt32OrZero(raw),
                                _ => raw,
                            };
                    }

                    results.Add(new RelationshipRowSnapshot(
                        row,
                        name,
                        writer.DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, objCol),
                        writer.DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, refObjCol),
                        writer.DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, colCol),
                        writer.DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, refColCol),
                        CatalogValueReader.ParseInt32OrZero(writer.DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, icolCol)),
                        CatalogValueReader.ParseInt32OrZero(writer.DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, ccolCol)),
                        CatalogValueReader.ParseInt32OrZero(writer.DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, grbitCol)),
                        values));
                }
            }
            finally
            {
                AccessBase.ReturnPage(page);
            }
        }

        return results;
    }

    public ValueTask RewriteRowsAsync(
        long msysRelTdefPage,
        TableDef msysRelDef,
        IReadOnlyList<object[]> rows,
        CancellationToken cancellationToken)
        => writer.RewriteSystemTableRowsAsync(
            msysRelTdefPage,
            msysRelDef,
            Constants.SystemTableNames.Relationships,
            rows,
            cancellationToken);

    public async ValueTask<IReadOnlyList<FkRelationship>> GetEnforcedRelationshipsAsync(CancellationToken cancellationToken)
    {
        long page = await FindSystemTableTdefPageAsync(Constants.SystemTableNames.Relationships, cancellationToken).ConfigureAwait(false);
        if (page == 0)
        {
            return [];
        }

        DataTable table = await writer.ReadTableSnapshotAsync(Constants.SystemTableNames.Relationships, cancellationToken).ConfigureAwait(false);
        try
        {
            if (!table.Columns.Contains("szRelationship"))
            {
                return [];
            }

            var groups = new Dictionary<string, List<DataRow>>(StringComparer.OrdinalIgnoreCase);
            foreach (DataRow row in table.Rows)
            {
                object nameValue = row["szRelationship"];
                if (nameValue == DBNull.Value)
                {
                    continue;
                }

                string name = nameValue.ToString() ?? string.Empty;
                if (name.Length == 0)
                {
                    continue;
                }

                if (!groups.TryGetValue(name, out List<DataRow>? list))
                {
                    list = [];
                    groups[name] = list;
                }

                list.Add(row);
            }

            var result = new List<FkRelationship>(groups.Count);
            foreach (KeyValuePair<string, List<DataRow>> group in groups)
            {
                List<DataRow> rows = group.Value;
                rows.Sort((left, right) => RelationshipCatalogInt32(left["icolumn"])
                    .CompareTo(RelationshipCatalogInt32(right["icolumn"])));

                DataRow head = rows[0];
                int grbit = RelationshipCatalogInt32(head["grbit"]);
                if ((grbit & Constants.RelationshipFlags.NoRefIntegrity) != 0)
                {
                    continue;
                }

                string primaryTable = head["szReferencedObject"]?.ToString() ?? string.Empty;
                string foreignTable = head["szObject"]?.ToString() ?? string.Empty;
                if (primaryTable.Length == 0 || foreignTable.Length == 0)
                {
                    continue;
                }

                int declaredColumnCount = RelationshipCatalogInt32(head["ccolumn"]);
                if (declaredColumnCount <= 0 || declaredColumnCount != rows.Count)
                {
                    continue;
                }

                var primaryColumns = new string[rows.Count];
                var foreignColumns = new string[rows.Count];
                bool malformedColumns = false;
                for (int column = 0; column < rows.Count; column++)
                {
                    primaryColumns[column] = rows[column]["szReferencedColumn"]?.ToString() ?? string.Empty;
                    foreignColumns[column] = rows[column]["szColumn"]?.ToString() ?? string.Empty;
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

            return result;
        }
        finally
        {
            table.Dispose();
        }
    }

    public async ValueTask<long> FindSystemTableTdefPageAsync(string tableName, CancellationToken cancellationToken)
    {
        TableDef? msys = await writer.ReadTableDefAsync(2, cancellationToken).ConfigureAwait(false);
        if (msys == null)
        {
            return 0;
        }

        List<CatalogRow> rows = await writer.GetCatalogRowsAsync(msys, cancellationToken).ConfigureAwait(false);
        foreach (CatalogRow row in rows)
        {
            if (row.ObjectType == Constants.SystemObjects.UserTableType
                && row.TDefPage > 0
                && string.Equals(row.Name, tableName, StringComparison.OrdinalIgnoreCase))
            {
                return row.TDefPage;
            }
        }

        return 0;
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

        long total = writer._stream.Length / writer._pgSz;
        for (long pageNumber = 3; pageNumber < total; pageNumber++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            byte[] page = await writer.ReadPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                if (page[0] != Constants.PageTypes.Data)
                {
                    continue;
                }

                if (AccessBase.Ri32(page, writer._dataPage.TDefOff) != msysRelTdefPage)
                {
                    continue;
                }

                foreach (RowLocation row in writer.EnumerateLiveRowLocations(pageNumber, page))
                {
                    string name = writer.DecodeSimpleColumnValue(page, row.RowStart, row.RowSize, nameCol);
                    if (!string.IsNullOrEmpty(name))
                    {
                        _ = names.Add(name);
                    }
                }
            }
            finally
            {
                AccessBase.ReturnPage(page);
            }
        }

        return names;
    }

    private static int RelationshipCatalogInt32(object? value)
    {
        if (value == null || value is DBNull)
        {
            return 0;
        }

        try
        {
            return Convert.ToInt32(value, CultureInfo.InvariantCulture);
        }
        catch (Exception ex) when (ex is FormatException || ex is InvalidCastException || ex is OverflowException)
        {
            return 0;
        }
    }
}
