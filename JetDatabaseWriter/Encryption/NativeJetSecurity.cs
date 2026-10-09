namespace JetDatabaseWriter.Encryption;

using System;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.ValueDecoding;
using JetDatabaseWriter.ValueDecoding.Models;

/// <summary>Remasks native Jet system security identities when a header password changes.</summary>
internal static class NativeJetSecurity
{
    /// <summary>Rewrites security fields through the destination's page codec.</summary>
    /// <param name="database">The destination database containing the source security fields.</param>
    /// <param name="pager">The destination page writer.</param>
    /// <param name="oldHeader">The source raw header.</param>
    /// <param name="newHeader">The destination raw header.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <param name="bootstrap">Whether this is an unpublished empty template whose ACE table will be initialized after remasking.</param>
    /// <exception cref="JetCorruptDataException">A required security field or row is malformed.</exception>
    /// <exception cref="NotSupportedException">The database does not use native Jet4 masked security identities.</exception>
    internal static async ValueTask RewriteAsync(DatabaseFile database, Pager pager, byte[] oldHeader, byte[] newHeader, CancellationToken cancellationToken, bool bootstrap = false)
    {
        if (!database.Format.UsesHeaderMaskedSecuritySids)
        {
            throw new NotSupportedException("Security identity remasking applies only to native Jet4 databases.");
        }

        var rows = new CatalogRowReader(database.Format, database.TableDefs, database.OwnedPages);
        await RewriteColumnAsync(2, "MSysObjects", "Owner").ConfigureAwait(false);
        if (!bootstrap)
        {
            long acesPage = await rows.FindSystemTableTdefPageAsync("MSysACEs", cancellationToken).ConfigureAwait(false);
            await RewriteColumnAsync(acesPage, "MSysACEs", "SID").ConfigureAwait(false);
        }

        async ValueTask RewriteColumnAsync(long page, string table, string columnName)
        {
            TableDef definition = await database.TableDefs.ReadRequiredTableDefAsync(page, table, cancellationToken).ConfigureAwait(false);
            ColumnInfo column = definition.FindColumn(columnName) ?? throw new JetCorruptDataException($"{table} is missing its security {columnName} field.");
            byte[] tdef = await database.TableDefs.ReadTDefBytesAsync(page, cancellationToken).ConfigureAwait(false)
                ?? throw new JetCorruptDataException($"{table} has no security table definition.");
            TDefCounts counts = TDefCodec.ReadCounts(database.Format, tdef);
            if (counts.RealIndexCount is < 0 or > Constants.TableDefinition.MaxIndexes
                || counts.LogicalIndexCount is < 0 or > Constants.TableDefinition.MaxIndexes)
            {
                throw new JetCorruptDataException($"{table} has invalid security index counts.");
            }

            if (counts.RealIndexCount != 0 || counts.LogicalIndexCount != 0)
            {
                int descriptorStart = IndexCatalogReader.LocateRealIdxDescStart(database.Format, tdef, counts.ColumnCount, counts.RealIndexCount);
                if (descriptorStart < 0)
                {
                    throw new JetCorruptDataException($"{table} has a malformed security index catalog.");
                }

                IndexSectionAnchors anchors = database.Format.Index.GetIndexSection(descriptorStart, counts.RealIndexCount, counts.LogicalIndexCount);
                for (int index = 0; index < counts.RealIndexCount; index++)
                {
                    if (!database.Format.Index.TryReadRealIdxSlotWithKeyColumns(tdef, descriptorStart, index, out _, out System.Collections.Generic.List<KeyColumn>? keys))
                    {
                        throw new JetCorruptDataException($"{table} has a malformed security index descriptor.");
                    }

                    foreach (KeyColumn key in keys)
                    {
                        if (key.ColNum == column.ColNum)
                        {
                            throw new NotSupportedException($"Password maintenance cannot remask the indexed security field {table}.{columnName}.");
                        }

                        if (definition.FindColumnIndex(candidate => candidate.ColNum == key.ColNum) < 0)
                        {
                            throw new JetCorruptDataException($"{table} has an unresolved security index column.");
                        }
                    }
                }

                for (int index = 0; index < counts.LogicalIndexCount; index++)
                {
                    if (!database.Format.Index.TryReadLogicalEntry(tdef, anchors.LogIdxStart, index, out LogicalIdxEntry entry)
                        || entry.IndexNum2 < 0 || entry.IndexNum2 >= counts.RealIndexCount)
                    {
                        throw new JetCorruptDataException($"{table} has a malformed logical security index.");
                    }
                }
            }

            await database.OwnedPages.ForEachLiveTableRowAsync(
                page,
                async (row, token) =>
                {
                    if (!RowDecodePlan.TryParseRowLayout(database.Format.RowFields, row.Page, row.Location.RowStart, row.Location.RowSize, definition.HasVarColumns, out RowLayout layout))
                    {
                        throw new JetCorruptDataException($"{table} has a malformed security row.");
                    }

                    ColumnSlice slice = RowDecodePlan.ResolveColumnSlice(database.Format.RowFields, row.Page, row.Location.RowStart, row.Location.RowSize, layout, column);
                    if (slice.Kind == ColumnSliceKind.Null || (slice.Kind == ColumnSliceKind.Var && slice.DataLen == 0))
                    {
                        return true;
                    }

                    if (slice.Kind is not (ColumnSliceKind.Fixed or ColumnSliceKind.Var))
                    {
                        throw new JetCorruptDataException($"{table} has a malformed security identity.");
                    }

                    int offset = row.Location.RowStart + slice.DataStart;
                    byte[] identity = Jet4SecuritySid.Encode(database.Format, oldHeader, row.Page.AsSpan(offset, slice.DataLen));
                    byte[]? remasked = null;
                    byte[]? updated = null;
                    try
                    {
                        remasked = Jet4SecuritySid.Encode(database.Format, newHeader, identity);
                        updated = await pager.ReadPageAsync(row.Location.DataPageNumber, token).ConfigureAwait(false);
                        remasked.CopyTo(updated, offset);
                        await pager.WritePageAsync(row.Location.DataPageNumber, updated, token).ConfigureAwait(false);
                    }
                    finally
                    {
                        CryptographicOperations.ZeroMemory(identity);
                        OfficeCryptoPrimitives.ZeroIfNotNull(remasked);
                        if (updated is not null)
                        {
                            PageBuffers.Return(updated);
                        }
                    }

                    return true;
                },
                cancellationToken,
                requireCompleteRows: true).ConfigureAwait(false);
        }
    }
}
