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
using JetDatabaseWriter.Pages;
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
    /// <param name="format">The destination format profile.</param>
    /// <param name="tableDefs">The destination table definition reader.</param>
    /// <param name="ownedPages">The destination table row traversal.</param>
    /// <param name="pager">The destination page writer.</param>
    /// <param name="indexes">The destination index maintenance service.</param>
    /// <param name="oldHeader">The source raw header.</param>
    /// <param name="newHeader">The destination raw header.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <param name="bootstrap">Whether this is an unpublished empty template whose ACE table will be initialized after remasking.</param>
    /// <exception cref="JetCorruptDataException">A required security field or row is malformed.</exception>
    /// <exception cref="NotSupportedException">The database does not use native Jet4 masked security identities.</exception>
    internal static async ValueTask RewriteAsync(JetFormat format, TableDefReader tableDefs, OwnedDataPages ownedPages, Pager pager, IndexMaintainer indexes, byte[] oldHeader, byte[] newHeader, CancellationToken cancellationToken, bool bootstrap = false)
    {
        if (!format.UsesHeaderMaskedSecuritySids)
        {
            throw new NotSupportedException("Security identity remasking applies only to native Jet4 databases.");
        }

        var rows = new CatalogRowReader(format, tableDefs, ownedPages);
        long acesPage = bootstrap ? 0 : await rows.FindSystemTableTdefPageAsync("MSysACEs", cancellationToken).ConfigureAwait(false);
        await RewriteColumnAsync(2, "MSysObjects", "Owner", preflightOnly: true).ConfigureAwait(false);
        if (!bootstrap)
        {
            await RewriteColumnAsync(acesPage, "MSysACEs", "SID", preflightOnly: true).ConfigureAwait(false);
        }

        await RewriteColumnAsync(2, "MSysObjects", "Owner", preflightOnly: false).ConfigureAwait(false);
        if (!bootstrap)
        {
            await RewriteColumnAsync(acesPage, "MSysACEs", "SID", preflightOnly: false).ConfigureAwait(false);
        }

        async ValueTask RewriteColumnAsync(long page, string table, string columnName, bool preflightOnly)
        {
            TableDef definition = await tableDefs.ReadRequiredTableDefAsync(page, table, cancellationToken).ConfigureAwait(false);
            ColumnInfo column = definition.FindColumn(columnName) ?? throw new JetCorruptDataException($"{table} is missing its security {columnName} field.");
            byte[] tdef = await tableDefs.ReadTDefBytesAsync(page, cancellationToken).ConfigureAwait(false)
                ?? throw new JetCorruptDataException($"{table} has no security table definition.");
            TDefCounts counts = TDefCodec.ReadCounts(format, tdef);
            bool indexedIdentity = false;
            if (counts.RealIndexCount is < 0 or > Constants.TableDefinition.MaxIndexes
                || counts.LogicalIndexCount is < 0 or > Constants.TableDefinition.MaxIndexes)
            {
                throw new JetCorruptDataException($"{table} has invalid security index counts.");
            }

            if (counts.RealIndexCount != 0 || counts.LogicalIndexCount != 0)
            {
                int descriptorStart = IndexCatalogReader.LocateRealIdxDescStart(format, tdef, counts.ColumnCount, counts.RealIndexCount);
                if (descriptorStart < 0)
                {
                    throw new JetCorruptDataException($"{table} has a malformed security index catalog.");
                }

                IndexSectionAnchors anchors = format.Index.GetIndexSection(descriptorStart, counts.RealIndexCount, counts.LogicalIndexCount);
                for (int index = 0; index < counts.RealIndexCount; index++)
                {
                    if (!format.Index.TryReadRealIdxSlotWithKeyColumns(tdef, descriptorStart, index, out _, out System.Collections.Generic.List<KeyColumn>? keys))
                    {
                        throw new JetCorruptDataException($"{table} has a malformed security index descriptor.");
                    }

                    foreach (KeyColumn key in keys)
                    {
                        if (key.ColNum == column.ColNum)
                        {
                            indexedIdentity = true;
                        }

                        if (definition.FindColumnIndex(candidate => candidate.ColNum == key.ColNum) < 0)
                        {
                            throw new JetCorruptDataException($"{table} has an unresolved security index column.");
                        }
                    }
                }

                for (int index = 0; index < counts.LogicalIndexCount; index++)
                {
                    if (!format.Index.TryReadLogicalEntry(tdef, anchors.LogIdxStart, index, out LogicalIdxEntry entry)
                        || entry.IndexNum2 < 0 || entry.IndexNum2 >= counts.RealIndexCount)
                    {
                        throw new JetCorruptDataException($"{table} has a malformed logical security index.");
                    }
                }
            }

            if (indexedIdentity)
            {
                await indexes.ThrowIfIndexesUnmaintainableAsync(page, definition, table, cancellationToken).ConfigureAwait(false);
            }

            if (preflightOnly)
            {
                return;
            }

            await ownedPages.ForEachLiveTableRowAsync(
                page,
                async (row, token) =>
                {
                    if (!RowDecodePlan.TryParseRowLayout(format.RowFields, row.Page, row.Location.RowStart, row.Location.RowSize, definition.HasVarColumns, out RowLayout layout))
                    {
                        throw new JetCorruptDataException($"{table} has a malformed security row.");
                    }

                    ColumnSlice slice = RowDecodePlan.ResolveColumnSlice(format.RowFields, row.Page, row.Location.RowStart, row.Location.RowSize, layout, column);
                    if (slice.Kind == ColumnSliceKind.Null || (slice.Kind == ColumnSliceKind.Var && slice.DataLen == 0))
                    {
                        return true;
                    }

                    if (slice.Kind is not (ColumnSliceKind.Fixed or ColumnSliceKind.Var))
                    {
                        throw new JetCorruptDataException($"{table} has a malformed security identity.");
                    }

                    int offset = row.Location.RowStart + slice.DataStart;
                    byte[] identity = Jet4SecuritySid.Encode(format, oldHeader, row.Page.AsSpan(offset, slice.DataLen));
                    byte[]? remasked = null;
                    byte[]? updated = null;
                    try
                    {
                        remasked = Jet4SecuritySid.Encode(format, newHeader, identity);
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

            if (indexedIdentity)
            {
                await indexes.MaintainIndexesAsync(page, definition, table, cancellationToken).ConfigureAwait(false);
            }
        }
    }
}
