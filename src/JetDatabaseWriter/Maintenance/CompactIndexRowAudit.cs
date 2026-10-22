namespace JetDatabaseWriter.Maintenance;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;

/// <summary>Compares stored leaf keys with independently scanned raw rows for explicitly supported scalar encodings.</summary>
/// <remarks>
/// The caller retains one immutable decrypted source and its verified definition throughout both scans.
/// Physical discovery covers live same-owner row headers, including verified moved payloads; optional active
/// membership covers its exact independently agreed rows while retaining unassigned physical originals.
/// Every included row must appear exactly once; ignored all-null rows must not appear. Ordinary Boolean
/// masks derive native non-null keys for both values; schema-absent Boolean slots derive false. Complex keys compare
/// only the stored four-byte parent reference, without following items. Ordinary Memo keys follow complete
/// bounded LVAL storage and decode only its exact declared payload. Calculated and unsupported
/// keys refuse. Intermediate keys must also match their independent rows, and complete
/// observed leaf chains, routing, exact separators and bookmark targets must agree. Date keys use their exact
/// stored double without calendar conversion. Native constraints,
/// complete source ownership, recovered catalog identities and reclamation remain separate requirements.
/// </remarks>
internal static class CompactIndexRowAudit
{
    /// <summary>Checks whether the descriptor names distinct known scalar columns with supported key codecs.</summary>
    /// <param name="definition">The immutable independently verified native definition.</param>
    /// <param name="descriptorIndex">The physical index ordinal.</param>
    /// <returns>Whether metadata supports attempted validation; malformed or unrepresentable stored values can still refuse.</returns>
    internal static bool SupportsDescriptor(TDefImage definition, int descriptorIndex)
    {
        Guard.NotNull(definition, nameof(definition));
        if (definition.Issues != TDefParseIssues.None || definition.Columns.Count > Constants.TableDefinition.MaxColumns
            || descriptorIndex < 0 || descriptorIndex >= definition.RealIndexes.Count)
        {
            return false;
        }

        TDefIndexDescriptor index = definition.RealIndexes[descriptorIndex];
        if (index.Columns.Count is < 1 or > 10)
        {
            return false;
        }

        for (int ordinal = 0; ordinal < index.Columns.Count; ordinal++)
        {
            KeyColumn key = index.Columns[ordinal];
            for (int earlier = 0; earlier < ordinal; earlier++)
            {
                if (index.Columns[earlier].ColNum == key.ColNum)
                {
                    return false;
                }
            }

            ColumnInfo? found = null;
            foreach (ColumnInfo column in definition.Columns)
            {
                if (column.ColNum == key.ColNum)
                {
                    if (found is not null)
                    {
                        return false;
                    }

                    found = column;
                }
            }

            if (found?.IsCalculated != false || !SupportedType(found.Type)
                || (found.Type is ColumnType.TextType or ColumnType.MemoType && !found.TextSortOrder.IsSupported))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>Checks whether an already supported descriptor requires ordinary Memo traversal.</summary>
    /// <param name="definition">The independently verified definition.</param>
    /// <param name="descriptorIndex">The supported physical descriptor ordinal.</param>
    /// <returns>Whether an indexed column stores ordinary Memo values.</returns>
    internal static bool HasMemoColumn(TDefImage definition, int descriptorIndex)
    {
        foreach (KeyColumn key in definition.RealIndexes[descriptorIndex].Columns)
        {
            foreach (ColumnInfo column in definition.Columns)
            {
                if (column.ColNum == key.ColNum && column.Type == ColumnType.MemoType)
                {
                    return true;
                }
            }
        }

        return false;
    }

    /// <summary>Validates exact leaf-key, bookmark, duplicate-entry and discovered-row coverage correspondence.</summary>
    /// <param name="pages">The immutable caller-owned decrypted source.</param>
    /// <param name="format">The verified source format.</param>
    /// <param name="ownerTDefPage">The expected original definition and physical row owner.</param>
    /// <param name="definition">The independently verified immutable definition from this source.</param>
    /// <param name="descriptorIndex">The physical stored index ordinal.</param>
    /// <param name="rowBudget">The independent raw-row scan's limits.</param>
    /// <param name="indexBudget">The independent raw-index scan's limits.</param>
    /// <param name="budget">Retained derived-key and scalar-decoding admission limits.</param>
    /// <param name="cancellationToken">Cancels both source scans, scalar derivation and final coverage inspection.</param>
    /// <param name="activeRows">Optional exact source-bound active membership; all unassigned physical originals remain pinned and replayed.</param>
    /// <returns>Scoped leaf correspondence evidence without a global ownership or database-validity claim.</returns>
    /// <exception cref="JetNotSupportedException">A key type, collation or stored scalar value has no supported faithful encoding.</exception>
    /// <exception cref="JetCorruptDataException">Definition identity, raw rows or stored leaf correspondence are malformed or incomplete.</exception>
    /// <exception cref="JetLimitationException">Row, index, key-length or simultaneous storage admission is exhausted.</exception>
    internal static async ValueTask<CompactIndexRowAuditResult> ValidateAsync(IPageSource pages, JetFormat format, long ownerTDefPage, TDefImage definition, int descriptorIndex, CompactRawRowBudget rowBudget, CompactIndexBudget indexBudget, CompactIndexRowBudget budget, CancellationToken cancellationToken, CompactActiveTableRows? activeRows = null)
    {
        Guard.NotNull(pages, nameof(pages));
        Guard.NotNull(format, nameof(format));
        Guard.NotNull(definition, nameof(definition));
        Guard.NotNull(rowBudget, nameof(rowBudget));
        Guard.NotNull(indexBudget, nameof(indexBudget));
        Guard.NotNull(budget, nameof(budget));
        Guard.InRange(budget.MaxRows, 0L, long.MaxValue, nameof(budget));
        Guard.Positive(budget.MaxKeyBytes, nameof(budget));
        Guard.InRange(budget.MaxWorkingBytes, 1L, long.MaxValue, nameof(budget));
        cancellationToken.ThrowIfCancellationRequested();
        if (!SupportsDescriptor(definition, descriptorIndex))
        {
            throw Unsupported(ownerTDefPage, "The stored index requires an unsupported or ambiguous scalar-key interpretation.");
        }

        if (definition.TDefPageNumber != ownerTDefPage)
        {
            throw Corrupt(ownerTDefPage, "The independently supplied definition does not identify the audited physical owner.");
        }

        TDefIndexDescriptor index = definition.RealIndexes[descriptorIndex];
        var accounting = CompactDiscoveryAccounting.ForWorkingStorage(budget.MaxWorkingBytes, budget.Accounting);
        CompactDiscoverySession? memoValues = null;
        if (HasMemoColumn(definition, descriptorIndex))
        {
            Guard.NotNull(budget.LongValues, nameof(budget));
            IPageSource memoPages = activeRows?.MemoAuditPages(pages, definition, cancellationToken) ?? pages;
            memoValues = new CompactDiscoverySession(memoPages, format, budget.LongValues, accounting);
        }

        IPageSource indexPages = activeRows?.IndexAuditPages(pages, definition, cancellationToken) ?? pages;
        long scratch = (64L * format.PageSize) + 65_536;
        if (scratch > budget.MaxWorkingBytes)
        {
            throw Limit(ownerTDefPage, "Scalar-key scratch storage exceeds MaxWorkingBytes.");
        }

        using IDisposable scratchReservation = accounting.ReserveWorkingBytes(scratch, ownerTDefPage);
        var rows = new Dictionary<(long Page, int Row), KeyEvidence>();
        long sourceRows = 0;
        long indexedRows = 0;
        long excluded = 0;
        long working = scratch;
        long sourcePages = pages.PageCount;
        bool terminal = false;
        try
        {
            if (activeRows is null)
            {
                await CompactRawRowReader.ScanAsync(pages, format, ownerTDefPage, rowBudget, InspectRowAsync, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                await activeRows.ScanIndexRowsAsync(InspectRowAsync, cancellationToken).ConfigureAwait(false);
            }

            CheckGeometry();
            await CompactIndexBookmarkAudit.ScanAsync(
                indexPages,
                format,
                ownerTDefPage,
                index.RootPage,
                indexBudget,
                new()
                {
                    Accounting = accounting,
                    MaxPageReads = rowBudget.MaxPageReads,
                    MaxDirectorySlots = rowBudget.MaxDirectorySlots,
                    MaxOverflowHops = rowBudget.MaxOverflowHops,
                    MaxWorkingBytes = budget.MaxWorkingBytes,
                },
                InspectIndexAsync,
                cancellationToken).ConfigureAwait(false);
            CheckGeometry();
            if (!terminal)
            {
                throw Corrupt(index.RootPage, "The stored index did not supply complete composed tree, routing, separator and bookmark evidence.");
            }

            foreach (KeyValuePair<(long Page, int Row), KeyEvidence> row in rows)
            {
                cancellationToken.ThrowIfCancellationRequested();
                accounting.Charge(CompactDiscoveryCharge.GraphSteps, 1, row.Key.Page);
                if (!row.Value.Observed)
                {
                    throw Corrupt(row.Key.Page, $"An independently discovered included row is missing from the stored index (owner {ownerTDefPage}, root {index.RootPage}, bookmark {row.Key.Page}/{row.Key.Row}, rows {sourceRows}/{indexedRows}).");
                }
            }

            return new(sourceRows, indexedRows, excluded);
        }
        finally
        {
            foreach (KeyEvidence evidence in rows.Values)
            {
                evidence.Reservation?.Dispose();
            }
        }

        void CheckGeometry()
        {
            pages.ThrowIfDisposedOrCancelled(cancellationToken);
            activeRows?.CheckSource(pages, definition, cancellationToken);
            if (pages.PageCount != sourcePages || pages.PageSize != format.PageSize)
            {
                throw Corrupt(ownerTDefPage, "The scalar-key source geometry changed between independent row and index scans.");
            }
        }

        async ValueTask InspectRowAsync(RowLocation location, ReadOnlyMemory<byte> payload, CancellationToken token)
        {
            if (sourceRows >= budget.MaxRows)
            {
                throw Limit(location.PageNumber, "Independent scalar-key rows exceed MaxRows.");
            }

            sourceRows++;
            (byte[] key, bool allNull) = await ReadKeyAsync(format, definition, descriptorIndex, location, payload, budget.MaxKeyBytes, accounting, memoValues, budget.LongValues, token).ConfigureAwait(false);
            if (allNull && (index.Slot.Flags & Constants.TableDefinition.IgnoreNullsIndexFlag) != 0)
            {
                excluded++;
                return;
            }

            long storage = 256L + key.Length;
            if (storage > budget.MaxWorkingBytes - working)
            {
                throw Limit(location.PageNumber, "Retained scalar-key row evidence exceeds MaxWorkingBytes.");
            }

            IDisposable? reservation = accounting.ReserveWorkingBytes(storage, location.PageNumber);
            try
            {
                if (!rows.TryAdd((location.PageNumber, location.RowIndex), new(location, key, reservation)))
                {
                    throw Corrupt(location.PageNumber, "Independent row discovery repeated a header bookmark.");
                }

                working += storage;
                reservation = null;
            }
            finally
            {
                reservation?.Dispose();
            }
        }

        ValueTask InspectIndexAsync(CompactIndexRecord record, CancellationToken token)
        {
            token.ThrowIfCancellationRequested();
            if (record.Kind == "index-terminal")
            {
                terminal = record.NavigableGraphAcyclic && record.EntryEdgeFencesValidated
                    && record.ObservedLeafChainOrdered && record.ObservedKeyRoutingValidated
                    && record.ObservedChildMaximumSeparatorsValidated && record.BookmarkTargetsValidated;
                return default;
            }

            if (record.Kind is not ("leaf-entry" or "intermediate-entry"))
            {
                return default;
            }

            accounting.Charge(CompactDiscoveryCharge.GraphSteps, 1, record.PageNumber);
            if (!rows.TryGetValue((record.TargetPageNumber!.Value, record.TargetRowIndex!.Value), out KeyEvidence? row))
            {
                throw Corrupt(record.PageNumber, "A stored index entry names an independently undiscovered or excluded row.");
            }

            if (record.Kind == "leaf-entry" && row.Observed)
            {
                throw Corrupt(record.PageNumber, "A stored leaf entry repeats a row bookmark.");
            }

            if (!record.CanonicalKey!.AsSpan().SequenceEqual(row.Key))
            {
                throw Corrupt(record.PageNumber, $"A stored index key disagrees with independently decoded fields (owner {ownerTDefPage}, root {index.RootPage}, descriptor {descriptorIndex}, bookmark {record.TargetPageNumber}/{record.TargetRowIndex}, key lengths {record.CanonicalKey!.Length}/{row.Key.Length}).");
            }

            if (!record.BookmarkTargetValidated || record.BookmarkDataPageNumber != row.Location.DataPageNumber
                || record.BookmarkDataRowIndex != row.Location.DataRowIndex)
            {
                throw Corrupt(record.PageNumber, "A stored index bookmark resolves to a different payload than its independently scanned row.");
            }

            if (record.Kind == "leaf-entry")
            {
                row.Observed = true;
                indexedRows++;
            }

            return default;
        }
    }

    /// <summary>Strictly derives one supported composite key from an independently located raw row.</summary>
    /// <param name="format">The verified format.</param>
    /// <param name="definition">The independently verified definition.</param>
    /// <param name="descriptorIndex">A descriptor already accepted by <see cref="SupportsDescriptor"/>.</param>
    /// <param name="location">The original row header and resolved payload coordinates.</param>
    /// <param name="payload">The borrowed complete stored row.</param>
    /// <param name="maxKeyBytes">The combined key byte allowance.</param>
    /// <param name="accounting">Optional cumulative field admission.</param>
    /// <param name="memoValues">The admitted source session for complete ordinary Memo traversal.</param>
    /// <param name="memoBudget">The source session's immutable per-value and cumulative Memo limits.</param>
    /// <param name="cancellationToken">Cancels field inspection.</param>
    /// <returns>The complete native key and whether every key component was null.</returns>
    /// <exception cref="JetNotSupportedException">A stored value has no faithful supported key encoding.</exception>
    /// <exception cref="JetLimitationException">The combined key exceeds its byte allowance.</exception>
    internal static async ValueTask<(byte[] Key, bool AllNull)> ReadKeyAsync(JetFormat format, TDefImage definition, int descriptorIndex, RowLocation location, ReadOnlyMemory<byte> payload, int maxKeyBytes, CompactDiscoveryAccounting? accounting, CompactDiscoverySession? memoValues, CompactDiscoveryBudget memoBudget, CancellationToken cancellationToken)
    {
        TDefIndexDescriptor index = definition.RealIndexes[descriptorIndex];
        byte[][] parts = new byte[index.Columns.Count][];
        bool allNull = true;
        int keyLength = 0;
        await CompactRawFieldReader.ScanAsync(format, definition, location, payload, InspectFieldAsync, cancellationToken).ConfigureAwait(false);
        byte[] key = new byte[keyLength];
        int offset = 0;
        foreach (byte[] part in parts)
        {
            part.CopyTo(key, offset);
            offset += part.Length;
        }

        return (key, allNull);

        async ValueTask InspectFieldAsync(CompactRawField field, CancellationToken token)
        {
            token.ThrowIfCancellationRequested();
            accounting?.Charge(CompactDiscoveryCharge.Fields, 1, location.PageNumber);
            for (int ordinal = 0; ordinal < index.Columns.Count; ordinal++)
            {
                KeyColumn column = index.Columns[ordinal];
                if (column.ColNum != field.Column.ColNum)
                {
                    continue;
                }

                byte[] encoded;
                if (field.Column.Type == ColumnType.BooleanType)
                {
                    if (field.Kind is not (CompactRawFieldKind.Boolean or CompactRawFieldKind.Absent))
                    {
                        throw Unsupported(location.PageNumber, "An indexed Boolean requires an ordinary mask value or schema-absent slot.");
                    }

                    // Native schema evolution reads absent Boolean slots as false, even with a True default.
                    // Both ordinary values remain included under IgnoreNulls.
                    bool booleanValue = field.Kind == CompactRawFieldKind.Boolean && field.BoolValue;
                    encoded = column.Ascending
                        ? [0x7F, booleanValue ? (byte)0 : byte.MaxValue]
                        : [0x80, booleanValue ? byte.MaxValue : (byte)0];
                    allNull = false;
                }
                else if (field.Column.Type == ColumnType.MemoType && field.Kind == CompactRawFieldKind.Stored)
                {
                    encoded = await ReadMemoKeyAsync(format, field, column, memoValues ?? throw Unsupported(location.PageNumber, "An indexed Memo requires an admitted immutable source session."), memoBudget, token).ConfigureAwait(false);
                    allNull = false;
                }
                else
                {
                    object? value = DecodeField(format, field);
                    allNull &= value is null;
                    encoded = EncodeFieldKey(format, field, column, value);
                }

                if (encoded.Length > maxKeyBytes - keyLength)
                {
                    throw Limit(location.PageNumber, "A derived scalar key exceeds MaxKeyBytes.");
                }

                parts[ordinal] = encoded;
                keyLength += encoded.Length;
            }
        }
    }

    private static bool SupportedType(ColumnType type)
        => type is ColumnType.BooleanType or ColumnType.ByteType or ColumnType.IntegerType or ColumnType.LongIntegerType
            or ColumnType.MoneyType or ColumnType.GuidType or ColumnType.NumericType or ColumnType.BigIntType
            or ColumnType.TextType or ColumnType.MemoType or ColumnType.BinaryType or ColumnType.DateTimeExtendedType
            or ColumnType.FloatType or ColumnType.DoubleType or ColumnType.DateTimeType or ColumnType.ComplexType;

    private static object? DecodeField(JetFormat format, CompactRawField field)
    {
        if (field.Kind is CompactRawFieldKind.Absent or CompactRawFieldKind.Null)
        {
            return null;
        }

        if (field.Column.Type == ColumnType.DateTimeType)
        {
            return JetTypeInfo.ReadDoubleLittleEndian(field.RawBytes.Span);
        }

        if (field.Column.Type == ColumnType.ComplexType)
        {
            if (field.RawBytes.Length != sizeof(int))
            {
                throw Corrupt(field.Location.PageNumber, "An indexed complex parent reference does not occupy exactly four stored bytes.");
            }

            int reference = BinaryPrimitives.ReadInt32LittleEndian(field.RawBytes.Span);
            if (reference <= 0)
            {
                throw Corrupt(field.Location.PageNumber, "An indexed assigned complex parent reference is not positive.");
            }

            return reference;
        }

        if (field.Column.Type is ColumnType.BinaryType or ColumnType.DateTimeExtendedType)
        {
            return field.RawBytes.ToArray();
        }

        if (field.Column.Type == ColumnType.TextType)
        {
            byte[] bytes = field.RawBytes.ToArray();
            return DecodeTextField(format, field, bytes);
        }

        object value = JetTypeInfo.ReadFixedTyped(field.RawBytes.Span, 0, field.Column, field.RawBytes.Length, strictNumeric: true);
        if (value is DBNull)
        {
            throw Corrupt(field.Location.PageNumber, "An assigned scalar key value could not be decoded faithfully.");
        }

        return value;
    }

    private static byte[] EncodeFieldKey(JetFormat format, CompactRawField field, KeyColumn column, object? value)
    {
        // Date keys use the stored IEEE double without calendar rounding.
        ColumnType keyType = field.Column.Type == ColumnType.DateTimeType ? ColumnType.DoubleType : field.Column.Type;
        return IndexKeyEncoder.TryEncodeColumnEntry(keyType, value, column.Ascending, field.Column.NumericScale, format.LegacyNumericIndexKeys, field.Column.TextSortOrder, field.Column.Type == ColumnType.BinaryType && field.Column.IsFixed ? field.Column.Size : 0)
            ?? throw Unsupported(field.Location.PageNumber, "An independently decoded scalar value has no supported exact index-key encoding.");
    }

    private static async ValueTask<byte[]> ReadMemoKeyAsync(JetFormat format, CompactRawField field, KeyColumn column, CompactDiscoverySession values, CompactDiscoveryBudget budget, CancellationToken token)
    {
        if (!LongValueDescriptor.TryRead(field.RawBytes.Span, out LongValueDescriptor descriptor))
        {
            throw Corrupt(field.Location.PageNumber, "An indexed Memo has a truncated native long-value descriptor.");
        }

        if (descriptor.Length > budget.MaxValueBytes)
        {
            throw Limit(field.Location.PageNumber, "An indexed Memo payload exceeds MaxValueBytes.");
        }

        values.Accounting.Charge(CompactDiscoveryCharge.LongValuePayloadBytes, descriptor.Length, field.Location.PageNumber);
        using IDisposable storage = values.Accounting.ReserveWorkingBytes((8L * descriptor.Length) + 65_536, field.Location.PageNumber);
        byte[] payload = new byte[descriptor.Length];
        int offset = 0;
        bool terminal = false;
        await values.AuditLongValueAsync(field.RawBytes, InspectAsync, token).ConfigureAwait(false);
        if (!terminal || offset != payload.Length)
        {
            throw Corrupt(field.Location.PageNumber, "The independently audited indexed Memo payload is incomplete.");
        }

        string text = DecodeTextField(format, field, payload, descriptor.IsInline || descriptor.IsSinglePage);
        return EncodeFieldKey(format, field, column, text);

        ValueTask InspectAsync(CompactLongValueRecord record, CancellationToken recordToken)
        {
            recordToken.ThrowIfCancellationRequested();
            if (record.DeclaredPayloadBytes < 0 || record.DeclaredPayloadBytes > payload.Length - offset
                || record.PayloadOffset < 0 || record.PayloadOffset > record.RawBytes.Length
                || record.DeclaredPayloadBytes > record.RawBytes.Length - record.PayloadOffset)
            {
                throw Corrupt(field.Location.PageNumber, "An indexed Memo audit delivered bytes outside the admitted payload or borrowed row.");
            }

            record.RawBytes.Slice(record.PayloadOffset, record.DeclaredPayloadBytes).CopyTo(payload.AsMemory(offset));
            offset += record.DeclaredPayloadBytes;
            terminal |= record.Kind == "lval-terminal";
            return default;
        }
    }

    private static string DecodeTextField(JetFormat format, CompactRawField field, byte[] bytes, bool? compressedStorage = null)
    {
        if (format.IsJet3)
        {
            string text = format.DecodeText(bytes, 0, bytes.Length);
            try
            {
                if (!format.EncodeText(text).AsSpan().SequenceEqual(bytes))
                {
                    throw Unsupported(field.Location.PageNumber, "The indexed ANSI text does not have a lossless supported scalar decoding.");
                }
            }
            catch (EncoderFallbackException)
            {
                throw Unsupported(field.Location.PageNumber, "The indexed ANSI text cannot be re-encoded without replacement.");
            }

            return text;
        }

        bool compressed = (compressedStorage ?? (field.Column.IsCompressedUnicode && !field.Column.IsFixed)) && bytes.Length >= 2 && bytes[0] == 0xFF && bytes[1] == 0xFE;
        if (!compressed)
        {
            if (bytes.Length % 2 != 0)
            {
                throw Corrupt(field.Location.PageNumber, "Indexed UCS-2 text contains an incomplete code unit.");
            }

            // Preserve UCS-2 code units instead of applying Unicode replacement fallback.
            return JetFormat.DecodeUtf16LE(bytes);
        }

        bool singleByte = true;
        for (int offset = 2; offset < bytes.Length; offset++)
        {
            if (bytes[offset] == 0)
            {
                singleByte = !singleByte;
            }
            else if (!singleByte && ++offset >= bytes.Length)
            {
                throw Corrupt(field.Location.PageNumber, "Indexed compressed text contains an incomplete UCS-2 code unit.");
            }
        }

        return format.DecodeText(bytes, 0, bytes.Length);
    }

    private static JetCorruptDataException Corrupt(long page, string reason)
        => new(JetErrorCode.CorruptIndex, reason, new JetErrorInfo { PageNumber = page, Reason = reason });

    private static JetLimitationException Limit(long page, string reason)
        => new(JetErrorCode.ValueTooLarge, reason, new JetErrorInfo { PageNumber = page, Reason = reason });

    private static JetNotSupportedException Unsupported(long page, string reason)
        => new(JetErrorCode.FeatureNotSupported, reason, new JetErrorInfo { PageNumber = page, Reason = reason });

    private sealed class KeyEvidence(RowLocation location, byte[] key, IDisposable? reservation)
    {
        internal RowLocation Location { get; } = location;

        internal byte[] Key { get; } = key;

        internal IDisposable? Reservation { get; } = reservation;

        internal bool Observed { get; set; }
    }
}
