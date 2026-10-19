namespace JetDatabaseWriter.Maintenance;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;

/// <summary>Reads only matching catalog properties through bounded raw row and complete LVAL scans.</summary>
/// <param name="format">The verified format.</param>
/// <param name="budget">The session limits.</param>
/// <param name="accounting">The persistent session accounting.</param>
/// <param name="scanRows">Scans intact-owner and independently accepted recovered raw catalog rows using the same session.</param>
/// <param name="auditValue">Audits every defined property-chain reference using the same session.</param>
internal sealed class CompactCalculatedColumnHydrator(
    JetFormat format,
    CompactDiscoveryBudget budget,
    CompactDiscoveryAccounting accounting,
    Func<long, Func<RowLocation, ReadOnlyMemory<byte>, CancellationToken, ValueTask>, CancellationToken, ValueTask> scanRows,
    Func<ReadOnlyMemory<byte>, Func<CompactLongValueRecord, CancellationToken, ValueTask>, CancellationToken, ValueTask> auditValue) : IDisposable
{
    private TDefImage? catalog;
    private IDisposable? catalogStorage;

    /// <summary>Retains the catalog definition, without retaining any catalog rows or property payloads.</summary>
    /// <param name="image">The inventory's verified catalog definition.</param>
    internal void SetCatalog(TDefImage image)
    {
        this.catalogStorage?.Dispose();
        this.catalogStorage = accounting.ReserveWorkingBytes((16L * image.LogicalByteLength) + 65_536, 2);
        this.catalog = image;
    }

    /// <summary>Visits a table with hydrated columns while their storage reservation remains held.</summary>
    /// <param name="root">The original table identity, matched against the complete signed catalog Id.</param>
    /// <param name="image">The original immutable definition.</param>
    /// <param name="visitor">Consumes columns while their admission remains active.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task completed after the consumer returns.</returns>
    /// <exception cref="JetCorruptDataException">Required catalog identities or matching property storage are malformed.</exception>
    internal async ValueTask WithColumnsAsync(long root, TDefImage image, Func<IReadOnlyList<ColumnInfo>?, CancellationToken, ValueTask> visitor, CancellationToken cancellationToken)
    {
        bool calculated = false;
        foreach (ColumnInfo column in image.Columns)
        {
            cancellationToken.ThrowIfCancellationRequested();
            calculated |= column.IsCalculated;
        }

        if (!calculated)
        {
            await visitor(null, cancellationToken).ConfigureAwait(false);
            return;
        }

        TDefImage definition = this.catalog ?? throw Corrupt(root, "The catalog definition is unavailable for calculated ResultType discovery.");
        using IDisposable columnsStorage = accounting.ReserveWorkingBytes((1024L * image.Columns.Count) + 65_536, root);
        var columns = new ColumnInfo[image.Columns.Count];
        for (int index = 0; index < columns.Length; index++)
        {
            columns[index] = image.Columns[index];
        }

        bool matched = false;
        await scanRows(
            2,
            async (location, bytes, token) =>
            {
                CompactRawField id = default;
                CompactRawField property = default;
                await CompactRawFieldReader.ScanAsync(
                    format,
                    definition,
                    location,
                    bytes,
                    (field, fieldToken) =>
                    {
                        fieldToken.ThrowIfCancellationRequested();
                        accounting.Charge(CompactDiscoveryCharge.GraphSteps, 1, location.PageNumber);
                        if (field.Column.Name.Equals("Id", StringComparison.OrdinalIgnoreCase))
                        {
                            id = field;
                        }
                        else if (field.Column.Name.Equals("LvProp", StringComparison.OrdinalIgnoreCase))
                        {
                            property = field;
                        }

                        return default;
                    },
                    token).ConfigureAwait(false);
                if (id.Column is null || id.Kind != CompactRawFieldKind.Stored || id.Column.Type != ColumnType.LongIntegerType || id.RawBytes.Length != sizeof(int))
                {
                    throw Corrupt(location.DataPageNumber, "The catalog Id is not an assigned stored Int32.");
                }

                if (BinaryPrimitives.ReadInt32LittleEndian(id.RawBytes.Span) != root)
                {
                    return;
                }

                if (matched)
                {
                    throw Corrupt(location.DataPageNumber, "Several catalog rows have the same calculated table Id.");
                }

                matched = true;
                if (property.Column is null || property.Kind is CompactRawFieldKind.Absent or CompactRawFieldKind.Null)
                {
                    return;
                }

                if (property.Kind != CompactRawFieldKind.Stored || !property.IsLongValue)
                {
                    throw Corrupt(location.DataPageNumber, "The matching catalog LvProp is not stored MEMO/OLE data.");
                }

                await this.ReadPropertiesAsync(root, columns, property, token).ConfigureAwait(false);
            },
            cancellationToken).ConfigureAwait(false);
        await visitor(columns, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        this.catalog = null;
        this.catalogStorage?.Dispose();
        this.catalogStorage = null;
    }

    private static JetCorruptDataException Corrupt(long page, string reason)
        => new(JetErrorCode.CorruptCatalog, reason, new JetErrorInfo { TableName = "MSysObjects", ColumnName = "LvProp", PageNumber = page, Reason = reason });

    private static ColumnType ReadResultType(ColumnPropertyEntry entry)
    {
        int value = 0;
        if (entry.DataType == ColumnType.ByteType && entry.Value.Length == sizeof(byte))
        {
            value = entry.Value[0];
        }
        else if (entry.DataType == ColumnType.IntegerType && entry.Value.Length == sizeof(short))
        {
            value = BinaryPrimitives.ReadInt16LittleEndian(entry.Value);
        }
        else if (entry.DataType == ColumnType.LongIntegerType && entry.Value.Length == sizeof(int))
        {
            value = BinaryPrimitives.ReadInt32LittleEndian(entry.Value);
        }

        if (value is <= 0 or > byte.MaxValue)
        {
            return default;
        }

        var type = (ColumnType)value;
        return JetTypeInfo.GetMetadata(type) is null ? default : type;
    }

    private ColumnType ResolveResultType(ColumnPropertyBlock? properties, string columnName, long root, CancellationToken cancellationToken)
    {
        if (properties is null)
        {
            return default;
        }

        ColumnType result = default;
        bool consistent = true;
        foreach (ColumnPropertyTarget target in properties.Targets)
        {
            cancellationToken.ThrowIfCancellationRequested();
            accounting.Charge(CompactDiscoveryCharge.GraphSteps, 1, root);
            if (!target.Name.Equals(columnName, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            foreach (ColumnPropertyEntry entry in target.Entries)
            {
                cancellationToken.ThrowIfCancellationRequested();
                accounting.Charge(CompactDiscoveryCharge.GraphSteps, 1, root);
                if (!entry.Name.Equals(Constants.ColumnPropertyNames.ResultType, StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                ColumnType candidate = ReadResultType(entry);
                if (candidate == default || (result != default && result != candidate))
                {
                    consistent = false;
                }
                else
                {
                    result = candidate;
                }
            }
        }

        return consistent ? result : default;
    }

    private async ValueTask ReadPropertiesAsync(long root, ColumnInfo[] columns, CompactRawField property, CancellationToken cancellationToken)
    {
        if (!LongValueDescriptor.TryRead(property.RawBytes.Span, out LongValueDescriptor descriptor))
        {
            throw Corrupt(property.Location.DataPageNumber, "The matching catalog property descriptor is truncated.");
        }

        if (descriptor.Length > Math.Min(budget.MaxDefinitionBytes, budget.MaxValueBytes))
        {
            const string reason = "The calculated catalog property payload exceeds the per-definition or per-value byte allowance.";
            throw new JetLimitationException(JetErrorCode.ValueTooLarge, reason, new JetErrorInfo { PageNumber = root, Reason = reason });
        }

        // Small property entries and empty name/target chunks can amplify collection
        // overhead. Admit parser objects, string capacity and simultaneous arrays
        // conservatively before allocating the payload; no parsed block is retained.
        accounting.Charge(CompactDiscoveryCharge.SchemaBytes, descriptor.Length, root);
        accounting.Charge(CompactDiscoveryCharge.LongValuePayloadBytes, descriptor.Length, root);
        using IDisposable propertyStorage = accounting.ReserveWorkingBytes((128L * descriptor.Length) + 65_536, root);
        byte[] payload = new byte[descriptor.Length];
        int offset = 0;
        await auditValue(
            property.RawBytes,
            (record, token) =>
            {
                token.ThrowIfCancellationRequested();
                if (record.DeclaredPayloadBytes < 0 || record.DeclaredPayloadBytes > payload.Length - offset
                    || record.PayloadOffset < 0 || record.PayloadOffset > record.RawBytes.Length
                    || record.DeclaredPayloadBytes > record.RawBytes.Length - record.PayloadOffset)
                {
                    throw Corrupt(property.Location.DataPageNumber, "The property audit returned payload bytes outside the admitted destination or borrowed slice.");
                }

                if (record.DeclaredPayloadBytes > 0)
                {
                    record.RawBytes.Slice(record.PayloadOffset, record.DeclaredPayloadBytes).CopyTo(payload.AsMemory(offset));
                    offset += record.DeclaredPayloadBytes;
                }

                return default;
            },
            cancellationToken).ConfigureAwait(false);
        if (offset != payload.Length)
        {
            throw Corrupt(property.Location.DataPageNumber, "The matching catalog property payload is incomplete.");
        }

        cancellationToken.ThrowIfCancellationRequested();
        var properties = ColumnPropertyBlock.Parse(payload, format);
        for (int index = 0; index < columns.Length; index++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            ColumnInfo column = columns[index];
            if (column.IsCalculated)
            {
                columns[index] = column.WithCalculatedResultType(this.ResolveResultType(properties, column.Name, root, cancellationToken));
            }
        }
    }
}
