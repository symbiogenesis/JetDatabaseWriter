namespace JetDatabaseWriter.Schema;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Text;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Models;

internal static class LinkedOdbcLvPropBuilder
{
    private const string AggregateType = "AggregateType";
    private const string ColumnHidden = "ColumnHidden";
    private const string ColumnOrder = "ColumnOrder";
    private const string ColumnWidth = "ColumnWidth";
    private const string CurrencyLcid = "CurrencyLCID";
    private const string DefaultView = "DefaultView";
    private const string DisplayControl = "DisplayControl";
    private const string DisplayViewsOnSharePointSite = "DisplayViewsOnSharePointSite";
    private const string FilterOnLoad = "FilterOnLoad";
    private const string GuidProperty = "GUID";
    private const string HideNewField = "HideNewField";
    private const string ImeMode = "IMEMode";
    private const string ImeSentenceMode = "IMESentenceMode";
    private const string NameMap = "NameMap";
    private const string OrderByOn = "OrderByOn";
    private const string OrderByOnLoad = "OrderByOnLoad";
    private const string Orientation = "Orientation";
    private const string ReadOnlyWhenDisconnected = "ReadOnlyWhenDisconnected";
    private const string TextAlign = "TextAlign";
    private const string TotalsRow = "TotalsRow";
    private const string UnicodeCompression = "UnicodeCompression";

    internal static byte[] Build(string foreignTableName, IReadOnlyList<ColumnDefinition>? sourceColumns, DatabaseFormat format)
    {
        Guard.NotNullOrEmpty(foreignTableName, nameof(foreignTableName));
        string sourceTableName = GetUnqualifiedSourceName(foreignTableName);
        List<ColumnIdentity> columns = CreateColumnIdentities(sourceColumns);
        var tableGuid = Guid.NewGuid();

        var builder = new ColumnPropertyBlockBuilder();
        var tableTarget = new ColumnPropertyTargetBuilder
        {
            Name = string.Empty,
            ChunkType = ColumnPropertyChunkType.PropertyBlock,
        };

        AddBinary(tableTarget, GuidProperty, tableGuid.ToByteArray(), ddlFlag: 0x01);
        AddByte(tableTarget, Orientation, 0, ddlFlag: 0x01);
        AddBoolean(tableTarget, OrderByOn, false, ddlFlag: 0x00);
        AddOle(tableTarget, NameMap, BuildNameMap(sourceTableName, tableGuid, columns, format), ddlFlag: 0x00);
        AddByte(tableTarget, DefaultView, 2, ddlFlag: 0x01);
        AddByte(tableTarget, DisplayViewsOnSharePointSite, 1, ddlFlag: 0x01);
        AddBoolean(tableTarget, TotalsRow, false, ddlFlag: 0x01);
        AddBoolean(tableTarget, FilterOnLoad, false, ddlFlag: 0x01);
        AddBoolean(tableTarget, OrderByOnLoad, true, ddlFlag: 0x01);
        AddBoolean(tableTarget, HideNewField, false, ddlFlag: 0x01);
        AddBoolean(tableTarget, ReadOnlyWhenDisconnected, false, ddlFlag: 0x01);
        builder.Targets.Add(tableTarget);

        for (int ordinal = 0; ordinal < columns.Count; ordinal++)
        {
            AddColumnTarget(builder, columns[ordinal], ordinal);
        }

        return builder.ToBytes(format)!;
    }

    internal static void ValidateSourceColumns(IReadOnlyList<ColumnDefinition> sourceColumns, string paramName)
    {
        Guard.NotNull(sourceColumns, paramName);
        if (sourceColumns.Count == 0)
        {
            throw new ArgumentException("At least one source column is required to generate a linked ODBC schema cache.", paramName);
        }

        _ = CreateColumnIdentities(sourceColumns);
    }

    private static List<ColumnIdentity> CreateColumnIdentities(IReadOnlyList<ColumnDefinition>? sourceColumns)
    {
        if (sourceColumns is null || sourceColumns.Count == 0)
        {
            return [];
        }

        var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var identities = new List<ColumnIdentity>(sourceColumns.Count);
        foreach (ColumnDefinition column in sourceColumns)
        {
            Guard.NotNull(column, nameof(sourceColumns));
            Guard.NotNullOrEmpty(column.Name, nameof(sourceColumns));
            if (!names.Add(column.Name))
            {
                throw new ArgumentException($"Duplicate source column name '{column.Name}'.", nameof(sourceColumns));
            }

            ColumnType typeCode = JetTypeInfo.TypeCodeFromDefinition(column);
            identities.Add(new ColumnIdentity(column, typeCode, Guid.NewGuid()));
        }

        return identities;
    }

    private static void AddColumnTarget(ColumnPropertyBlockBuilder builder, ColumnIdentity identity, int ordinal)
    {
        ColumnDefinition column = identity.Column;
        var target = new ColumnPropertyTargetBuilder
        {
            Name = column.Name,
            ChunkType = ColumnPropertyChunkType.PropertyBlockAlt1,
        };

        if (column.IsPrimaryKey || (!column.IsAutoIncrement && !column.IsNullable))
        {
            AddBoolean(target, Constants.ColumnPropertyNames.Required, true, ddlFlag: 0x01);
        }

        if (IsTextLikeColumn(column))
        {
            AddBoolean(target, Constants.ColumnPropertyNames.AllowZeroLength, true, ddlFlag: 0x01);
        }

        AddBinary(target, GuidProperty, identity.Guid.ToByteArray(), ddlFlag: 0x81);
        AddInteger32(target, ColumnWidth, -1, ddlFlag: 0x80);
        AddInteger32(target, ColumnOrder, ordinal, ddlFlag: 0x80);
        AddBoolean(target, ColumnHidden, false, ddlFlag: 0x80);

        if (IsTextLikeColumn(column))
        {
            AddInteger16(target, DisplayControl, 109, ddlFlag: 0x81);
            AddByte(target, ImeMode, 0, ddlFlag: 0x81);
            AddByte(target, ImeSentenceMode, 3, ddlFlag: 0x81);
            AddBoolean(target, UnicodeCompression, false, ddlFlag: 0x81);
        }

        AddByte(target, TextAlign, 0, ddlFlag: 0x80);
        AddLong(target, AggregateType, -1, ddlFlag: 0x80);
        AddByte(target, Constants.ColumnPropertyNames.ResultType, 0, ddlFlag: 0x81);
        AddLong(target, CurrencyLcid, 0, ddlFlag: 0x80);
        builder.Targets.Add(target);
    }

    private static byte[] BuildNameMap(
        string sourceTableName,
        Guid tableGuid,
        List<ColumnIdentity> columns,
        DatabaseFormat format)
    {
        Encoding encoding = format == DatabaseFormat.Jet3Mdb ? Encoding.GetEncoding(1252) : Encoding.Unicode;
        using var stream = new MemoryStream();

        WriteUInt32(stream, 0x550E_CC0A);
        WriteUInt32(stream, 0);
        WriteGuid(stream, tableGuid);
        WriteUInt32(stream, 0);
        WriteGuid(stream, Guid.NewGuid());
        WriteUInt64(stream, 0);
        WriteNameMapString(stream, encoding, sourceTableName);

        foreach (ColumnIdentity column in columns)
        {
            WriteGuid(stream, column.Guid);
            WriteUInt16(stream, 0x0007);
            WriteGuid(stream, tableGuid);
            WriteByte(stream, (byte)column.TypeCode);
            WriteByte(stream, column.Column.IsNullable ? (byte)0x01 : (byte)0x00);
            WriteUInt16(stream, (ushort)Math.Min(Math.Max(column.Column.MaxLength, 0), ushort.MaxValue));
            WriteNameMapString(stream, encoding, column.Column.Name);
        }

        WriteUInt32(stream, 0);
        return stream.ToArray();
    }

    private static void WriteNameMapString(Stream stream, Encoding encoding, string value)
    {
        byte[] encoded = encoding.GetBytes(value);
        stream.Write(encoded, 0, encoded.Length);
        if (encoding == Encoding.Unicode)
        {
            WriteUInt16(stream, 0);
            WriteUInt16(stream, 0);
            WriteUInt16(stream, 0);
        }
        else
        {
            WriteByte(stream, 0);
            WriteByte(stream, 0);
            WriteByte(stream, 0);
        }
    }

    private static string GetUnqualifiedSourceName(string foreignTableName)
    {
        int dot = foreignTableName.LastIndexOf('.');
        string name = dot >= 0 && dot + 1 < foreignTableName.Length
            ? foreignTableName[(dot + 1)..]
            : foreignTableName;

        return name.Length >= 2 && name[0] == '[' && name[^1] == ']'
            ? name[1..^1]
            : name;
    }

    private static bool IsTextLikeColumn(ColumnDefinition column) =>
        column.ClrType == typeof(string) || column.ClrType == typeof(Hyperlink);

    private static void AddBoolean(
        ColumnPropertyTargetBuilder target,
        string propertyName,
        bool value,
        byte ddlFlag) =>
        AddEntry(
            target,
            propertyName,
            ColumnType.BooleanType,
            ddlFlag,
            [value ? (byte)0xFF : (byte)0x00]);

    private static void AddByte(
        ColumnPropertyTargetBuilder target,
        string propertyName,
        byte value,
        byte ddlFlag) =>
        AddEntry(target, propertyName, ColumnType.ByteType, ddlFlag, [value]);

    private static void AddInteger16(
        ColumnPropertyTargetBuilder target,
        string propertyName,
        short value,
        byte ddlFlag)
    {
        byte[] bytes = new byte[sizeof(short)];
        BinaryPrimitives.WriteInt16LittleEndian(bytes, value);
        AddEntry(target, propertyName, ColumnType.IntegerType, ddlFlag, bytes);
    }

    private static void AddInteger32(
        ColumnPropertyTargetBuilder target,
        string propertyName,
        int value,
        byte ddlFlag)
    {
        byte[] bytes = new byte[sizeof(int)];
        BinaryPrimitives.WriteInt32LittleEndian(bytes, value);
        AddEntry(target, propertyName, ColumnType.IntegerType, ddlFlag, bytes);
    }

    private static void AddLong(
        ColumnPropertyTargetBuilder target,
        string propertyName,
        int value,
        byte ddlFlag)
    {
        byte[] bytes = new byte[sizeof(int)];
        BinaryPrimitives.WriteInt32LittleEndian(bytes, value);
        AddEntry(target, propertyName, ColumnType.LongIntegerType, ddlFlag, bytes);
    }

    private static void AddBinary(
        ColumnPropertyTargetBuilder target,
        string propertyName,
        byte[] value,
        byte ddlFlag) =>
        AddEntry(target, propertyName, ColumnType.BinaryType, ddlFlag, value);

    private static void AddOle(
        ColumnPropertyTargetBuilder target,
        string propertyName,
        byte[] value,
        byte ddlFlag) =>
        AddEntry(target, propertyName, ColumnType.OleType, ddlFlag, value);

    private static void AddEntry(
        ColumnPropertyTargetBuilder target,
        string propertyName,
        ColumnType dataType,
        byte ddlFlag,
        byte[] value) =>
        target.Entries.Add(new ColumnPropertyEntryBuilder
        {
            Name = propertyName,
            DataType = dataType,
            DdlFlag = ddlFlag,
            Value = value,
        });

    private static void WriteGuid(Stream stream, Guid value)
    {
        byte[] bytes = value.ToByteArray();
        stream.Write(bytes, 0, bytes.Length);
    }

    private static void WriteUInt16(Stream stream, ushort value)
    {
        Span<byte> bytes = stackalloc byte[sizeof(ushort)];
        BinaryPrimitives.WriteUInt16LittleEndian(bytes, value);
        stream.Write(bytes);
    }

    private static void WriteUInt32(Stream stream, uint value)
    {
        Span<byte> bytes = stackalloc byte[sizeof(uint)];
        BinaryPrimitives.WriteUInt32LittleEndian(bytes, value);
        stream.Write(bytes);
    }

    private static void WriteUInt64(Stream stream, ulong value)
    {
        Span<byte> bytes = stackalloc byte[sizeof(ulong)];
        BinaryPrimitives.WriteUInt64LittleEndian(bytes, value);
        stream.Write(bytes);
    }

    private static void WriteByte(Stream stream, byte value) =>
        stream.WriteByte(value);

    private sealed record ColumnIdentity(ColumnDefinition Column, ColumnType TypeCode, Guid Guid);
}
