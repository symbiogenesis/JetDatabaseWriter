namespace JetDatabaseReader;

using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;

#pragma warning disable SA1204 // Static members should appear before non-static members
#pragma warning disable CA1822 // Mark members as static

/// <summary>
/// Pure-managed writer for Microsoft Access JET databases (.mdb / .accdb).
/// Supports creating tables, inserting, updating, and deleting rows.
/// </summary>
public sealed class AccessWriter : IAccessWriter
{
    private const byte T_BOOL = 0x01;
    private const byte T_BYTE = 0x02;
    private const byte T_INT = 0x03;
    private const byte T_LONG = 0x04;
    private const byte T_MONEY = 0x05;
    private const byte T_FLOAT = 0x06;
    private const byte T_DOUBLE = 0x07;
    private const byte T_DATETIME = 0x08;
    private const byte T_BINARY = 0x09;
    private const byte T_TEXT = 0x0A;
    private const byte T_OLE = 0x0B;
    private const byte T_MEMO = 0x0C;
    private const byte T_GUID = 0x0F;
    private const byte T_NUMERIC = 0x10;

    private const int ObjTable = 1;
    private const uint SysTableMask = 0x80000002U;
    private const int MaxInlineMemoBytes = 1024;
    private const int MaxInlineOleBytes = 256;

    private readonly FileStream _fs;
    private readonly string _path;
    private readonly bool _jet4;
    private readonly Encoding _ansiEncoding;
    private readonly int _pgSz;
    private readonly int _dpTDefOff;
    private readonly int _dpNumRows;
    private readonly int _dpRowsStart;
    private readonly int _tdNumCols;
    private readonly int _tdNumRealIdx;
    private readonly int _tdBlockEnd;
    private readonly int _colDescSz;
    private readonly int _colTypeOff;
    private readonly int _colVarOff;
    private readonly int _colFixedOff;
    private readonly int _colSzOff;
    private readonly int _colFlagsOff;
    private readonly int _colNumOff;
    private readonly int _realIdxEntrySz;
    private readonly int _numColsFldSz;
    private readonly int _varEntrySz;
    private readonly int _eodFldSz;
    private readonly int _varLenFldSz;

    private List<CatalogEntry>? _catalogCache;
    private bool _disposed;

    static AccessWriter()
    {
        Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
    }

    private AccessWriter(string path, FileStream fs)
    {
        _path = path;
        _fs = fs;

        var hdr = new byte[0x80];
        _ = _fs.Seek(0, SeekOrigin.Begin);
        _ = _fs.Read(hdr, 0, hdr.Length);

        byte ver = hdr[0x14];
        _jet4 = ver >= 1;
        _pgSz = _jet4 ? 4096 : 2048;

        int cpOffset = _jet4 ? 0x3C : 0x3A;
        int sortOrder = (hdr.Length > cpOffset + 1) ? Ru16(hdr, cpOffset) : 0;
        int codePage = (sortOrder >> 8) & 0xFF;
        if (codePage == 0)
        {
            codePage = 1252;
        }

        try
        {
            _ansiEncoding = Encoding.GetEncoding(codePage);
        }
        catch (ArgumentException)
        {
            _ansiEncoding = Encoding.UTF8;
        }
        catch (NotSupportedException)
        {
            _ansiEncoding = Encoding.UTF8;
        }

        if (_jet4)
        {
            _dpTDefOff = 4;
            _dpNumRows = 12;
            _dpRowsStart = 14;
            _tdNumCols = 45;
            _tdNumRealIdx = 51;
            _tdBlockEnd = 63;
            _colDescSz = 25;
            _colTypeOff = 0;
            _colVarOff = 7;
            _colFixedOff = 21;
            _colSzOff = 23;
            _colFlagsOff = 15;
            _colNumOff = 5;
            _realIdxEntrySz = 12;
            _numColsFldSz = 2;
            _varEntrySz = 2;
            _eodFldSz = 2;
            _varLenFldSz = 2;
        }
        else
        {
            _dpTDefOff = 4;
            _dpNumRows = 8;
            _dpRowsStart = 10;
            _tdNumCols = 25;
            _tdNumRealIdx = 31;
            _tdBlockEnd = 43;
            _colDescSz = 18;
            _colTypeOff = 0;
            _colVarOff = 3;
            _colFixedOff = 14;
            _colSzOff = 16;
            _colFlagsOff = 13;
            _colNumOff = 1;
            _realIdxEntrySz = 8;
            _numColsFldSz = 1;
            _varEntrySz = 1;
            _eodFldSz = 1;
            _varLenFldSz = 1;
        }
    }

    /// <summary>
    /// Opens a JET database file for writing and returns a new AccessWriter instance.
    /// </summary>
    /// <param name="path">Path to the .mdb or .accdb file.</param>
    /// <returns>An AccessWriter instance for the specified database.</returns>
    public static AccessWriter Open(string path)
    {
        Guard.NotNullOrEmpty(path, nameof(path));

        if (!File.Exists(path))
        {
            throw new FileNotFoundException($"Database file not found: {path}", path);
        }

        var fs = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.Read);
        return new AccessWriter(path, fs);
    }

    /// <inheritdoc/>
    public void CreateTable(string tableName, IReadOnlyList<ColumnDefinition> columns)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNull(columns, nameof(columns));
        Guard.NotDisposed(_disposed, nameof(AccessWriter));

        EnsureJet4WriteSupported();

        if (columns.Count == 0)
        {
            throw new ArgumentException("At least one column is required", nameof(columns));
        }

        if (GetCatalogEntry(tableName) != null)
        {
            throw new InvalidOperationException($"Table '{tableName}' already exists.");
        }

        TableDef tableDef = BuildTableDefinition(columns);
        byte[] tdefPage = BuildTDefPage(tableDef);
        long tdefPageNumber = AppendPage(tdefPage);

        InsertCatalogEntry(tableName, tdefPageNumber);
        InvalidateCatalogCache();
    }

    /// <inheritdoc/>
    public void DropTable(string tableName)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotDisposed(_disposed, nameof(AccessWriter));

        EnsureJet4WriteSupported();

        TableDef? msys = ReadTableDef(2);
        if (msys == null)
        {
            return;
        }

        int deleted = 0;
        foreach (CatalogRow row in EnumerateCatalogRows(msys))
        {
            if (!string.Equals(row.Name, tableName, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            if (row.ObjectType != ObjTable)
            {
                continue;
            }

            if ((unchecked((uint)row.Flags) & SysTableMask) != 0)
            {
                continue;
            }

            MarkRowDeleted(row.PageNumber, row.RowIndex);
            deleted++;
        }

        if (deleted > 0)
        {
            InvalidateCatalogCache();
        }
    }

    /// <inheritdoc/>
    public void InsertRow(string tableName, object[] values)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNull(values, nameof(values));
        Guard.NotDisposed(_disposed, nameof(AccessWriter));

        EnsureJet4WriteSupported();

        CatalogEntry entry = GetRequiredCatalogEntry(tableName);
        TableDef tableDef = ReadRequiredTableDef(entry.TDefPage, tableName);
        InsertRowInternal(entry.TDefPage, tableDef, values);
    }

    /// <inheritdoc/>
    public int InsertRows(string tableName, IEnumerable<object[]> rows)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNull(rows, nameof(rows));
        Guard.NotDisposed(_disposed, nameof(AccessWriter));

        EnsureJet4WriteSupported();

        CatalogEntry entry = GetRequiredCatalogEntry(tableName);
        TableDef tableDef = ReadRequiredTableDef(entry.TDefPage, tableName);
        int inserted = 0;

        foreach (object[] row in rows)
        {
            Guard.NotNull(row, nameof(rows));
            InsertRowInternal(entry.TDefPage, tableDef, row);
            inserted++;
        }

        return inserted;
    }

    /// <inheritdoc/>
    public void InsertRow<T>(string tableName, T item)
        where T : class, new()
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNull(item, nameof(item));
        Guard.NotDisposed(_disposed, nameof(AccessWriter));

        EnsureJet4WriteSupported();

        CatalogEntry entry = GetRequiredCatalogEntry(tableName);
        TableDef tableDef = ReadRequiredTableDef(entry.TDefPage, tableName);
        var headers = tableDef.Columns.ConvertAll(c => c.Name);
        var index = RowMapper<T>.BuildIndex(headers);
        InsertRowInternal(entry.TDefPage, tableDef, RowMapper<T>.ToRow(item, index));
    }

    /// <inheritdoc/>
    public int InsertRows<T>(string tableName, IEnumerable<T> items)
        where T : class, new()
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNull(items, nameof(items));
        Guard.NotDisposed(_disposed, nameof(AccessWriter));

        EnsureJet4WriteSupported();

        CatalogEntry entry = GetRequiredCatalogEntry(tableName);
        TableDef tableDef = ReadRequiredTableDef(entry.TDefPage, tableName);
        var headers = tableDef.Columns.ConvertAll(c => c.Name);
        var index = RowMapper<T>.BuildIndex(headers);
        int inserted = 0;

        foreach (T item in items)
        {
            Guard.NotNull(item, nameof(items));
            InsertRowInternal(entry.TDefPage, tableDef, RowMapper<T>.ToRow(item, index));
            inserted++;
        }

        return inserted;
    }

    /// <inheritdoc/>
    public int UpdateRows(string tableName, string predicateColumn, object predicateValue, IDictionary<string, object> updatedValues)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNullOrEmpty(predicateColumn, nameof(predicateColumn));
        Guard.NotNull(updatedValues, nameof(updatedValues));
        Guard.NotDisposed(_disposed, nameof(AccessWriter));

        EnsureJet4WriteSupported();

        if (updatedValues.Count == 0)
        {
            return 0;
        }

        CatalogEntry entry = GetRequiredCatalogEntry(tableName);
        TableDef tableDef = ReadRequiredTableDef(entry.TDefPage, tableName);
        int predicateIndex = FindColumnIndex(tableDef, predicateColumn);
        if (predicateIndex < 0)
        {
            return 0;
        }

        var updateIndexes = new Dictionary<int, object>();
        foreach (KeyValuePair<string, object> kvp in updatedValues)
        {
            int columnIndex = FindColumnIndex(tableDef, kvp.Key);
            if (columnIndex < 0)
            {
                throw new ArgumentException($"Column '{kvp.Key}' was not found in table '{tableName}'.", nameof(updatedValues));
            }

            updateIndexes[columnIndex] = kvp.Value;
        }

        using (DataTable snapshot = ReadTableSnapshot(tableName))
        {
            List<RowLocation> locations = GetLiveRowLocations(entry.TDefPage);
            int total = Math.Min(snapshot.Rows.Count, locations.Count);
            var replacements = new List<object[]>();

            for (int i = 0; i < total; i++)
            {
                object currentValue = snapshot.Rows[i][predicateIndex];
                if (!ValuesEqual(currentValue, predicateValue))
                {
                    continue;
                }

                object[] rowValues = (object[])snapshot.Rows[i].ItemArray.Clone();
                foreach (KeyValuePair<int, object> update in updateIndexes)
                {
                    rowValues[update.Key] = update.Value ?? DBNull.Value;
                }

                replacements.Add(rowValues);
                MarkRowDeleted(locations[i].PageNumber, locations[i].RowIndex);
            }

            foreach (object[] replacement in replacements)
            {
                InsertRowInternal(entry.TDefPage, tableDef, replacement);
            }

            return replacements.Count;
        }
    }

    /// <inheritdoc/>
    public int DeleteRows(string tableName, string predicateColumn, object predicateValue)
    {
        Guard.NotNullOrEmpty(tableName, nameof(tableName));
        Guard.NotNullOrEmpty(predicateColumn, nameof(predicateColumn));
        Guard.NotDisposed(_disposed, nameof(AccessWriter));

        EnsureJet4WriteSupported();

        CatalogEntry entry = GetRequiredCatalogEntry(tableName);
        TableDef tableDef = ReadRequiredTableDef(entry.TDefPage, tableName);
        int predicateIndex = FindColumnIndex(tableDef, predicateColumn);
        if (predicateIndex < 0)
        {
            return 0;
        }

        using (DataTable snapshot = ReadTableSnapshot(tableName))
        {
            List<RowLocation> locations = GetLiveRowLocations(entry.TDefPage);
            int total = Math.Min(snapshot.Rows.Count, locations.Count);
            int deleted = 0;

            for (int i = 0; i < total; i++)
            {
                object currentValue = snapshot.Rows[i][predicateIndex];
                if (!ValuesEqual(currentValue, predicateValue))
                {
                    continue;
                }

                MarkRowDeleted(locations[i].PageNumber, locations[i].RowIndex);
                deleted++;
            }

            return deleted;
        }
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        try
        {
            _fs?.Dispose();
        }
        finally
        {
            _disposed = true;
        }
    }

    private static ushort Ru16(byte[] b, int o)
    {
        return (ushort)(b[o] | (b[o + 1] << 8));
    }

    private static int Ri32(byte[] b, int o)
    {
        return b[o] | (b[o + 1] << 8) | (b[o + 2] << 16) | (b[o + 3] << 24);
    }

    private static uint Ru32(byte[] b, int o)
    {
        return (uint)Ri32(b, o);
    }

    private static void Wu16(byte[] b, int o, int value)
    {
        b[o] = (byte)(value & 0xFF);
        b[o + 1] = (byte)((value >> 8) & 0xFF);
    }

    private static void Wi32(byte[] b, int o, int value)
    {
        b[o] = (byte)(value & 0xFF);
        b[o + 1] = (byte)((value >> 8) & 0xFF);
        b[o + 2] = (byte)((value >> 16) & 0xFF);
        b[o + 3] = (byte)((value >> 24) & 0xFF);
    }

    private static void WriteUInt24(byte[] b, int o, int value)
    {
        b[o] = (byte)(value & 0xFF);
        b[o + 1] = (byte)((value >> 8) & 0xFF);
        b[o + 2] = (byte)((value >> 16) & 0xFF);
    }

    private static bool ValuesEqual(object left, object right)
    {
        bool leftDbNull = left == null || left is DBNull;
        bool rightDbNull = right == null || right is DBNull;
        if (leftDbNull || rightDbNull)
        {
            return leftDbNull && rightDbNull;
        }

        return object.Equals(left, right);
    }

    private static int FixedSize(byte type, int declaredSize)
    {
        switch (type)
        {
            case T_BYTE: return 1;
            case T_INT: return 2;
            case T_LONG: return 4;
            case T_MONEY: return 8;
            case T_FLOAT: return 4;
            case T_DOUBLE: return 8;
            case T_DATETIME: return 8;
            case T_GUID: return 16;
            case T_NUMERIC: return 17;
            default: return declaredSize > 0 ? declaredSize : 0;
        }
    }

    private static string ReadFixedString(byte[] row, int start, byte type, int size)
    {
        try
        {
            switch (type)
            {
                case T_BYTE:
                    return row[start].ToString(CultureInfo.InvariantCulture);
                case T_INT:
                    return ((short)Ru16(row, start)).ToString(CultureInfo.InvariantCulture);
                case T_LONG:
                    return Ri32(row, start).ToString(CultureInfo.InvariantCulture);
                case T_FLOAT:
                    return BitConverter.ToSingle(row, start).ToString("G", CultureInfo.InvariantCulture);
                case T_DOUBLE:
                    return BitConverter.ToDouble(row, start).ToString("G", CultureInfo.InvariantCulture);
                case T_DATETIME:
                    return DateTime.FromOADate(BitConverter.ToDouble(row, start)).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
                case T_MONEY:
                    return (BitConverter.ToInt64(row, start) / 10000.0m).ToString("F4", CultureInfo.InvariantCulture);
                case T_GUID:
                    return new Guid(ReadGuidBytes(row, start)).ToString("B");
                case T_NUMERIC:
                    return ReadNumericString(row, start);
                default:
                    return BitConverter.ToString(row, start, Math.Min(size, 8));
            }
        }
        catch (ArgumentException)
        {
            return string.Empty;
        }
        catch (IndexOutOfRangeException)
        {
            return string.Empty;
        }
        catch (OverflowException)
        {
            return string.Empty;
        }
    }

    private static byte[] ReadGuidBytes(byte[] b, int start)
    {
        var guidBytes = new byte[16];
        Buffer.BlockCopy(b, start, guidBytes, 0, 16);
        return guidBytes;
    }

    private static string ReadNumericString(byte[] b, int start)
    {
        if (start + 16 >= b.Length)
        {
            return string.Empty;
        }

        byte scale = b[start + 1];
        bool negative = b[start + 2] != 0;
        uint lo = Ru32(b, start + 4);
        uint mid = Ru32(b, start + 8);
        uint hi = Ru32(b, start + 12);

        if (scale > 28)
        {
            return string.Empty;
        }

        try
        {
            return new decimal((int)lo, (int)mid, (int)hi, negative, scale).ToString("G", CultureInfo.InvariantCulture);
        }
        catch (OverflowException)
        {
            return string.Empty;
        }
    }

    private static byte TypeCodeFromDefinition(ColumnDefinition column)
    {
        Type clrType = column.ClrType;
        if (clrType == typeof(bool))
        {
            return T_BOOL;
        }

        if (clrType == typeof(byte))
        {
            return T_BYTE;
        }

        if (clrType == typeof(short))
        {
            return T_INT;
        }

        if (clrType == typeof(int))
        {
            return T_LONG;
        }

        if (clrType == typeof(float))
        {
            return T_FLOAT;
        }

        if (clrType == typeof(double))
        {
            return T_DOUBLE;
        }

        if (clrType == typeof(DateTime))
        {
            return T_DATETIME;
        }

        if (clrType == typeof(decimal))
        {
            return T_NUMERIC;
        }

        if (clrType == typeof(Guid))
        {
            return T_GUID;
        }

        if (clrType == typeof(byte[]))
        {
            return column.MaxLength > 0 && column.MaxLength <= 255 ? T_BINARY : T_OLE;
        }

        if (clrType == typeof(string))
        {
            return column.MaxLength > 0 && column.MaxLength <= 255 ? T_TEXT : T_MEMO;
        }

        throw new NotSupportedException($"CLR type '{clrType}' is not supported for table creation.");
    }

    private static bool IsVariableType(byte type)
    {
        return type == T_TEXT || type == T_BINARY || type == T_MEMO || type == T_OLE;
    }

    private static Type TypeCodeToClrType(byte typeCode)
    {
        switch (typeCode)
        {
            case T_BOOL: return typeof(bool);
            case T_BYTE: return typeof(byte);
            case T_INT: return typeof(short);
            case T_LONG: return typeof(int);
            case T_MONEY: return typeof(decimal);
            case T_FLOAT: return typeof(float);
            case T_DOUBLE: return typeof(double);
            case T_DATETIME: return typeof(DateTime);
            case T_GUID: return typeof(Guid);
            case T_NUMERIC: return typeof(decimal);
            default: return typeof(string);
        }
    }

    private void EnsureJet4WriteSupported()
    {
        if (!_jet4)
        {
            throw new NotSupportedException("Write support currently requires a Jet4/ACE-format database.");
        }
    }

    private DataTable ReadTableSnapshot(string tableName)
    {
        var options = new AccessReaderOptions { FileShare = FileShare.ReadWrite, ValidateOnOpen = false };
        using (var reader = AccessReader.Open(_path, options))
        {
            return reader.ReadTable(tableName) ?? new DataTable(tableName);
        }
    }

    private CatalogEntry GetRequiredCatalogEntry(string tableName)
    {
        CatalogEntry entry = GetCatalogEntry(tableName);
        if (entry == null)
        {
            throw new InvalidOperationException($"Table '{tableName}' was not found.");
        }

        return entry;
    }

    private TableDef ReadRequiredTableDef(long tdefPage, string tableName)
    {
        TableDef? tableDef = ReadTableDef(tdefPage);
        if (tableDef == null)
        {
            throw new InvalidDataException($"Table definition for '{tableName}' could not be read.");
        }

        return tableDef;
    }

    private static int FindColumnIndex(TableDef tableDef, string columnName)
    {
        return tableDef.Columns.FindIndex(c => string.Equals(c.Name, columnName, StringComparison.OrdinalIgnoreCase));
    }

    private void InsertCatalogEntry(string tableName, long tdefPageNumber)
    {
        TableDef msys = ReadRequiredTableDef(2, "MSysObjects");
        var values = new object[msys.Columns.Count];
        DateTime now = DateTime.UtcNow;

        for (int i = 0; i < msys.Columns.Count; i++)
        {
            values[i] = DBNull.Value;
        }

        SetValue(msys, values, "Id", (int)tdefPageNumber);
        SetValue(msys, values, "ParentId", 0);
        SetValue(msys, values, "Name", tableName);
        SetValue(msys, values, "Type", (short)ObjTable);
        SetValue(msys, values, "DateCreate", now);
        SetValue(msys, values, "DateUpdate", now);
        SetValue(msys, values, "Flags", 0);

        InsertRowInternal(2, msys, values);
    }

    private static void SetValue(TableDef tableDef, object[] values, string columnName, object value)
    {
        int index = FindColumnIndex(tableDef, columnName);
        if (index >= 0)
        {
            values[index] = value;
        }
    }

    private static TableDef BuildTableDefinition(IReadOnlyList<ColumnDefinition> columns)
    {
        var result = new TableDef();
        int fixedOffset = 0;
        int nextVarIndex = 0;

        for (int i = 0; i < columns.Count; i++)
        {
            ColumnDefinition definition = columns[i];
            byte type = TypeCodeFromDefinition(definition);
            bool variable = IsVariableType(type);
            int size = GetDeclaredSize(type, definition.MaxLength);

            var column = new ColumnInfo
            {
                Name = definition.Name,
                Type = type,
                ColNum = i,
                VarIdx = variable ? nextVarIndex : 0,
                FixedOff = variable ? 0 : fixedOffset,
                Size = size,
                Flags = (byte)(variable ? 0x02 : 0x03),
            };

            result.Columns.Add(column);

            if (variable)
            {
                nextVarIndex++;
            }
            else
            {
                fixedOffset += FixedSize(type, size);
            }
        }

        return result;
    }

    private static int GetDeclaredSize(byte type, int maxLength)
    {
        switch (type)
        {
            case T_BOOL:
                return 0;
            case T_BYTE:
                return 1;
            case T_INT:
                return 2;
            case T_LONG:
                return 4;
            case T_MONEY:
                return 8;
            case T_FLOAT:
                return 4;
            case T_DOUBLE:
                return 8;
            case T_DATETIME:
                return 8;
            case T_GUID:
                return 16;
            case T_NUMERIC:
                return 17;
            case T_TEXT:
                return Math.Max(2, (maxLength > 0 ? maxLength : 255) * 2);
            case T_BINARY:
                return maxLength > 0 ? maxLength : 255;
            default:
                return 0;
        }
    }

    private byte[] BuildTDefPage(TableDef tableDef)
    {
        byte[] page = new byte[_pgSz];
        int numCols = tableDef.Columns.Count;
        int numVarCols = tableDef.Columns.Count(c => IsVariableType(c.Type));
        int colStart = _tdBlockEnd;
        int namePos = colStart + (numCols * _colDescSz);

        page[0] = 0x02;
        page[1] = 0x01;
        Wi32(page, 4, 0);
        Wi32(page, 16, 0);
        page[40] = 0x4E;
        Wu16(page, 41, numCols);
        Wu16(page, 43, numVarCols);
        Wu16(page, 45, numCols);
        Wi32(page, 47, 0);
        Wi32(page, 51, 0);
        Wi32(page, 55, 0);
        Wi32(page, 59, 0);

        for (int i = 0; i < numCols; i++)
        {
            ColumnInfo col = tableDef.Columns[i];
            int o = colStart + (i * _colDescSz);

            page[o + _colTypeOff] = col.Type;
            Wu16(page, o + _colNumOff, col.ColNum);
            Wu16(page, o + _colVarOff, col.VarIdx);
            page[o + _colFlagsOff] = col.Flags;
            Wu16(page, o + _colFixedOff, col.FixedOff);
            Wu16(page, o + _colSzOff, col.Size);

            byte[] nameBytes = Encoding.Unicode.GetBytes(col.Name);
            if (namePos + 2 + nameBytes.Length > page.Length)
            {
                throw new NotSupportedException("Table definition does not fit within a single TDEF page.");
            }

            Wu16(page, namePos, nameBytes.Length);
            namePos += 2;
            Buffer.BlockCopy(nameBytes, 0, page, namePos, nameBytes.Length);
            namePos += nameBytes.Length;
        }

        Wi32(page, 8, Math.Max(0, namePos - 8));
        return page;
    }

    private void InsertRowInternal(long tdefPage, TableDef tableDef, object[] values)
    {
        if (values.Length != tableDef.Columns.Count)
        {
            throw new ArgumentException(
                $"Expected {tableDef.Columns.Count} values for table row but received {values.Length}.",
                nameof(values));
        }

        byte[] rowBytes = SerializeRow(tableDef, values);
        PageInsertTarget target = FindInsertTarget(tdefPage, rowBytes.Length);
        WriteRowToPage(target.PageNumber, target.Page, rowBytes);
    }

    private PageInsertTarget FindInsertTarget(long tdefPage, int rowLength)
    {
        long total = _fs.Length / _pgSz;
        PageInsertTarget? candidate = null;

        for (long pageNumber = 3; pageNumber < total; pageNumber++)
        {
            byte[] page = ReadPage(pageNumber);
            if (page[0] != 0x01)
            {
                continue;
            }

            if ((long)Ri32(page, _dpTDefOff) != tdefPage)
            {
                continue;
            }

            if (CanInsertRow(page, rowLength))
            {
                candidate = new PageInsertTarget { PageNumber = pageNumber, Page = page };
            }
        }

        if (candidate != null)
        {
            return candidate;
        }

        long newPageNumber = AppendPage(CreateEmptyDataPage(tdefPage));
        return new PageInsertTarget { PageNumber = newPageNumber, Page = ReadPage(newPageNumber) };
    }

    private bool CanInsertRow(byte[] page, int rowLength)
    {
        int numRows = Ru16(page, _dpNumRows);
        int dataStart = GetFirstRowStart(page, numRows);
        int nextOffsetPos = _dpRowsStart + ((numRows + 1) * 2);
        return dataStart - nextOffsetPos >= rowLength;
    }

    private int GetFirstRowStart(byte[] page, int numRows)
    {
        int first = _pgSz;
        for (int i = 0; i < numRows; i++)
        {
            int raw = Ru16(page, _dpRowsStart + (i * 2));
            int start = raw & 0x1FFF;
            if (start > 0 && start < first)
            {
                first = start;
            }
        }

        return first;
    }

    private byte[] CreateEmptyDataPage(long tdefPage)
    {
        byte[] page = new byte[_pgSz];
        page[0] = 0x01;
        page[1] = 0x01;
        Wu16(page, 2, _pgSz - _dpRowsStart);
        Wi32(page, _dpTDefOff, (int)tdefPage);
        Wu16(page, _dpNumRows, 0);
        return page;
    }

    private void WriteRowToPage(long pageNumber, byte[] page, byte[] rowBytes)
    {
        int numRows = Ru16(page, _dpNumRows);
        int firstRowStart = GetFirstRowStart(page, numRows);
        int rowStart = firstRowStart - rowBytes.Length;
        int rowOffsetPos = _dpRowsStart + (numRows * 2);

        Buffer.BlockCopy(rowBytes, 0, page, rowStart, rowBytes.Length);
        Wu16(page, rowOffsetPos, rowStart);
        Wu16(page, _dpNumRows, numRows + 1);

        int freeSpace = rowStart - (_dpRowsStart + ((numRows + 1) * 2));
        if (freeSpace < 0)
        {
            throw new InvalidDataException("Insufficient free space remained on the target page.");
        }

        Wu16(page, 2, freeSpace);
        WritePage(pageNumber, page);
    }

    private byte[] SerializeRow(TableDef tableDef, object[] values)
    {
        int numCols = tableDef.Columns.Count == 0 ? 0 : tableDef.Columns.Max(c => c.ColNum) + 1;
        int nullMaskSize = (numCols + 7) / 8;
        var nullMask = new byte[nullMaskSize];
        var fixedChunks = new List<FixedValueChunk>();
        var variableDataByIndex = new Dictionary<int, byte[]>();
        int fixedAreaSize = 0;
        int maxVarIndex = -1;

        for (int i = 0; i < tableDef.Columns.Count; i++)
        {
            ColumnInfo column = tableDef.Columns[i];
            object value = values[i];
            if (value == null)
            {
                value = DBNull.Value;
            }

            if (column.Type == T_BOOL)
            {
                if (!(value is DBNull) && Convert.ToBoolean(value, CultureInfo.InvariantCulture))
                {
                    SetNullMaskBit(nullMask, column.ColNum, true);
                }

                continue;
            }

            if (value is DBNull)
            {
                continue;
            }

            if (column.IsFixed)
            {
                if (!CanStoreFixedColumn(column))
                {
                    continue;
                }

                byte[]? fixedValue = EncodeFixedValue(column, value);
                if (fixedValue == null)
                {
                    continue;
                }

                fixedChunks.Add(new FixedValueChunk { Offset = column.FixedOff, Bytes = fixedValue });
                fixedAreaSize = Math.Max(fixedAreaSize, column.FixedOff + fixedValue.Length);
                SetNullMaskBit(nullMask, column.ColNum, true);
            }
            else
            {
                byte[]? variableValue = EncodeVariableValue(column, value);
                if (variableValue == null)
                {
                    continue;
                }

                variableDataByIndex[column.VarIdx] = variableValue;
                maxVarIndex = Math.Max(maxVarIndex, column.VarIdx);
                SetNullMaskBit(nullMask, column.ColNum, true);
            }
        }

        byte[] fixedArea = new byte[fixedAreaSize];
        foreach (FixedValueChunk chunk in fixedChunks)
        {
            Buffer.BlockCopy(chunk.Bytes, 0, fixedArea, chunk.Offset, chunk.Bytes.Length);
        }

        int varLen = maxVarIndex + 1;
        var variableOffsets = new int[Math.Max(varLen, 0)];
        var variablePayload = new List<byte[]>();
        int currentOffset = _numColsFldSz + fixedArea.Length;

        for (int varIndex = 0; varIndex < varLen; varIndex++)
        {
            variableOffsets[varIndex] = currentOffset;
            byte[] payload;
            if (variableDataByIndex.TryGetValue(varIndex, out payload))
            {
                variablePayload.Add(payload);
                currentOffset += payload.Length;
            }
        }

        int eod = currentOffset;
        int rowLength = _numColsFldSz + fixedArea.Length + variablePayload.Sum(bytes => bytes.Length) + _eodFldSz + (varLen * _varEntrySz) + _varLenFldSz + nullMask.Length;
        var row = new byte[rowLength];
        int pos = 0;

        Wu16(row, pos, numCols);
        pos += _numColsFldSz;

        if (fixedArea.Length > 0)
        {
            Buffer.BlockCopy(fixedArea, 0, row, pos, fixedArea.Length);
            pos += fixedArea.Length;
        }

        foreach (byte[] payload in variablePayload)
        {
            Buffer.BlockCopy(payload, 0, row, pos, payload.Length);
            pos += payload.Length;
        }

        Wu16(row, pos, eod);
        pos += _eodFldSz;

        for (int varIndex = varLen - 1; varIndex >= 0; varIndex--)
        {
            Wu16(row, pos, variableOffsets[varIndex]);
            pos += _varEntrySz;
        }

        Wu16(row, pos, varLen);
        pos += _varLenFldSz;
        Buffer.BlockCopy(nullMask, 0, row, pos, nullMask.Length);

        return row;
    }

    private bool CanStoreFixedColumn(ColumnInfo column)
    {
        int size = FixedSize(column.Type, column.Size);
        return size >= 0 && column.FixedOff >= 0 && column.FixedOff + size < _pgSz;
    }

    private static void SetNullMaskBit(byte[] mask, int columnNumber, bool state)
    {
        if (columnNumber < 0)
        {
            return;
        }

        int byteIndex = columnNumber / 8;
        int bitIndex = columnNumber % 8;
        if (byteIndex >= mask.Length)
        {
            return;
        }

        if (state)
        {
            mask[byteIndex] = (byte)(mask[byteIndex] | (1 << bitIndex));
        }
    }

    private byte[]? EncodeFixedValue(ColumnInfo column, object value)
    {
        switch (column.Type)
        {
            case T_BYTE:
                return new[] { Convert.ToByte(value, CultureInfo.InvariantCulture) };
            case T_INT:
                return BitConverter.GetBytes(Convert.ToInt16(value, CultureInfo.InvariantCulture));
            case T_LONG:
                return BitConverter.GetBytes(Convert.ToInt32(value, CultureInfo.InvariantCulture));
            case T_FLOAT:
                return BitConverter.GetBytes(Convert.ToSingle(value, CultureInfo.InvariantCulture));
            case T_DOUBLE:
                return BitConverter.GetBytes(Convert.ToDouble(value, CultureInfo.InvariantCulture));
            case T_DATETIME:
                return BitConverter.GetBytes(Convert.ToDateTime(value, CultureInfo.InvariantCulture).ToOADate());
            case T_MONEY:
                decimal currency = Convert.ToDecimal(value, CultureInfo.InvariantCulture);
                long scaledCurrency = decimal.ToInt64(decimal.Round(currency * 10000m, 0, MidpointRounding.AwayFromZero));
                return BitConverter.GetBytes(scaledCurrency);
            case T_NUMERIC:
                return EncodeNumericValue(Convert.ToDecimal(value, CultureInfo.InvariantCulture));
            case T_GUID:
                Guid guid = value is Guid ? (Guid)value : Guid.Parse(Convert.ToString(value, CultureInfo.InvariantCulture));
                return guid.ToByteArray();
            default:
                return null;
        }
    }

    private byte[]? EncodeVariableValue(ColumnInfo column, object value)
    {
        switch (column.Type)
        {
            case T_TEXT:
                return EncodeTextValue(Convert.ToString(value, CultureInfo.InvariantCulture), column.Size);
            case T_BINARY:
                return EncodeBinaryValue(value, column.Size);
            case T_MEMO:
                return EncodeMemoValue(Convert.ToString(value, CultureInfo.InvariantCulture));
            case T_OLE:
                return EncodeOleValue(value);
            default:
                return null;
        }
    }

    private byte[]? EncodeTextValue(string? value, int maxSize)
    {
        if (value == null)
        {
            return null;
        }

        byte[] bytes = _jet4 ? Encoding.Unicode.GetBytes(value) : _ansiEncoding.GetBytes(value);
        if (maxSize > 0 && bytes.Length > maxSize)
        {
            int allowed = _jet4 ? maxSize & ~1 : maxSize;
            if (allowed <= 0)
            {
                return Array.Empty<byte>();
            }

            Array.Resize(ref bytes, allowed);
        }

        return bytes;
    }

    private byte[]? EncodeBinaryValue(object value, int maxSize)
    {
        byte[]? bytes = value as byte[];
        if (bytes == null)
        {
            string? stringValue = Convert.ToString(value, CultureInfo.InvariantCulture);
            if (string.IsNullOrEmpty(stringValue))
            {
                return null;
            }

            bytes = _ansiEncoding.GetBytes(stringValue);
        }

        if (maxSize > 0 && bytes.Length > maxSize)
        {
            Array.Resize(ref bytes, maxSize);
        }

        return bytes;
    }

    private byte[]? EncodeMemoValue(string? value)
    {
        if (value == null)
        {
            return null;
        }

        byte[] data = _jet4 ? Encoding.Unicode.GetBytes(value) : _ansiEncoding.GetBytes(value);
        if (data.Length > MaxInlineMemoBytes)
        {
            return null;
        }

        return WrapInlineLongValue(data);
    }

    private static byte[]? EncodeOleValue(object value)
    {
        byte[]? data = value as byte[];
        if (data == null)
        {
            string? stringValue = value as string;
            if (string.IsNullOrEmpty(stringValue) || stringValue.Length > 128)
            {
                return null;
            }

            data = Encoding.UTF8.GetBytes(stringValue);
        }

        if (data.Length > MaxInlineOleBytes)
        {
            return null;
        }

        return WrapInlineLongValue(data);
    }

    private static byte[]? WrapInlineLongValue(byte[]? data)
    {
        if (data == null)
        {
            return null;
        }

        var buffer = new byte[12 + data.Length];
        WriteUInt24(buffer, 0, data.Length);
        buffer[3] = 0x80;
        Buffer.BlockCopy(data, 0, buffer, 12, data.Length);
        return buffer;
    }

    private byte[] EncodeNumericValue(decimal value)
    {
        int[] bits = decimal.GetBits(value);
        int flags = bits[3];
        bool negative = (flags & unchecked((int)0x80000000)) != 0;
        byte scale = (byte)((flags >> 16) & 0x7F);

        decimal absolute = Math.Abs(value);
        string digits = absolute.ToString(CultureInfo.InvariantCulture).Replace(".", string.Empty, StringComparison.Ordinal);
        if (string.IsNullOrEmpty(digits))
        {
            digits = "0";
        }

        byte precision = (byte)Math.Min(digits.TrimStart('0').Length == 0 ? 1 : digits.TrimStart('0').Length, 28);
        var buffer = new byte[17];
        buffer[0] = precision;
        buffer[1] = scale;
        buffer[2] = negative ? (byte)1 : (byte)0;

        Buffer.BlockCopy(BitConverter.GetBytes(bits[0]), 0, buffer, 4, 4);
        Buffer.BlockCopy(BitConverter.GetBytes(bits[1]), 0, buffer, 8, 4);
        Buffer.BlockCopy(BitConverter.GetBytes(bits[2]), 0, buffer, 12, 4);
        return buffer;
    }

    private byte[] ReadPage(long pageNumber)
    {
        var buffer = new byte[_pgSz];
        _ = _fs.Seek(pageNumber * _pgSz, SeekOrigin.Begin);

        int read = 0;
        while (read < _pgSz)
        {
            int got = _fs.Read(buffer, read, _pgSz - read);
            if (got == 0)
            {
                break;
            }

            read += got;
        }

        return buffer;
    }

    private void WritePage(long pageNumber, byte[] page)
    {
        _ = _fs.Seek(pageNumber * _pgSz, SeekOrigin.Begin);
        _fs.Write(page, 0, page.Length);
        _fs.Flush();
    }

    private long AppendPage(byte[] page)
    {
        long pageNumber = _fs.Length / _pgSz;
        _ = _fs.Seek(pageNumber * _pgSz, SeekOrigin.Begin);
        _fs.Write(page, 0, page.Length);
        _fs.Flush();
        return pageNumber;
    }

    private byte[]? ReadTDefBytes(long startPage)
    {
        var parts = new List<byte[]>();
        var seen = new HashSet<long>();
        long pageNumber = startPage;

        while (pageNumber != 0 && !seen.Contains(pageNumber))
        {
            seen.Add(pageNumber);
            byte[] page = ReadPage(pageNumber);
            if (page[0] != 0x02)
            {
                break;
            }

            parts.Add(page);
            pageNumber = Ru32(page, 4);
        }

        if (parts.Count == 0)
        {
            return null;
        }

        if (parts.Count == 1)
        {
            return parts[0];
        }

        int total = parts[0].Length;
        for (int i = 1; i < parts.Count; i++)
        {
            total += parts[i].Length - 8;
        }

        var result = new byte[total];
        Buffer.BlockCopy(parts[0], 0, result, 0, parts[0].Length);
        int pos = parts[0].Length;

        for (int i = 1; i < parts.Count; i++)
        {
            int len = parts[i].Length - 8;
            Buffer.BlockCopy(parts[i], 8, result, pos, len);
            pos += len;
        }

        return result;
    }

    private TableDef? ReadTableDef(long tdefPage)
    {
        byte[]? td = ReadTDefBytes(tdefPage);
        if (td == null || td.Length < _tdBlockEnd)
        {
            return null;
        }

        int numCols = Ru16(td, _tdNumCols);
        int numRealIdx = Ri32(td, _tdNumRealIdx);
        if (numRealIdx < 0 || numRealIdx > 1000)
        {
            numRealIdx = 0;
        }

        if (numCols < 0 || numCols > 4096)
        {
            return null;
        }

        int colStart = _tdBlockEnd + (numRealIdx * _realIdxEntrySz);
        int namePos = colStart + (numCols * _colDescSz);
        if (namePos > td.Length)
        {
            return null;
        }

        var cols = new List<ColumnInfo>(numCols);
        for (int i = 0; i < numCols; i++)
        {
            int o = colStart + (i * _colDescSz);
            if (o + _colDescSz > td.Length)
            {
                break;
            }

            cols.Add(new ColumnInfo
            {
                Type = td[o + _colTypeOff],
                ColNum = Ru16(td, o + _colNumOff),
                VarIdx = Ru16(td, o + _colVarOff),
                FixedOff = Ru16(td, o + _colFixedOff),
                Size = Ru16(td, o + _colSzOff),
                Flags = td[o + _colFlagsOff],
            });
        }

        for (int i = 0; i < cols.Count; i++)
        {
            if (namePos >= td.Length)
            {
                break;
            }

            if (_jet4)
            {
                if (namePos + 2 > td.Length)
                {
                    break;
                }

                int len = Ru16(td, namePos);
                namePos += 2;
                if (namePos + len > td.Length)
                {
                    break;
                }

                cols[i].Name = Encoding.Unicode.GetString(td, namePos, len);
                namePos += len;
            }
            else
            {
                int len = td[namePos++];
                if (namePos + len > td.Length)
                {
                    break;
                }

                cols[i].Name = _ansiEncoding.GetString(td, namePos, len);
                namePos += len;
            }
        }

        cols.Sort((a, b) => a.ColNum.CompareTo(b.ColNum));
        bool hasDeletedColumns = false;
        for (int i = 1; i < cols.Count; i++)
        {
            if (cols[i].ColNum != cols[i - 1].ColNum + 1)
            {
                hasDeletedColumns = true;
                break;
            }
        }

        return new TableDef
        {
            Columns = cols,
            RowCount = td.Length > 20 ? (long)Ru32(td, 16) : 0,
            HasDeletedColumns = hasDeletedColumns,
        };
    }

    private List<CatalogEntry> GetUserTables()
    {
        if (_catalogCache != null)
        {
            return _catalogCache;
        }

        TableDef? msys = ReadTableDef(2);
        if (msys == null)
        {
            _catalogCache = new List<CatalogEntry>();
            return _catalogCache;
        }

        var result = new List<CatalogEntry>();
        foreach (CatalogRow row in EnumerateCatalogRows(msys))
        {
            if (row.ObjectType != ObjTable)
            {
                continue;
            }

            if ((unchecked((uint)row.Flags) & SysTableMask) != 0)
            {
                continue;
            }

            if (string.IsNullOrEmpty(row.Name) || row.TDefPage <= 0)
            {
                continue;
            }

            result.Add(new CatalogEntry { Name = row.Name, TDefPage = row.TDefPage });
        }

        _catalogCache = result;
        return _catalogCache;
    }

    private CatalogEntry GetCatalogEntry(string tableName)
    {
        return GetUserTables().Find(e => string.Equals(e.Name, tableName, StringComparison.OrdinalIgnoreCase));
    }

    private void InvalidateCatalogCache()
    {
        _catalogCache = null;
    }

    private IEnumerable<CatalogRow> EnumerateCatalogRows(TableDef msys)
    {
        ColumnInfo idColumn = msys.Columns.Find(c => string.Equals(c.Name, "Id", StringComparison.OrdinalIgnoreCase));
        ColumnInfo nameColumn = msys.Columns.Find(c => string.Equals(c.Name, "Name", StringComparison.OrdinalIgnoreCase));
        ColumnInfo typeColumn = msys.Columns.Find(c => string.Equals(c.Name, "Type", StringComparison.OrdinalIgnoreCase));
        ColumnInfo flagsColumn = msys.Columns.Find(c => string.Equals(c.Name, "Flags", StringComparison.OrdinalIgnoreCase));
        if (nameColumn == null || typeColumn == null)
        {
            yield break;
        }

        long total = _fs.Length / _pgSz;
        for (long pageNumber = 3; pageNumber < total; pageNumber++)
        {
            byte[] page = ReadPage(pageNumber);
            if (page[0] != 0x01)
            {
                continue;
            }

            if (Ri32(page, _dpTDefOff) != 2)
            {
                continue;
            }

            foreach (RowLocation row in EnumerateLiveRowLocations(pageNumber, page))
            {
                yield return new CatalogRow
                {
                    PageNumber = row.PageNumber,
                    RowIndex = row.RowIndex,
                    Name = ReadColumnValue(page, row.RowStart, row.RowSize, nameColumn),
                    ObjectType = ParseInt32(ReadColumnValue(page, row.RowStart, row.RowSize, typeColumn)),
                    Flags = ParseInt64(ReadColumnValue(page, row.RowStart, row.RowSize, flagsColumn)),
                    TDefPage = ParseInt64(ReadColumnValue(page, row.RowStart, row.RowSize, idColumn)) & 0x00FFFFFFL,
                };
            }
        }
    }

    private string ReadColumnValue(byte[] page, int rowStart, int rowSize, ColumnInfo column)
    {
        if (column == null || rowSize < _numColsFldSz)
        {
            return string.Empty;
        }

        int numCols = _jet4 ? Ru16(page, rowStart) : page[rowStart];
        if (numCols == 0)
        {
            return string.Empty;
        }

        int nullMaskSz = (numCols + 7) / 8;
        int nullMaskPos = rowSize - nullMaskSz;
        if (nullMaskPos < _numColsFldSz)
        {
            return string.Empty;
        }

        int varLenPos = nullMaskPos - _varLenFldSz;
        if (varLenPos < _numColsFldSz)
        {
            return string.Empty;
        }

        int varLen = _jet4 ? Ru16(page, rowStart + varLenPos) : page[rowStart + varLenPos];
        int jumpSize = _jet4 ? 0 : rowSize / 256;
        int varTableStart = varLenPos - jumpSize - (varLen * _varEntrySz);
        int eodPos = varTableStart - _eodFldSz;
        if (eodPos < _numColsFldSz)
        {
            return string.Empty;
        }

        int eod = _jet4 ? Ru16(page, rowStart + eodPos) : page[rowStart + eodPos];
        bool nullBit = false;
        if (column.ColNum < numCols)
        {
            int mByte = nullMaskPos + (column.ColNum / 8);
            int mBit = column.ColNum % 8;
            if (mByte < rowSize)
            {
                nullBit = (page[rowStart + mByte] & (1 << mBit)) != 0;
            }
        }

        if (column.Type == T_BOOL)
        {
            return nullBit ? "True" : "False";
        }

        if (column.ColNum >= numCols || !nullBit)
        {
            return string.Empty;
        }

        if (column.IsFixed)
        {
            int start = _numColsFldSz + column.FixedOff;
            int size = FixedSize(column.Type, column.Size);
            if (size == 0 || start + size > rowSize)
            {
                return string.Empty;
            }

            return ReadFixedString(page, rowStart + start, column.Type, size);
        }

        if (column.VarIdx >= varLen)
        {
            return string.Empty;
        }

        int entryPos = varTableStart + ((varLen - 1 - column.VarIdx) * _varEntrySz);
        if (entryPos < 0 || entryPos + _varEntrySz > rowSize)
        {
            return string.Empty;
        }

        int varOff = _jet4 ? Ru16(page, rowStart + entryPos) : page[rowStart + entryPos];
        int varEnd;
        if (column.VarIdx + 1 < varLen)
        {
            int nextEntry = varTableStart + ((varLen - 2 - column.VarIdx) * _varEntrySz);
            varEnd = _jet4 ? Ru16(page, rowStart + nextEntry) : page[rowStart + nextEntry];
        }
        else
        {
            varEnd = eod;
        }

        int dataStart = varOff;
        int dataLen = varEnd - varOff;
        if (dataLen <= 0 || dataStart < 0 || dataStart + dataLen > rowSize)
        {
            return string.Empty;
        }

        switch (column.Type)
        {
            case T_TEXT:
                return _jet4 ? DecodeJet4Text(page, rowStart + dataStart, dataLen) : _ansiEncoding.GetString(page, rowStart + dataStart, dataLen);
            case T_BINARY:
                return BitConverter.ToString(page, rowStart + dataStart, dataLen);
            default:
                return string.Empty;
        }
    }

    private string DecodeJet4Text(byte[] bytes, int start, int length)
    {
        if (length < 2)
        {
            return string.Empty;
        }

        if (bytes[start] == 0xFF && bytes[start + 1] == 0xFE)
        {
            return DecompressJet4(bytes, start + 2, length - 2);
        }

        int evenLength = length & ~1;
        return evenLength > 0 ? Encoding.Unicode.GetString(bytes, start, evenLength) : string.Empty;
    }

    private string DecompressJet4(byte[] bytes, int start, int length)
    {
        var sb = new StringBuilder(length);
        bool compressed = true;
        int i = start;
        int end = start + length;

        while (i < end)
        {
            if (compressed)
            {
                if (bytes[i] == 0x00)
                {
                    compressed = false;
                    i++;
                    continue;
                }

                sb.Append((char)bytes[i]);
                i++;
            }
            else
            {
                if (i + 1 >= end)
                {
                    break;
                }

                if (bytes[i] == 0x00 && bytes[i + 1] == 0x00)
                {
                    compressed = true;
                    i += 2;
                    continue;
                }

                sb.Append((char)(bytes[i] | (bytes[i + 1] << 8)));
                i += 2;
            }
        }

        return sb.ToString();
    }

    private int ParseInt32(string value)
    {
        int parsed;
        return int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out parsed) ? parsed : 0;
    }

    private long ParseInt64(string value)
    {
        long parsed;
        return long.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out parsed) ? parsed : 0L;
    }

    private List<RowLocation> GetLiveRowLocations(long tdefPage)
    {
        var result = new List<RowLocation>();
        long total = _fs.Length / _pgSz;
        for (long pageNumber = 3; pageNumber < total; pageNumber++)
        {
            byte[] page = ReadPage(pageNumber);
            if (page[0] != 0x01)
            {
                continue;
            }

            if ((long)Ri32(page, _dpTDefOff) != tdefPage)
            {
                continue;
            }

            result.AddRange(EnumerateLiveRowLocations(pageNumber, page));
        }

        return result;
    }

    private IEnumerable<RowLocation> EnumerateLiveRowLocations(long pageNumber, byte[] page)
    {
        int numRows = Ru16(page, _dpNumRows);
        if (numRows == 0)
        {
            yield break;
        }

        var rawOffsets = new int[numRows];
        for (int r = 0; r < numRows; r++)
        {
            rawOffsets[r] = Ru16(page, _dpRowsStart + (r * 2));
        }

        int[] positions = rawOffsets
            .Select(o => o & 0x1FFF)
            .Where(o => o > 0 && o < _pgSz)
            .OrderBy(o => o)
            .ToArray();

        for (int r = 0; r < numRows; r++)
        {
            int raw = rawOffsets[r];
            if ((raw & 0x8000) != 0 || (raw & 0x4000) != 0)
            {
                continue;
            }

            int rowStart = raw & 0x1FFF;
            int rowEnd = _pgSz - 1;
            foreach (int pos in positions)
            {
                if (pos > rowStart)
                {
                    rowEnd = pos - 1;
                    break;
                }
            }

            yield return new RowLocation
            {
                PageNumber = pageNumber,
                RowIndex = r,
                RowStart = rowStart,
                RowSize = rowEnd - rowStart + 1,
            };
        }
    }

    private void MarkRowDeleted(long pageNumber, int rowIndex)
    {
        byte[] page = ReadPage(pageNumber);
        int offsetPos = _dpRowsStart + (rowIndex * 2);
        int raw = Ru16(page, offsetPos);
        if ((raw & 0x8000) != 0)
        {
            return;
        }

        Wu16(page, offsetPos, raw | 0x8000);
        WritePage(pageNumber, page);
    }

    private sealed class CatalogEntry
    {
        public string Name { get; set; } = string.Empty;

        public long TDefPage { get; set; }
    }

    private sealed class CatalogRow
    {
        public long PageNumber { get; set; }

        public int RowIndex { get; set; }

        public string Name { get; set; } = string.Empty;

        public int ObjectType { get; set; }

        public long Flags { get; set; }

        public long TDefPage { get; set; }
    }

    private sealed class RowLocation
    {
        public long PageNumber { get; set; }

        public int RowIndex { get; set; }

        public int RowStart { get; set; }

        public int RowSize { get; set; }
    }

    private sealed class PageInsertTarget
    {
        public long PageNumber { get; set; }

        public byte[] Page { get; set; } = Array.Empty<byte>();
    }

    private sealed class FixedValueChunk
    {
        public int Offset { get; set; }

        public byte[] Bytes { get; set; } = Array.Empty<byte>();
    }
}
