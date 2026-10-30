namespace JetDatabaseWriter.Tests.Maintenance;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Maintenance;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.ValueDecoding;
using JetDatabaseWriter.ValueDecoding.Models;
using JetDatabaseWriter.ValueEncoding;
using Xunit;
using static JetDatabaseWriter.Enums.ColumnType;

public sealed class CompactRawFieldReaderTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task Scan_PreservesUnassignedFixedAndVariableStorage(DatabaseFormat databaseFormat)
    {
        var format = JetFormat.ForNewDatabase(databaseFormat);
        TDefImage image = Image(
            new ColumnInfo { ColNum = 0, Name = "Fixed", Type = LongIntegerType, Flags = 1, Size = 4 },
            new ColumnInfo { ColNum = 1, Name = "Variable", Type = BinaryType, VarIdx = 0 });
        byte[] row = new RowEncoder(format).SerializeRow(image.Definition, [123, new byte[] { 0xFF, 0xFE, 0, 0x80 }]);
        row[^1] = 0;

        List<CompactRawField> fields = await ScanAsync(format, image, row);

        Assert.All(fields, field => Assert.Equal(CompactRawFieldKind.Null, field.Kind));
        Assert.Equal(new byte[] { 123, 0, 0, 0 }, fields[0].RawBytes.ToArray());
        Assert.Equal(new byte[] { 0xFF, 0xFE, 0, 0x80 }, fields[1].RawBytes.ToArray());
        row[fields[1].RowOffset] = 0x55;
        Assert.Equal(0x55, fields[1].RawBytes.Span[0]);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Scan_IdentifiesOpaqueCalculatedAndOrdinaryLongValueSlots(bool calculated)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.AceAccdb);
        TDefImage image = Image(new ColumnInfo { ColNum = 0, Name = "Memo", Type = MemoType, ExtraFlags = calculated ? (byte)0xC0 : (byte)0, CalculatedResultType = calculated ? MemoType : default });
        byte[] row = new RowEncoder(format).SerializeRow(image.Definition, ["abc"]);
        int first = format.RowFields.NumCols;
        row[first] = 0xFF;
        row[first + 1] = 0xFE;

        CompactRawField field = Assert.Single(await ScanAsync(format, image, row));

        Assert.Equal(CompactRawFieldKind.Stored, field.Kind);
        Assert.True(field.IsLongValue);
        Assert.Equal(0xFF, field.RawBytes.Span[0]);
        Assert.Equal(0xFE, field.RawBytes.Span[1]);
    }

    [Theory]
    [InlineData(TextType, MemoType, true)]
    [InlineData(TextType, OleType, true)]
    [InlineData(MemoType, TextType, false)]
    [InlineData(MemoType, LongIntegerType, false)]
    [InlineData(OleType, BinaryType, true)]
    [InlineData(TextType, BinaryType, true)]
    [InlineData(DoubleType, BinaryType, true)]
    public async Task Scan_CalculatedLongValueStorageFollowsPersistedResultType(ColumnType descriptorType, ColumnType resultType, bool expectedLongValue)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.AceAccdb);
        TDefImage image = Image(new ColumnInfo { ColNum = 0, Name = "Cached", Type = descriptorType, CalculatedResultType = resultType, ExtraFlags = 0xC0, Size = 1000 });
        object value = "abc";
        if (resultType == LongIntegerType)
        {
            value = 123;
        }
        else if (resultType is BinaryType or OleType)
        {
            value = new byte[] { 0xFF, 0xFE, 0x80 };
        }

        byte[] row = new RowEncoder(format).SerializeRow(image.Definition, [value]);
        byte[] original = row.AsSpan().ToArray();

        CompactRawField field = Assert.Single(await ScanAsync(format, image, row));

        Assert.Equal(CompactRawFieldKind.Stored, field.Kind);
        Assert.True(field.IsStorageTypeKnown);
        Assert.Equal(expectedLongValue, field.IsLongValue);
        Assert.Equal(original, row);
        Assert.Equal<byte>(original.AsSpan(field.RowOffset, field.RawBytes.Length), field.RawBytes.Span);
    }

    [Theory]
    [InlineData(TextType)]
    [InlineData(MemoType)]
    [InlineData(OleType)]
    public async Task Scan_UnhydratedCalculatedStorageRemainsOpaque(ColumnType descriptorType)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.AceAccdb);
        TDefImage image = Image(new ColumnInfo { ColNum = 0, Name = "Cached", Type = descriptorType, ExtraFlags = 0xC0, Size = 1000 });
        object value = descriptorType == OleType ? new byte[] { 0xFF, 0xFE, 0x80 } : "abc";
        byte[] row = new RowEncoder(format).SerializeRow(image.Definition, [value]);

        CompactRawField field = Assert.Single(await ScanAsync(format, image, row));

        Assert.Equal(CompactRawFieldKind.Stored, field.Kind);
        Assert.False(field.IsStorageTypeKnown);
        Assert.False(field.IsLongValue);
        Assert.Equal<byte>(row.AsSpan(field.RowOffset, field.RawBytes.Length), field.RawBytes.Span);
    }

    [Fact]
    public async Task Scan_DistinguishesAbsentBooleanNullAndEmptyAssignedStorage()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        TDefImage image = Image(
            new ColumnInfo { ColNum = 0, Name = "Bool", Type = BooleanType, Flags = 1 },
            new ColumnInfo { ColNum = 1, Name = "Binary", Type = BinaryType, VarIdx = 0 },
            new ColumnInfo { ColNum = 2, Name = "Later", Type = MemoType, VarIdx = 1 });
        byte[] row = new RowEncoder(format).SerializeRow(new TableDef { Columns = [image.Columns[0], image.Columns[1]] }, [false, Array.Empty<byte>()]);
        row[^1] |= 2;

        List<CompactRawField> fields = await ScanAsync(format, image, row);

        Assert.Equal(CompactRawFieldKind.Boolean, fields[0].Kind);
        Assert.False(fields[0].BoolValue);
        Assert.Equal(CompactRawFieldKind.Stored, fields[1].Kind);
        Assert.True(fields[1].RawBytes.IsEmpty);
        Assert.Equal(CompactRawFieldKind.Absent, fields[2].Kind);
        Assert.True(fields[2].RawBytes.IsEmpty);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Scan_OmittedVariableSlotRequiresAnUnsetAssignmentBit(bool assigned)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        TDefImage image = Image(new ColumnInfo { ColNum = 0, Type = MemoType });
        byte[] row = [1, 0, 2, 0, 0, 0, assigned ? (byte)1 : (byte)0];
        if (assigned)
        {
            await Assert.ThrowsAsync<JetCorruptDataException>(async () => await ScanAsync(format, image, row));
        }
        else
        {
            CompactRawField field = Assert.Single(await ScanAsync(format, image, row));
            Assert.Equal(CompactRawFieldKind.Null, field.Kind);
            Assert.True(field.RawBytes.IsEmpty);
        }
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public async Task Scan_RefusesDamagedOffsetsBeforeDeliveringFields(int damage)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        TDefImage image = Image(new ColumnInfo { ColNum = 0, Name = "A", Type = BinaryType, VarIdx = 0 }, new ColumnInfo { ColNum = 1, Name = "B", Type = BinaryType, VarIdx = 1 });
        byte[] row = new RowEncoder(format).SerializeRow(image.Definition, [new byte[] { 1 }, new byte[] { 2 }]);
        Assert.True(RowDecodePlan.TryParseRowLayout(format.RowFields, row, 0, row.Length, true, out RowLayout layout));
        int trailer = layout.VarTableStart - format.RowFields.Eod;
        int offset = damage switch { 0 => layout.VarTableStart + 2, 1 => layout.VarTableStart, _ => trailer };
        int value = damage switch { 0 => 0, 1 => format.RowFields.NumCols - 1, 2 => row.Length, _ => 1 };
        row[offset] = checked((byte)value);
        row[offset + 1] = 0;
        row[^1] = 0;
        int visited = 0;

        JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
            await CompactRawFieldReader.ScanAsync(
                format,
                image,
                new(7, 2, 42, row.Length),
                row,
                (_, _) =>
                {
                    visited++;
                    return ValueTask.CompletedTask;
                },
                Ct));

        Assert.Equal(0, visited);
        Assert.Equal(7, error.ErrorInfo.PageNumber);
    }

    [Fact]
    public async Task Scan_RefusesTruncatedFixedStorageEvenWhenNull()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        TDefImage image = Image(new ColumnInfo { ColNum = 0, Name = "A", Type = LongIntegerType, Flags = 1, FixedOff = 100, Size = 4 });

        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await ScanAsync(format, image, [1, 0, 0]));
    }

    [Fact]
    public async Task Scan_Jet3VariableSlicesCrossBothJumpBoundaries()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet3Mdb);
        TDefImage image = Image(new ColumnInfo { ColNum = 0, Type = BinaryType, VarIdx = 0 }, new ColumnInfo { ColNum = 1, Type = BinaryType, VarIdx = 1 }, new ColumnInfo { ColNum = 2, Type = BinaryType, VarIdx = 2 });
        byte[] first = new byte[255];
        byte[] second = new byte[255];
        byte[] third = [0xFF, 0xFE, 0x80];
        byte[] row = new RowEncoder(format).SerializeRow(image.Definition, [first, second, third]);
        Assert.Equal(2, Jet3JumpTable.EntryCount(row.Length));

        List<CompactRawField> fields = await ScanAsync(format, image, row);

        Assert.Equal(first, fields[0].RawBytes.ToArray());
        Assert.Equal(second, fields[1].RawBytes.ToArray());
        Assert.Equal(third, fields[2].RawBytes.ToArray());
        Assert.Equal(511, fields[2].RowOffset);
    }

    [Theory]
    [InlineData(nameof(TestDatabases.OverflowTestV1997))]
    [InlineData(nameof(TestDatabases.OverflowTestV2000))]
    [InlineData(nameof(TestDatabases.OverflowTestV2007))]
    public async Task Scan_NativeRowsPreserveEveryAssignedFieldSlice(string fieldName)
    {
        string path = (string)typeof(TestDatabases).GetField(fieldName)!.GetValue(null)!;
        await using AccessReader reader = await TestDatabases.OpenAsync(path, cancellationToken: Ct);
        CatalogEntry entry = Assert.IsType<CatalogEntry>(await reader.GetCatalogEntryAsync("Table1", Ct));
        TDefImage image = Assert.IsType<TDefImage>(await reader.Database.TableDefs.ReadImageAsync(entry.TDefPage, Ct));
        JetFormat format = reader.Database.Format;
        int visited = 0;
        await CompactRawRowReader.ScanAsync(
            reader.Database.Pages,
            format,
            entry.TDefPage,
            new(),
            async (location, row, token) =>
            {
                Assert.True(RowDecodePlan.TryParseRowLayout(format.RowFields, row.Span, 0, row.Length, image.Definition.HasVarColumns, out RowLayout layout));
                await CompactRawFieldReader.ScanAsync(
                    format,
                    image,
                    location,
                    row,
                    (field, _) =>
                    {
                        ColumnSlice expected = RowDecodePlan.ResolveColumnSlice(format.RowFields, row.Span, 0, row.Length, layout, field.Column);
                        if (expected.Kind is ColumnSliceKind.Fixed or ColumnSliceKind.Var)
                        {
                            Assert.Equal(CompactRawFieldKind.Stored, field.Kind);
                            Assert.Equal<byte>(row.Span.Slice(expected.DataStart, expected.DataLen), field.RawBytes.Span);
                            Assert.Equal(expected.DataStart, field.RowOffset);
                        }

                        Assert.Equal(location, field.Location);
                        visited++;
                        return ValueTask.CompletedTask;
                    },
                    token);
            },
            Ct);
        Assert.True(visited > 0);
    }

    [Theory]
    [InlineData("NativeFixedTextJet3Cp1252.mdb")]
    [InlineData("NativeFixedTextJet3Cp932.mdb")]
    [InlineData("NativeFixedTextJet4.mdb")]
    [InlineData("NativeFixedTextAce.accdb")]
    public async Task Scan_NativeFixedTextPreservesNullAndAssignedSlotBytes(string fileName)
    {
        string path = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Databases", "FixedText", fileName);
        await using AccessReader reader = await TestDatabases.OpenAsync(path, cancellationToken: Ct);
        CatalogEntry entry = Assert.IsType<CatalogEntry>(await reader.GetCatalogEntryAsync("FixedText", Ct));
        TDefImage image = Assert.IsType<TDefImage>(await reader.Database.TableDefs.ReadImageAsync(entry.TDefPage, Ct));
        int unassigned = 0;
        int assigned = 0;
        await CompactRawRowReader.ScanAsync(
            reader.Database.Pages,
            reader.Database.Format,
            entry.TDefPage,
            new(),
            async (location, row, token) =>
                await CompactRawFieldReader.ScanAsync(
                    reader.Database.Format,
                    image,
                    location,
                    row,
                    (field, _) =>
                    {
                        if (field.Column.Name == "Value")
                        {
                            Assert.True(field.Column.IsFixed);
                            Assert.Equal(field.Column.Size, field.RawBytes.Length);
                            Assert.Equal<byte>(row.Span.Slice(field.RowOffset, field.RawBytes.Length), field.RawBytes.Span);
                            if (field.Kind == CompactRawFieldKind.Null)
                            {
                                unassigned++;
                            }
                            else
                            {
                                Assert.Equal(CompactRawFieldKind.Stored, field.Kind);
                                assigned++;
                            }
                        }

                        return ValueTask.CompletedTask;
                    },
                    token),
            Ct);
        Assert.Equal(1, unassigned);
        Assert.True(assigned >= 4);
    }

    [Fact]
    public async Task Scan_NativeComplexReferencesRemainFourOpaqueBytes()
    {
        await using AccessReader reader = await TestDatabases.OpenAsync(TestDatabases.ComplexFields, cancellationToken: Ct);
        CatalogEntry entry = Assert.IsType<CatalogEntry>(await reader.GetCatalogEntryAsync("Documents", Ct));
        TDefImage image = Assert.IsType<TDefImage>(await reader.Database.TableDefs.ReadImageAsync(entry.TDefPage, Ct));
        int references = 0;
        await CompactRawRowReader.ScanAsync(
            reader.Database.Pages,
            reader.Database.Format,
            entry.TDefPage,
            new(),
            async (location, row, token) =>
                await CompactRawFieldReader.ScanAsync(
                    reader.Database.Format,
                    image,
                    location,
                    row,
                    (field, _) =>
                    {
                        if (field.Column.Type == ComplexType)
                        {
                            Assert.False(field.IsLongValue);
                            Assert.Equal(4, field.RawBytes.Length);
                            Assert.Equal<byte>(row.Span.Slice(field.RowOffset, 4), field.RawBytes.Span);
                            references++;
                        }

                        return ValueTask.CompletedTask;
                    },
                    token),
            Ct);
        Assert.True(references > 0);
    }

    [Fact]
    public async Task Scan_AwaitsVisitorAndHonorsCancellationBeforeNextField()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        TDefImage image = Image(new ColumnInfo { ColNum = 0, Type = BinaryType, VarIdx = 0 }, new ColumnInfo { ColNum = 1, Type = BinaryType, VarIdx = 1 });
        byte[] row = new RowEncoder(format).SerializeRow(image.Definition, [new byte[] { 1 }, new byte[] { 2 }]);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        int visited = 0;

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await CompactRawFieldReader.ScanAsync(
                format,
                image,
                new(7, 0, 0, row.Length),
                row,
                async (_, _) =>
                {
                    await Task.Yield();
                    visited++;
                    await cancellation.CancelAsync();
                },
                cancellation.Token));
        Assert.Equal(1, visited);
    }

    [Fact]
    public async Task Scan_PropagatesVisitorIoFailure()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        TDefImage image = Image(new ColumnInfo { ColNum = 0, Type = BinaryType });
        byte[] row = new RowEncoder(format).SerializeRow(image.Definition, [new byte[] { 1 }]);
        var expected = new IOException("visitor");

        IOException actual = await Assert.ThrowsAsync<IOException>(async () =>
            await CompactRawFieldReader.ScanAsync(format, image, new(7, 0, 0, row.Length), row, (_, _) => ValueTask.FromException(expected), Ct));
        Assert.Same(expected, actual);
    }

    private static TDefImage Image(params ColumnInfo[] columns)
        => new([], new(columns.Length, 0, 0, default), [.. columns], false, TDefParseIssues.None, null, [], []);

    private static async Task<List<CompactRawField>> ScanAsync(JetFormat format, TDefImage image, byte[] row)
    {
        var fields = new List<CompactRawField>();
        await CompactRawFieldReader.ScanAsync(
            format,
            image,
            new(7, 2, 42, row.Length),
            row,
            (field, _) =>
            {
                fields.Add(field);
                return ValueTask.CompletedTask;
            },
            Ct);
        return fields;
    }
}
