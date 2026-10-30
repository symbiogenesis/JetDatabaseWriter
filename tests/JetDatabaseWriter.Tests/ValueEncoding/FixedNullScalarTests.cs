namespace JetDatabaseWriter.Tests.ValueEncoding;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.ValueEncoding;
using Xunit;

/// <summary>Null fixed scalars retain their declared slots before variable data.</summary>
public sealed class FixedNullScalarTests
{
    /// <summary>Gets the supported fixed scalar shapes in each format.</summary>
    public static TheoryData<DatabaseFormat, ColumnType, int> Shapes
    {
        get
        {
            var shapes = new TheoryData<DatabaseFormat, ColumnType, int>();
            foreach (DatabaseFormat format in new[] { DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb })
            {
                foreach ((ColumnType type, int width) in new[]
                {
                    (ColumnType.ByteType, 1),
                    (ColumnType.IntegerType, 2),
                    (ColumnType.LongIntegerType, 4),
                    (ColumnType.MoneyType, 8),
                    (ColumnType.FloatType, 4),
                    (ColumnType.DoubleType, 8),
                    (ColumnType.DateTimeType, 8),
                    (ColumnType.GuidType, 16),
                })
                {
                    shapes.Add(format, type, width);
                }

                if (format != DatabaseFormat.Jet3Mdb)
                {
                    shapes.Add(format, ColumnType.NumericType, 17);
                }

                if (format == DatabaseFormat.AceAccdb)
                {
                    shapes.Add(format, ColumnType.BigIntType, 8);
                    shapes.Add(format, ColumnType.DateTimeExtendedType, 42);
                }
            }

            return shapes;
        }
    }

    [Theory]
    [MemberData(nameof(Shapes))]
    public void SerializeRow_TrailingNullScalarReservesDeclaredSlot(DatabaseFormat databaseFormat, ColumnType type, int width)
    {
        var format = JetFormat.ForNewDatabase(databaseFormat);
        var definition = new TableDef
        {
            Columns =
            [
                new() { Name = "Id", Type = ColumnType.LongIntegerType, ColNum = 0, Flags = Constants.ColumnDescriptorFlags.Fixed, Size = 4 },
                new() { Name = "Value", Type = type, ColNum = 1, Flags = Constants.ColumnDescriptorFlags.Fixed, FixedOff = 4, Size = width },
                new() { Name = "Flag", Type = ColumnType.BooleanType, ColNum = 2, Flags = Constants.ColumnDescriptorFlags.Fixed },
                new() { Name = "Tail", Type = ColumnType.TextType, ColNum = 3, VarIdx = 0, Size = 40 },
            ],
        };
        var encoder = new RowEncoder(format);
        byte[] row = encoder.SerializeRow(definition, [1, DBNull.Value, true, "tail"]);
        byte[] tail = format.PropertyTextEncoding.GetBytes("tail");
        int fixedEnd = format.RowFields.NumCols + 4 + width;
        int expectedLength = fixedEnd + tail.Length + format.RowFields.Eod + format.RowFields.VarEntry + format.RowFields.VarLen + 1;
        Assert.Equal(expectedLength, row.Length);
        Assert.Equal(new byte[width], row.AsSpan(format.RowFields.NumCols + 4, width).ToArray());
        Assert.Equal(tail, row.AsSpan(fixedEnd, tail.Length).ToArray());
        Assert.Equal(0b1101, row[^1]);

        byte[] nulls = encoder.SerializeRow(definition, [DBNull.Value, DBNull.Value, false, DBNull.Value]);
        Assert.Equal(expectedLength - tail.Length, nulls.Length);
        Assert.Equal(new byte[4 + width], nulls.AsSpan(format.RowFields.NumCols, 4 + width).ToArray());
        Assert.Equal(0, nulls[^1]);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task NullScalarSlots_TooWideRowRefusesBeforeLongValueAllocation(DatabaseFormat databaseFormat)
    {
        var columns = new List<ColumnDefinition> { new("Id", typeof(int)), new("Memo", typeof(string), 20_000) };
        var values = new List<object?> { 1, new string('m', 10_000) };
        for (int index = 0; index < 253; index++)
        {
            columns.Add(new("G" + index, typeof(Guid)));
            values.Add(null);
        }

        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, databaseFormat, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("Items", columns, TestContext.Current.CancellationToken);
        }

        byte[] before = stream.ToArray();
        await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, cancellationToken: TestContext.Current.CancellationToken))
        {
            JetLimitationException error = await Assert.ThrowsAsync<JetLimitationException>(async () =>
                await writer.InsertRowAsync("Items", values.ToArray(), TestContext.Current.CancellationToken));
            Assert.Equal(JetErrorCode.RowTooLarge, error.ErrorCode);
        }

        Assert.Equal(before, stream.ToArray());
    }
}
