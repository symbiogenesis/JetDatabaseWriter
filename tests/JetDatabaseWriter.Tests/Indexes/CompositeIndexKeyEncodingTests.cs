namespace JetDatabaseWriter.Tests.Indexes;

using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Collation;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Models;
using Xunit;

/// <summary>Composite keys retain descriptor semantics across seeks, uniqueness and index maintenance.</summary>
public sealed class CompositeIndexKeyEncodingTests
{
    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, true)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    public void SeekKey_UsesKeyOrderScaleFixedWidthCollationAndNullDirection(DatabaseFormat databaseFormat, bool ascending)
    {
        var format = JetFormat.ForNewDatabase(databaseFormat);
        var amount = new ColumnInfo { ColNum = 7, Type = ColumnType.NumericType, NumericScale = 2 };
        var data = new ColumnInfo { ColNum = 3, Type = ColumnType.BinaryType, Size = 4, Flags = Constants.ColumnDescriptorFlags.Fixed };
        var label = new ColumnInfo { ColNum = 12, Type = ColumnType.TextType, TextSortOrder = new TextSortOrder(0x0409, 1, true) };
        var optional = new ColumnInfo { ColNum = 19, Type = ColumnType.LongIntegerType };
        var table = new TableDef { Columns = [data, optional, amount, label] };
        var index = new IndexMetadata
        {
            Name = "Composite",
            Columns =
            [
                new IndexColumnReference { ColumnNumber = 12, IsAscending = ascending },
                new IndexColumnReference { ColumnNumber = 7, IsAscending = !ascending },
                new IndexColumnReference { ColumnNumber = 3, IsAscending = ascending },
                new IndexColumnReference { ColumnNumber = 19, IsAscending = !ascending },
            ],
        };
        object?[] values = ["Caf\u00e9", 1.235m, new byte[] { 1 }, DBNull.Value];
        byte[] prefix =
        [
            .. IndexKeyEncoder.EncodeColumnEntry(format, label, "Caf\u00e9", ascending),
            .. IndexKeyEncoder.EncodeColumnEntry(format, amount, 1.24m, !ascending),
        ];
        byte[] expected =
        [
            .. prefix,
            .. IndexKeyEncoder.EncodeColumnEntry(format, data, new byte[] { 1, 0, 0, 0 }, ascending),
            !ascending ? (byte)0x00 : (byte)0xFF,
        ];

        Assert.Equal(expected, IndexKeyEncoder.EncodeIndexSeekKey(format, "Samples", index, table, values));
        Assert.Equal(prefix, IndexKeyEncoder.EncodeIndexKeyPrefix(format, "Samples", index, table, values[..2], "prefix"));
        Assert.True(IndexKeyEncoder.CanEncodeIndexKeyPrefix(format, "Samples", index, table, values));
    }

    [Fact]
    public void SeekKey_RejectsInvalidArityAndSoftEncodingRetainsDescriptorRefusals()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.AceAccdb);
        var column = new ColumnInfo { ColNum = 7, Type = ColumnType.NumericType, NumericScale = 29 };
        var table = new TableDef { Columns = [column] };
        var index = new IndexMetadata { Name = "Amount", Columns = [new IndexColumnReference { ColumnNumber = 7 }] };

        ArgumentException full = Assert.Throws<ArgumentException>(() => IndexKeyEncoder.EncodeIndexSeekKey(format, "Samples", index, table, []));
        Assert.Equal("keyValues", full.ParamName);
        ArgumentException prefix = Assert.Throws<ArgumentException>(() => IndexKeyEncoder.EncodeIndexKeyPrefix(format, "Samples", index, table, [], "prefix"));
        Assert.Equal("prefix", prefix.ParamName);
        Assert.False(IndexKeyEncoder.CanEncodeIndexKeyPrefix(format, "Samples", index, table, [DBNull.Value]));
        Assert.False(IndexKeyEncoder.CanEncodeIndexKeyPrefix(format, "Samples", index, table, [1m]));
        Assert.Throws<JetCorruptDataException>(() => IndexKeyEncoder.EncodeIndexSeekKey(format, "Samples", index, new TableDef(), [1m]));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task UniqueCompositeIndex_IncrementalAndRebuiltKeysRemainSeekable(DatabaseFormat databaseFormat)
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, databaseFormat, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken))
        {
            await writer.CreateTableAsync(
                "Samples",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Amount", typeof(decimal)) { NumericPrecision = 28, NumericScale = 2 },
                    new ColumnDefinition("Data", typeof(byte[]), 4) { ColumnTypeOverride = ColumnType.BinaryType },
                    new ColumnDefinition("Label", typeof(string), 40),
                    new ColumnDefinition("Optional", typeof(int)),
                ],
                [new IndexDefinition("Composite", ["Label", "Amount", "Data", "Optional"]) { IsUnique = true, DescendingColumns = ["Amount", "Optional"] }],
                cancellationToken);
            await writer.InsertRowAsync("Samples", [1, 1.235m, new byte[] { 1 }, "Alpha", DBNull.Value], cancellationToken);
            await writer.InsertRowAsync("Samples", [2, -2.345m, new byte[] { 2 }, "Beta", 7], cancellationToken);

            byte[] before = stream.ToArray();
            JetConstraintException duplicate = await Assert.ThrowsAsync<JetConstraintException>(async () =>
                await writer.InsertRowAsync("Samples", [3, 1.24m, new byte[] { 1, 0, 0, 0 }, "ALPHA", DBNull.Value], cancellationToken));
            Assert.Equal(JetErrorCode.UniqueViolation, duplicate.ErrorCode);
            Assert.Equal(before, stream.ToArray());

            await using JetTransaction transaction = await writer.BeginTransactionAsync(cancellationToken);
            ResolvedTable table = await writer.Services.Catalog.ResolveRequiredTableAsync("Samples", cancellationToken);
            await writer.Services.Indexes.MaintainIndexesAsync(table.Entry.TDefPage, table.Definition, "Samples", cancellationToken);
            await transaction.CommitAsync(cancellationToken);
            await writer.InsertRowAsync("Samples", [3, 3.455m, new byte[] { 3 }, "Gamma", DBNull.Value], cancellationToken);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken);
        object[] alpha = Assert.Single(await reader.SeekRowsAsync("Samples", "Composite", ["alpha", 1.235m, new byte[] { 1 }, DBNull.Value], cancellationToken).ToListAsync(cancellationToken));
        Assert.Equal(1, alpha[0]);
        object[] beta = Assert.Single(await reader.SeekRowsAsync("Samples", "Composite", ["BETA", -2.34m, new byte[] { 2, 0, 0, 0 }, 7], cancellationToken).ToListAsync(cancellationToken));
        Assert.Equal(2, beta[0]);
        object[] gamma = Assert.Single(await reader.SeekRowsAsync("Samples", "Composite", ["gamma", 3.46m, new byte[] { 3 }, DBNull.Value], cancellationToken).ToListAsync(cancellationToken));
        Assert.Equal(3, gamma[0]);
    }
}
