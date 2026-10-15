namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;
using static JetDatabaseWriter.Enums.ColumnType;

/// <summary>Pins known type metadata and refuses to author unknown on-disk discriminators.</summary>
public sealed class JetTypeInfoMetadataTests
{
    private const int DeclaredSize = 30;

    private static readonly IReadOnlyDictionary<ColumnType, ExpectedTypeInfo> KnownTypes = new Dictionary<ColumnType, ExpectedTypeInfo>
    {
        [BooleanType] = new(0, typeof(bool), false, 0, "Yes/No", ColumnSize.FromBits(1)),
        [ByteType] = new(1, typeof(byte), false, 1, "Byte", ColumnSize.FromBytes(1)),
        [IntegerType] = new(2, typeof(short), false, 2, "Integer", ColumnSize.FromBytes(2)),
        [LongIntegerType] = new(4, typeof(int), false, 4, "Long Integer", ColumnSize.FromBytes(4)),
        [MoneyType] = new(8, typeof(decimal), false, 8, "Currency", ColumnSize.FromBytes(8)),
        [FloatType] = new(4, typeof(float), false, 4, "Single", ColumnSize.FromBytes(4)),
        [DoubleType] = new(8, typeof(double), false, 8, "Double", ColumnSize.FromBytes(8)),
        [DateTimeType] = new(8, typeof(DateTime), false, 8, "Date/Time", ColumnSize.FromBytes(8)),
        [BinaryType] = new(0, typeof(byte[]), true, 0, "Binary", ColumnSize.FromBytes(DeclaredSize)),
        [TextType] = new(0, typeof(string), true, 0, "Text", ColumnSize.FromChars(DeclaredSize / 2)),
        [OleType] = new(0, typeof(byte[]), true, 0, "OLE Object", ColumnSize.Lval),
        [MemoType] = new(0, typeof(string), true, 0, "Memo", ColumnSize.Lval),
        [GuidType] = new(16, typeof(Guid), false, 16, "GUID", ColumnSize.FromBytes(16)),
        [NumericType] = new(17, typeof(decimal), false, 17, "Decimal", ColumnSize.FromBytes(17)),
        [BigBinaryType] = new(3992, typeof(byte[]), false, 0, "Big Binary", ColumnSize.FromBytes(DeclaredSize)),
        [ComplexType] = new(4, typeof(byte[]), false, 4, "Complex", ColumnSize.Lval),
        [BigIntType] = new(8, typeof(long), false, 8, "Big Integer", ColumnSize.FromBytes(8)),
        [DateTimeExtendedType] = new(42, typeof(DateTime), false, 42, "Date/Time Extended", ColumnSize.FromBytes(DeclaredSize)),
    };

    [Fact]
    public void Metadata_All256TypeCodes_HaveKnownFactsOrUnknownSentinels()
    {
        Assert.Equal(Enum.GetValues<ColumnType>().Order(), KnownTypes.Keys.Order());
        for (int code = 0; code <= byte.MaxValue; code++)
        {
            var type = (ColumnType)code;
            if (KnownTypes.TryGetValue(type, out ExpectedTypeInfo expected))
            {
                Assert.Equal(expected.FixedSize, JetTypeInfo.GetFixedSize(type));
                Assert.Equal(expected.ClrType, JetTypeInfo.GetClrType(type));
                Assert.Equal(expected.AlwaysVariable, JetTypeInfo.IsAlwaysVariableLength(type));
                Assert.Equal(expected.VariablePayloadSize > 0, JetTypeInfo.TryGetVariableSlotFixedPayloadSize(type, out int payloadSize));
                Assert.Equal(expected.VariablePayloadSize, payloadSize);
                Assert.Equal(expected.DisplayName, JetTypeInfo.GetTypeDisplayName(type));
                Assert.Equal(expected.ColumnSize, JetTypeInfo.GetColumnSize(type, DeclaredSize));
            }
            else
            {
                Assert.Equal(0, JetTypeInfo.GetFixedSize(type));
                Assert.Null(JetTypeInfo.GetClrType(type));
                Assert.False(JetTypeInfo.IsAlwaysVariableLength(type));
                Assert.False(JetTypeInfo.TryGetVariableSlotFixedPayloadSize(type, out int payloadSize));
                Assert.Equal(0, payloadSize);
                Assert.Equal($"0x{code:X2}", JetTypeInfo.GetTypeDisplayName(type));

                // Unknown descriptors retain their declared metadata size. This
                // is not evidence that a codec or an authorable type exists.
                Assert.Equal(ColumnSize.FromBytes(DeclaredSize), JetTypeInfo.GetColumnSize(type, DeclaredSize));
                Assert.Equal(ColumnSize.Variable, JetTypeInfo.GetColumnSize(type, 0));
            }
        }
    }

    [Fact]
    public void PhysicalMetadata_All256TypeCodes_HaveExplicitKnownFactsOrNoDescriptor()
    {
        for (int code = 0; code <= byte.MaxValue; code++)
        {
            var type = (ColumnType)code;
            if (KnownTypes.TryGetValue(type, out ExpectedTypeInfo expected))
            {
                JetTypeMetadata metadata = Assert.IsType<JetTypeMetadata>(JetTypeInfo.GetMetadata(type));
                Assert.Equal(expected.FixedSize, metadata.FixedSize);
                Assert.Equal(expected.VariablePayloadSize, metadata.VariableSlotPayloadSize);
                Assert.Equal(expected.ClrType, metadata.ClrType);
                Assert.Equal(expected.DisplayName, metadata.DisplayName);
                Assert.Equal(expected.AlwaysVariable, metadata.AlwaysVariableLength);
                Assert.Equal(type is TextType or BinaryType or BigBinaryType, metadata.UsesDeclaredFixedSize);
                Assert.Equal(metadata.UsesDeclaredFixedSize ? DeclaredSize : expected.FixedSize, JetTypeInfo.GetFixedStorageSize(type, DeclaredSize));
                Assert.Equal(type is MemoType or OleType, metadata.IsLongValue);
                Assert.Equal(type == BooleanType, metadata.UsesNullMaskValue);
            }
            else
            {
                Assert.Null(JetTypeInfo.GetMetadata(type));
                Assert.Equal(0, JetTypeInfo.GetFixedStorageSize(type, DeclaredSize));
            }
        }
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void ColumnSize_NonpositiveDeclaredSize_PreservesTypeSpecificDefaults(int declaredSize)
    {
        Assert.Equal(ColumnSize.FromBits(1), JetTypeInfo.GetColumnSize(BooleanType, declaredSize));
        Assert.Equal(ColumnSize.FromChars(255), JetTypeInfo.GetColumnSize(TextType, declaredSize));
        Assert.Equal(ColumnSize.FromBytes(4), JetTypeInfo.GetColumnSize(LongIntegerType, declaredSize));
        Assert.Equal(ColumnSize.Lval, JetTypeInfo.GetColumnSize(MemoType, declaredSize));
        Assert.Equal(ColumnSize.Lval, JetTypeInfo.GetColumnSize(ComplexType, declaredSize));
        Assert.Equal(ColumnSize.Variable, JetTypeInfo.GetColumnSize(BinaryType, declaredSize));
        Assert.Equal(ColumnSize.Variable, JetTypeInfo.GetColumnSize(BigBinaryType, declaredSize));
        Assert.Equal(ColumnSize.Variable, JetTypeInfo.GetColumnSize(DateTimeExtendedType, declaredSize));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void TypeCodeFromDefinition_AllUnknownCodes_AreRefused(bool calculated)
    {
        foreach (ColumnType type in UnknownCodes())
        {
            // Zero ResultType denotes an absent calculated override.
            if (calculated && type == default)
            {
                continue;
            }

            ColumnDefinition definition = calculated
                ? new("Unknown", typeof(string)) { IsCalculated = true, CalculatedResultType = (byte)type, CalculationExpression = "\"value\"" }
                : new("Unknown", typeof(string)) { ColumnTypeOverride = type };
            NotSupportedException error = Assert.Throws<NotSupportedException>(() => JetTypeInfo.TypeCodeFromDefinition(definition));
            Assert.Contains(JetTypeInfo.GetTypeDisplayName(type), error.Message, StringComparison.Ordinal);
        }
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public void ResolveStorageType_AllUnknownCodes_AreRefused(DatabaseFormat kind)
    {
        var format = JetFormat.ForNewDatabase(kind);
        var definition = new ColumnDefinition("Unknown", typeof(string));
        foreach (ColumnType type in UnknownCodes())
        {
            Assert.Throws<NotSupportedException>(() => JetTypeInfo.ResolveStorageType(definition, type, format, nameof(definition)));
        }
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CreateTable_AllUnknownCodes_RefusesBeforeChangingDatabase(DatabaseFormat kind)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, kind, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct);
        byte[] original = stream.ToArray();

        foreach (ColumnType type in UnknownCodes())
        {
            var definition = new ColumnDefinition("Unknown", typeof(string)) { ColumnTypeOverride = type };
            await Assert.ThrowsAsync<NotSupportedException>(async () => await writer.CreateTableAsync("Samples", [definition], ct));
            Assert.Equal(original, stream.ToArray());
        }
    }

    [Theory]
    [InlineData((byte)0x0D)]
    [InlineData((byte)0x48)]
    [InlineData(byte.MaxValue)]
    public async Task Rewrite_UnknownNativeDescriptor_IsAlreadyRefusedWithoutChangingBytes(byte code)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.Jet4Mdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct))
        {
            await writer.CreateTableAsync("Samples", [new ColumnDefinition("Opaque", typeof(int)), new ColumnDefinition("Known", typeof(int))], ct);
        }

        stream.Position = 0;
        await using (AccessWriter harness = await TestDatabase.OpenWriterAsync(stream, cancellationToken: ct))
        {
            ResolvedTable table = await harness.Services.Catalog.ResolveRequiredTableAsync("Samples", ct);
            long pageNumber = table.Entry.TDefPage;
            byte[] page = await harness.Database.Pages.ReadPageCopyAsync(pageNumber, ct);
            int realIndexes = BinaryPrimitives.ReadInt32LittleEndian(page.AsSpan(harness.Database.Format.TDef.NumRealIdx, 4));
            int firstDescriptor = harness.Database.Format.TDef.BlockEnd + (realIndexes * harness.Database.Format.TDef.RealIdxEntrySz);
            page[firstDescriptor + harness.Database.Format.ColumnDescriptor.TypeOff] = code;
            await harness.Pager.WritePageAsync(pageNumber, page, ct);
        }

        byte[] original = stream.ToArray();
        stream.Position = 0;
        await using AccessWriter reopened = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct);
        InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(async () => await reopened.RenameColumnAsync("Samples", "Known", "Renamed", ct));
        Assert.Contains($"0x{code:X2}", error.Message, StringComparison.Ordinal);
        Assert.Equal(original, stream.ToArray());
    }

    private static IEnumerable<ColumnType> UnknownCodes()
    {
        for (int code = 0; code <= byte.MaxValue; code++)
        {
            var type = (ColumnType)code;
            if (!KnownTypes.ContainsKey(type))
            {
                yield return type;
            }
        }
    }

    private readonly record struct ExpectedTypeInfo(int FixedSize, Type ClrType, bool AlwaysVariable, int VariablePayloadSize, string DisplayName, ColumnSize ColumnSize);
}
