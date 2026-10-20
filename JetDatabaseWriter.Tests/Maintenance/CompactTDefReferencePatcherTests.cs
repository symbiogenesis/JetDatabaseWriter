namespace JetDatabaseWriter.Tests.Maintenance;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Threading;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.LongValues;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Maintenance;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using Xunit;
using static JetDatabaseWriter.Schema.JetTypeInfo;

public sealed class CompactTDefReferencePatcherTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, true)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    public void Patch_ChangesOnlyKnownReferencesAndRetainsOpaqueBytes(DatabaseFormat kind, bool wide)
    {
        var format = JetFormat.ForNewDatabase(kind);
        byte[] source = Definition(format, wide);
        TDefImage image = Assert.IsType<TDefImage>(TDefCodec.Parse(format, source));
        Assert.True(LongValueUsageMaps.TryRead(format, source, out List<LongValueUsageMap> maps, out _));
        byte[] expected = (byte[])source.Clone();
        UsageMap.WritePointer(expected, format.TDef.UsedPages, 2, 100);
        UsageMap.WritePointer(expected, format.TDef.FreePages, 3, 100);
        int root = Assert.Single(image.RealIndexes).Slot.FirstDpOffset;
        Assert.Equal(wide, root > format.PageSize);
        Wi32(expected, root, 101);
        UsageMap.WritePointer(expected, root - 4, 4, 100);
        UsageMap.WritePointer(expected, maps[0].OwnedOffset, 5, 100);
        UsageMap.WritePointer(expected, maps[0].FreeOffset, 6, 100);

        byte[] patched = CompactTDefReferencePatcher.Patch(
            format, image, new(2, 100), new(3, 100), [new(101, new(4, 100))], [new(0, new(5, 100), new(6, 100))], 1_000_000, Ct);

        Assert.Equal(expected, patched);
        Assert.Equal(source, image.CopyBytes());
        TDefImage reopened = Assert.IsType<TDefImage>(TDefCodec.Parse(format, patched));
        Assert.Equal(image.Header.Counters, reopened.Header.Counters);
        Assert.Equal(image.Columns[0] with { RawDescriptor = [] }, reopened.Columns[0] with { RawDescriptor = [] });
        Assert.Equal(image.Columns[0].RawDescriptor, reopened.Columns[0].RawDescriptor);
        Assert.Equal(image.LogicalIndexes[0].RawDescriptor, reopened.LogicalIndexes[0].RawDescriptor);
        Assert.Equal(101u, reopened.RealIndexes[0].RootPage);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public void Patch_RefusesIncompleteOrUnrepresentableReferences(int fault)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.AceAccdb);
        TDefImage image = Assert.IsType<TDefImage>(TDefCodec.Parse(format, Definition(format)));
        CompactTDefIndexReferences[] indexes = fault == 0 ? [] : [new(fault == 1 ? 0x1000000u : 101, new(4, 100))];
        CompactTDefLongValueReferences[] values = fault == 2 ? [] : [new(fault == 3 ? 1 : 0, new(5, 100), new(6, 100))];
        Assert.Throws<ArgumentException>(() => CompactTDefReferencePatcher.Patch(format, image, new(2, 100), new(3, 100), indexes, values, 1_000_000, Ct));
    }

    [Fact]
    public void Patch_RefusesMalformedTrailerBudgetAndCancellation()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        byte[] source = Definition(format);
        TDefImage image = Assert.IsType<TDefImage>(TDefCodec.Parse(format, source));
        CompactTDefIndexReferences[] indexes = [new(101, new(4, 100))];
        CompactTDefLongValueReferences[] values = [new(0, new(5, 100), new(6, 100))];
        Assert.Throws<JetLimitationException>(() => CompactTDefReferencePatcher.Patch(format, image, new(2, 100), new(3, 100), indexes, values, 1, Ct));
        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();
        Assert.ThrowsAny<OperationCanceledException>(() => CompactTDefReferencePatcher.Patch(format, image, new(2, 100), new(3, 100), indexes, values, 1_000_000, cancelled.Token));
        Wi32(source, 8, format.TDef.BlockEnd - 8);
        TDefImage malformed = Assert.IsType<TDefImage>(TDefCodec.Parse(format, source));
        Assert.Throws<JetCorruptDataException>(() => CompactTDefReferencePatcher.Patch(format, malformed, new(2, 100), new(3, 100), indexes, values, 1_000_000, Ct));
    }

    [Fact]
    public void Patch_ManyLongValueMapRecordsRequireCompleteParserStorageAdmission()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        byte[] original = Definition(format);
        Assert.True(LongValueUsageMaps.TryRead(format, original, out List<LongValueUsageMap> maps, out _));
        int trailer = maps[0].OwnedOffset - 2;
        int end = trailer + (Constants.TableDefinition.MaxColumns * 10) + 10;
        byte[] bytes = new byte[end];
        original.AsSpan(0, trailer).CopyTo(bytes);
        Wi32(bytes, 8, end - 8);
        var replacements = new CompactTDefLongValueReferences[Constants.TableDefinition.MaxColumns];
        for (int column = 0; column < replacements.Length; column++)
        {
            Wu16(bytes, trailer + (column * 10), column);
            replacements[column] = new(column, new(0, 100), new(1, 100));
        }

        Wu16(bytes, end - 10, ushort.MaxValue);
        TDefImage image = Assert.IsType<TDefImage>(TDefCodec.Parse(format, bytes));

        Assert.Throws<JetLimitationException>(() => CompactTDefReferencePatcher.Patch(format, image, new(2, 100), new(3, 100), [new(101, new(4, 100))], replacements, 150_000, Ct));

        Assert.Equal(bytes, image.CopyBytes());
    }

    private static byte[] Definition(JetFormat format, bool wide = false)
    {
        byte[] columnName = format.EncodeTDefNameRecord(wide ? new string('C', 60) : "Memo");
        byte[] indexName = format.EncodeTDefNameRecord("IX");
        int columns = wide ? 32 : 1;
        int column = format.TDef.BlockEnd + format.TDef.RealIdxEntrySz;
        int names = column + (columns * format.ColumnDescriptor.Size);
        int physical = names + (columns * columnName.Length);
        int logical = physical + format.Index.RealIdxPhysSize;
        int trailer = logical + format.Index.LogicalEntrySize + indexName.Length;
        byte[] bytes = new byte[wide ? (2 * format.PageSize) - 8 : format.PageSize];
        bytes[0] = Constants.PageTypes.TableDefinition;
        Wu16(bytes, format.TDef.NumCols, columns);
        Wu16(bytes, format.TDef.MaxCols, columns);
        Wu16(bytes, format.TDef.NumVarCols, columns);
        Wi32(bytes, format.TDef.NumIdx, 1);
        Wi32(bytes, format.TDef.NumRealIdx, 1);
        Wi32(bytes, format.TDef.NumRows, 17);
        Wi32(bytes, format.TDef.AutoNumber, 99);
        for (int i = 0; i < columns; i++)
        {
            int descriptor = column + (i * format.ColumnDescriptor.Size);
            bytes[descriptor + format.ColumnDescriptor.TypeOff] = (byte)ColumnType.MemoType;
            Wu16(bytes, descriptor + format.ColumnDescriptor.NumOff, i);
            Wu16(bytes, descriptor + format.ColumnDescriptor.VarOff, i);
            columnName.CopyTo(bytes, names + (i * columnName.Length));
        }

        format.Index.WriteRealIdxDescriptor(bytes, physical, [0], flags: 0, firstDp: 20);
        format.Index.WriteLogicalEntry(bytes, logical, 0, 0, 2, 8, 77, 1, 1, IndexKind.ForeignKey);
        indexName.CopyTo(bytes, logical + format.Index.LogicalEntrySize);
        Wu16(bytes, trailer, 0);
        UsageMap.WritePointer(bytes, trailer + 2, 0, 21);
        UsageMap.WritePointer(bytes, trailer + 6, 1, 21);
        Wu16(bytes, trailer + 10, ushort.MaxValue);
        int end = trailer + 24;
        Wi32(bytes, 8, end - 8);
        bytes[trailer + 22] = 0xA5;
        bytes[end + 5] = 0xD7;
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(format.TDef.UsedPages), 0x1500);
        return bytes;
    }
}
