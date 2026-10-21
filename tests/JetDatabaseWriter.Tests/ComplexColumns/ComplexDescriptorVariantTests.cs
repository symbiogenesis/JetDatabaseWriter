namespace JetDatabaseWriter.Tests.ComplexColumns;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Microsoft-authored complex descriptor variants and their preservation.</summary>
public sealed class ComplexDescriptorVariantTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static string NativeFixture => Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Databases", "ComplexDescriptorVariants.accdb");

    /// <summary>Gets native tables with complex and calculated descriptor variants.</summary>
    public static TheoryData<string, string> NativeTables => new()
    {
        { NativeFixture, "Variants" },
        { TestDatabases.CalcFieldTestV2010, "Table1" },
    };

    /// <summary>Native decimal, primitive and append-only descriptors retain their distinct metadata.</summary>
    [Fact]
    public async Task NativeMultiValueDescriptors_DecimalUsesPrecisionInsteadOfTextMisc()
    {
        await using var stream = new MemoryStream(await File.ReadAllBytesAsync(NativeFixture, Ct));
        await using AccessWriter harness = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct);
        ResolvedTable parent = await harness.Services.Catalog.ResolveRequiredTableAsync("Variants", Ct);
        Assert.Equal(3, BinaryPrimitives.ReadInt32LittleEndian((await harness.Pager.ReadPageCopyAsync(parent.Entry.TDefPage, Ct)).AsSpan(28, 4)));
        ColumnPropertyBlock block = Assert.IsType<ColumnPropertyBlock>(await harness.Services.Snapshots.ReadLvPropBlockAsync(parent.Entry.TDefPage, Ct));
        Assert.True(Assert.Single(block.Targets, target => target.Name == "Notes").GetBooleanValue("AppendOnly"));
        IReadOnlyList<ComplexColumnInfo> complex;
        stream.Position = 0;
        await using (AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, Ct))
        {
            complex = await reader.GetComplexColumnsAsync("Variants", Ct);
            Assert.Equal(12m, Assert.Single(await reader.GetMultiValueItemsAsync("Variants", "MV108", Ct)).Value);
            Assert.Equal("initial", Assert.Single(await reader.GetMultiValueItemsAsync("Variants", "MV109", Ct)).Value);
            Assert.Equal("late", Assert.Single(await reader.GetMultiValueItemsAsync("Variants", "LateText", Ct)).Value);
        }

        Assert.Equal(10, complex.Count);
        foreach (ComplexColumnInfo info in complex)
        {
            ColumnInfo reference = Assert.Single(parent.Definition.Columns, column => column.Name == info.ColumnName);
            Assert.Equal(ColumnType.ComplexType, reference.Type);
            Assert.Equal(4, reference.Size);
            Assert.Equal(0x07, reference.Flags);
            Assert.Equal(info.ComplexId, reference.Misc);
            Assert.Equal(info.Kind == ComplexColumnKind.VersionHistory ? 0x20 : 0, reference.ExtraFlags);
            TableDef flat = Assert.IsType<TableDef>(await harness.Services.Catalog.ReadTableDefAsync(info.FlatTableId, Ct));
            ColumnInfo foreignKey = Assert.Single(flat.Columns, column => column.Name.StartsWith('_'));
            Assert.Equal((0x02, 0x08), (foreignKey.Flags, foreignKey.ExtraFlags));
            ColumnInfo scalar = Assert.Single(flat.Columns, column => column.IsAutoNumber);
            Assert.Equal((0x06, 0x04), (scalar.Flags, scalar.ExtraFlags));
            if (info.Kind == ComplexColumnKind.MultiValue)
            {
                ColumnInfo value = Assert.Single(flat.Columns, column => column.Name == "Value");
                Assert.Equal(0, value.ExtraFlags);
                if (value.Type == ColumnType.NumericType)
                {
                    AssertDecimalDescriptor(value);
                }
                else
                {
                    Assert.Equal(0x0409, value.Misc);
                }
            }
        }

        AssertDecimalDescriptor(Assert.Single((await ReadTemplateAsync(harness)).Columns));
    }

    /// <summary>Fresh default Decimal descriptors match the native template and flat value.</summary>
    [Fact]
    public async Task WriterMultiValueDecimal_UsesNativeTemplateAndDefaultValueDescriptor()
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, leaveOpen: true, cancellationToken: Ct))
        {
            await writer.CreateTableAsync("Ledger", [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Amounts", typeof(object)) { IsMultiValue = true, MultiValueElementType = typeof(decimal) }], Ct);
        }

        stream.Position = 0;
        await using AccessWriter harness = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct);
        AssertDecimalDescriptor(Assert.Single((await ReadTemplateAsync(harness)).Columns));
        long flatPage;
        stream.Position = 0;
        await using (AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, Ct))
        {
            flatPage = Assert.Single(await reader.GetComplexColumnsAsync("Ledger", Ct)).FlatTableId;
        }

        AssertDecimalDescriptor(Assert.Single(Assert.IsType<TableDef>(await harness.Services.Catalog.ReadTableDefAsync(flatPage, Ct)).Columns, column => column.Name == "Value"));
    }

    /// <summary>Schema edits preserve native descriptor semantics and complex reference high-water marks.</summary>
    /// <param name="path">The native fixture.</param>
    /// <param name="tableName">The parent table.</param>
    [Theory]
    [MemberData(nameof(NativeTables))]
    public async Task NativeDescriptorVariants_SurviveAddRenameDrop(string path, string tableName)
    {
        await using var stream = new MemoryStream();
        await stream.WriteAsync(await File.ReadAllBytesAsync(path, Ct), Ct);
        if (tableName == "Variants")
        {
            await AssertReferencesAsync(stream, 3, [1, 2]);
        }

        Dictionary<string, byte[]> before = await ReadDescriptorsAsync(stream, tableName);
        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct))
        {
            await writer.AddColumnAsync(tableName, new ColumnDefinition("AddedMarker", typeof(int)), Ct);
            await writer.RenameColumnAsync(tableName, "AddedMarker", "RenamedMarker", Ct);
            await writer.DropColumnAsync(tableName, "RenamedMarker", Ct);
        }

        if (tableName == "Variants")
        {
            await AssertReferencesAsync(stream, 3, [1, 2]);
        }

        Dictionary<string, byte[]> after = await ReadDescriptorsAsync(stream, tableName);
        Assert.Equal(before.Keys.Order(StringComparer.Ordinal), after.Keys.Order(StringComparer.Ordinal));
        foreach ((string name, byte[] descriptor) in before)
        {
            Assert.Equal(descriptor, after[name]);
        }
    }

    /// <summary>Native DAO reads and mutates default and declared-scale Decimal values before compacting.</summary>
    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task DecimalDescriptors_NativeAndWriterTablesSurviveMutationRewriteAndCompact()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.DecimalDescriptors");
        await File.WriteAllBytesAsync(session.SourcePath, await File.ReadAllBytesAsync(NativeFixture, Ct), Ct);
        await using (AccessWriter writer = await session.OpenWriterAsync(Ct))
        {
            await writer.CreateTableAsync("Ledger", [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Amounts", typeof(object)) { IsMultiValue = true, MultiValueElementType = typeof(decimal) }, new ColumnDefinition("Scaled", typeof(object)) { IsMultiValue = true, MultiValueElementType = typeof(decimal), NumericPrecision = 10, NumericScale = 2 }], Ct);
            await writer.InsertRowAsync("Ledger", [1, DBNull.Value, DBNull.Value], Ct);
            await writer.AddMultiValueItemAsync("Ledger", "Scaled", new Dictionary<string, object?> { ["Id"] = 1 }, 12.34m, Ct);
            await writer.AddMultiValueItemAsync("Ledger", "Amounts", new Dictionary<string, object?> { ["Id"] = 1 }, -123456789012345678m, Ct);
            await writer.InsertRowAsync("Variants", new RowValues { ["Id"] = 4 }, Ct);
            await writer.AddMultiValueItemAsync("Variants", "MV108", new Dictionary<string, object?> { ["Id"] = 4 }, 13m, Ct);
            foreach (string tableName in new[] { "Ledger", "Variants" })
            {
                await writer.AddColumnAsync(tableName, new ColumnDefinition("Marker", typeof(int)), Ct);
                await writer.RenameColumnAsync(tableName, "Marker", "RenamedMarker", Ct);
                await writer.DropColumnAsync(tableName, "RenamedMarker", Ct);
            }
        }

        const string nativeScript = """
            $rs = $db.OpenRecordset('Ledger')
            try {
                $rs.Edit()
                $child = $rs.Fields('Amounts').Value
                try {
                    if ($child.Fields('Value').Value -ne [decimal]'-123456789012345678') { throw 'Decimal value changed.' }
                    $child.AddNew()
                    $child.Fields('Value').Value = [decimal]123
                    $child.Update()
                    $duplicateRefused = $false
                    try {
                        $child.AddNew()
                        $child.Fields('Value').Value = [decimal]123
                        $child.Update()
                    } catch {
                        if ($engine.Errors.Item(0).Number -ne 3820) { throw }
                        $duplicateRefused = $true
                        $child.CancelUpdate()
                    }
                    if (!$duplicateRefused) { throw 'Native duplicate accepted.' }
                } finally { $child.Close() }
                $scaled = $rs.Fields('Scaled').Value
                try {
                    if ($scaled.Fields('Value').Value -ne [decimal]'12.34') { throw 'Declared scale lost.' }
                    $scaled.AddNew()
                    $scaled.Fields('Value').Value = [decimal]'56.78'
                    $scaled.Update()
                } finally { $scaled.Close() }
                $rs.Update()
            } finally { $rs.Close() }
            $rs = $db.OpenRecordset('SELECT * FROM Variants WHERE Id=4')
            try {
                $child = $rs.Fields('MV108').Value
                try { if ($child.Fields('Value').Value -ne [decimal]13) { throw 'Native Decimal lost.' } } finally { $child.Close() }
            } finally { $rs.Close() }
            if (!$db.TableDefs('Variants').Fields('Notes').AppendOnly) { throw 'AppendOnly lost.' }
            $db.Execute('INSERT INTO Variants (Id) VALUES (5)',128)
            Write-Output 'NATIVE_DECIMAL_OK'
            """;
        AccessRoundTripEnvironment.CompactResult result = session.RunDaoDatabaseScriptThenCompact(nativeScript, TimeSpan.FromMinutes(2));
        Assert.True(result.ExitCode == 0, $"DAO failed: {result.StdOut}\n{result.StdErr}");
        Assert.Contains("NATIVE_DECIMAL_OK", result.StdOut, StringComparison.Ordinal);
        await using AccessReader compacted = await AccessReader.OpenAsync(session.CompactedPath, new AccessReaderOptions { UseLockFile = false }, Ct);
        Assert.Equal([-123456789012345678m, 123m], (await compacted.GetMultiValueItemsAsync("Ledger", "Amounts", Ct)).Select(item => Assert.IsType<decimal>(item.Value)).Order());
        Assert.Equal([12m, 13m], (await compacted.GetMultiValueItemsAsync("Variants", "MV108", Ct)).Select(item => Assert.IsType<decimal>(item.Value)).Order());
        Assert.Equal([12.34m, 56.78m], (await compacted.GetMultiValueItemsAsync("Ledger", "Scaled", Ct)).Select(item => Assert.IsType<decimal>(item.Value)).Order());
        IReadOnlyList<ComplexColumnInfo> complex = await compacted.GetComplexColumnsAsync("Variants", Ct);
        Assert.Equal(4, Assert.Single(await compacted.GetMultiValueItemsAsync("Variants", "MV108", Ct), item => Equals(item.Value, 13m)).ConceptualTableId);
        ComplexColumnInfo history = Assert.Single(complex, column => column.Kind == ComplexColumnKind.VersionHistory);
        IReadOnlyList<MultiValueItem> versions = await compacted.GetMultiValueItemsAsync("Variants", history.ColumnName, Ct);
        Assert.Contains(versions, item => Equals(item.Value, "first") && item.Modified.HasValue);
        Assert.Contains(versions, item => Equals(item.Value, "revised") && item.Modified.HasValue);
        await using var stream = new MemoryStream(await File.ReadAllBytesAsync(session.CompactedPath, Ct));
        await AssertReferencesAsync(stream, 5, [1, 2, 4, 5], allowCounterAdvance: true);
    }

    /// <summary>Stored calculated types remain separate from their persisted result types.</summary>
    [Fact]
    public async Task NativeCalculatedDescriptors_RetainDistinctStorageAndResultTypes()
    {
        await using var stream = new MemoryStream(await File.ReadAllBytesAsync(TestDatabases.CalcFieldTestV2010, Ct));
        await using AccessWriter harness = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct);
        ResolvedTable table = await harness.Services.Catalog.ResolveRequiredTableAsync("Table1", Ct);
        (string Name, ColumnType Storage, ColumnType Result)[] variants =
        [
            ("AllNames", ColumnType.TextType, ColumnType.MemoType),
            ("MonthlySalary", ColumnType.DoubleType, ColumnType.MoneyType),
            ("WeeklySalary", ColumnType.DoubleType, ColumnType.NumericType),
            ("IsRich", ColumnType.IntegerType, ColumnType.BooleanType),
            ("BoolTest", ColumnType.IntegerType, ColumnType.BooleanType),
            ("FloatTest", ColumnType.NumericType, ColumnType.FloatType),
        ];
        foreach ((string name, ColumnType storage, ColumnType result) in variants)
        {
            ColumnInfo column = Assert.Single(table.Definition.Columns, column => column.Name == name);
            Assert.True(column.IsCalculated);
            Assert.Equal(storage, column.Type);
            Assert.Equal(result, column.CalculatedResultType);
            Assert.Equal(0xC0, column.ExtraFlags & 0xC0);
        }
    }

    private static async Task<TableDef> ReadTemplateAsync(AccessWriter harness)
    {
        long page = await harness.Services.CatalogRows.FindSystemTableTdefPageAsync("MSysComplexType_Decimal", Ct);
        return Assert.IsType<TableDef>(await harness.Services.Catalog.ReadTableDefAsync(page, Ct));
    }

    private static async Task AssertReferencesAsync(MemoryStream stream, int counter, int[] identifiers, bool allowCounterAdvance = false)
    {
        stream.Position = 0;
        await using AccessWriter harness = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct);
        ResolvedTable parent = await harness.Services.Catalog.ResolveRequiredTableAsync("Variants", Ct);
        int actualCounter = BinaryPrimitives.ReadInt32LittleEndian((await harness.Pager.ReadPageCopyAsync(parent.Entry.TDefPage, Ct)).AsSpan(28, 4));
        Assert.InRange(actualCounter, counter, allowCounterAdvance ? int.MaxValue : counter);
        using DataTable snapshot = await harness.Services.Snapshots.ReadTableSnapshotAsync("Variants", Ct);
        Assert.Equal(identifiers, snapshot.Rows.Cast<DataRow>().Select(row => Assert.IsType<int>(row["Id"])).Order());
        foreach (DataRow row in snapshot.Rows)
        {
            int expected = Assert.IsType<int>(row["Id"]);
            foreach (ColumnInfo column in parent.Definition.Columns.Where(column => column.Type == ColumnType.ComplexType))
            {
                Assert.Equal(expected, Assert.IsType<ComplexIdRef>(row[column.Name]).Id);
            }
        }
    }

    private static void AssertDecimalDescriptor(ColumnInfo column)
    {
        Assert.Equal(ColumnType.NumericType, column.Type);
        Assert.Equal(0x02, column.Flags);
        Assert.Equal(0, column.ExtraFlags);
        Assert.Equal(9, column.Size);
        Assert.Equal(18, column.NumericPrecision);
        Assert.Equal(0, column.NumericScale);
        Assert.Equal([0x12, 0, 0, 0], column.RawDescriptor.Skip(11).Take(4));
    }

    private static async Task<Dictionary<string, byte[]>> ReadDescriptorsAsync(MemoryStream stream, string tableName)
    {
        stream.Position = 0;
        await using AccessWriter harness = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct);
        ResolvedTable table = await harness.Services.Catalog.ResolveRequiredTableAsync(tableName, Ct);
        return table.Definition.Columns.ToDictionary(column => column.Name, column => column.RawDescriptor.Where((_, offset) => offset is < 5 or (> 8 and < 21) or > 22).ToArray(), StringComparer.Ordinal);
    }
}
