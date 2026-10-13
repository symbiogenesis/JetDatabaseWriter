namespace JetDatabaseWriter.Tests.ComplexColumns;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
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

/// <summary>Native acceptance of complete schema rewrites on Microsoft-authored complex tables.</summary>
public sealed class ComplexSchemaDaoTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Gets the native complex tables and their persisted reference high-water marks.</summary>
    public static TheoryData<string, string, int> Fixtures => new()
    {
        { TestDatabases.NorthwindTraders, "ProductCategories", 17 },
        { TestDatabases.ComplexFields, "Documents", 2 },
        { TestDatabases.ComplexDataTestV2007, "Table1", 7 },
        { TestDatabases.ComplexDataTestV2010, "Table1", 8 },
    };

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    [MemberData(nameof(Fixtures))]
    public async Task NativeComplexTable_CompleteRewritePreservesValuesIndexesCountersAndAppendOnly(string fixture, string tableName, int seed)
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.ComplexSchema");
        await File.WriteAllBytesAsync(session.SourcePath, await File.ReadAllBytesAsync(fixture, Ct), Ct);
        IReadOnlyList<ComplexColumnInfo> complex;
        IReadOnlyList<IndexMetadata> indexes;
        var attachments = new Dictionary<string, IReadOnlyList<AttachmentRecord>>(StringComparer.Ordinal);
        var values = new Dictionary<string, IReadOnlyList<MultiValueItem>>(StringComparer.Ordinal);
        await using (AccessReader original = await AccessReader.OpenAsync(session.SourcePath, new AccessReaderOptions { UseLockFile = false }, Ct))
        {
            complex = await original.GetComplexColumnsAsync(tableName, Ct);
            indexes = await original.ListIndexesAsync(tableName, Ct);
            foreach (ComplexColumnInfo column in complex)
            {
                if (column.Kind == ComplexColumnKind.Attachment)
                {
                    attachments.Add(column.ColumnName, await original.GetAttachmentsAsync(tableName, column.ColumnName, Ct));
                }
                else
                {
                    values.Add(column.ColumnName, await original.GetMultiValueItemsAsync(tableName, column.ColumnName, Ct));
                }
            }
        }

        string renamedColumn = complex.First(column => column.Kind == ComplexColumnKind.Attachment).ColumnName;
        Dictionary<string, bool> appendOnly;
        ParentSnapshot parentValues;
        RowValues newRow;
        await using (WriterHarness original = await WriterHarness.OpenAsync(session.SourcePath, cancellationToken: Ct))
        {
            ResolvedTable table = await original.Services.Catalog.ResolveRequiredTableAsync(tableName, Ct);
            using DataTable snapshot = await original.Services.Snapshots.ReadTableSnapshotAsync(tableName, Ct);
            string keyColumn = indexes.FirstOrDefault(index => index.Kind == IndexKind.PrimaryKey)?.Columns[0].Name
                ?? table.Definition.Columns.First(column => column.Type != ColumnType.ComplexType).Name;
            string[] scalarColumns = [.. table.Definition.Columns.Where(column => column.Type != ColumnType.ComplexType).Select(column => column.Name)];
            parentValues = new ParentSnapshot(
                keyColumn,
                scalarColumns,
                snapshot.Rows.Cast<DataRow>().ToDictionary(row => Convert.ToString(row[keyColumn], CultureInfo.InvariantCulture)!, row => scalarColumns.Select(column => row[column]).ToArray(), StringComparer.Ordinal),
                snapshot.Rows.Cast<DataRow>().ToDictionary(row => Convert.ToString(row[keyColumn], CultureInfo.InvariantCulture)!, row => complex.ToDictionary(column => column.ColumnName, column => Assert.IsType<ComplexIdRef>(row[column.ColumnName]).Id, StringComparer.Ordinal), StringComparer.Ordinal));
            newRow = BuildNewRow(snapshot, table.Definition, indexes, keyColumn);
            appendOnly = await ReadAppendOnlyAsync(original, table.Entry.TDefPage);
            if (tableName == "Table1")
            {
                Assert.Contains(true, appendOnly.Values);
            }

            Assert.Equal(seed, await ReadCounterAsync(original, table.Entry.TDefPage));
        }

        await using (AccessWriter writer = await session.OpenWriterAsync(Ct))
        {
            await writer.AddColumnAsync(tableName, new ColumnDefinition("AddedMarker", typeof(int)), Ct);
            await writer.RenameColumnAsync(tableName, "AddedMarker", "RenamedMarker", Ct);
            await writer.DropColumnAsync(tableName, "RenamedMarker", Ct);
            await writer.AddColumnAsync(tableName, new ColumnDefinition("AddedFiles", typeof(byte[])) { IsAttachment = true }, Ct);
            await writer.AddColumnAsync(tableName, new ColumnDefinition("AddedTags", typeof(object), 100) { IsMultiValue = true, MultiValueElementType = typeof(string) }, Ct);
            await writer.AddColumnAsync(tableName, new ColumnDefinition("DroppedFiles", typeof(byte[])) { IsAttachment = true }, Ct);
            await writer.RenameColumnAsync(tableName, renamedColumn, "RenamedAttachment", Ct);
            await writer.RenameColumnAsync(tableName, "AddedFiles", "RenamedFiles", Ct);
            await writer.DropColumnAsync(tableName, "DroppedFiles", Ct);
            await writer.InsertRowAsync(tableName, newRow, Ct);
        }

        await AssertMetadataAsync(session.SourcePath, tableName, renamedColumn, indexes, seed + 1, parentValues, appendOnly);

        string payloadPath = Path.Combine(session.WorkDir, "native-added.txt");
        await File.WriteAllBytesAsync(payloadPath, [5, 8, 13], Ct);
        string tableLiteral = Quote(tableName);
        string nativeMutation = $$"""
            $table = $db.TableDefs({{tableLiteral}})
            foreach ($columnName in @('RenamedFiles','AddedTags','RenamedAttachment')) {
                $matches = @($table.Indexes | Where-Object { $_.Fields.Count -eq 1 -and $_.Fields.Item(0).Name -eq $columnName })
                if ($matches.Count -ne 1 -or !$matches[0].Unique -or !$matches[0].Required) { throw ('Invalid reference index: ' + $columnName) }
            }
            $rs = $db.OpenRecordset({{tableLiteral}})
            try {
                $rs.Edit()
                $child = $rs.Fields('RenamedFiles').Value
                try {
                    $child.AddNew()
                    $child.Fields('FileData').LoadFromFile({{Quote(payloadPath)}})
                    $child.Update()
                } finally { $child.Close() }
                $child = $rs.Fields('AddedTags').Value
                try {
                    $child.AddNew()
                    $child.Fields('Value').Value = 'native-tag'
                    $child.Update()
                } finally { $child.Close() }
                $rs.Update()
            } finally { $rs.Close() }
            Write-Output 'NATIVE_MUTATION_OK'
            """;
        AccessRoundTripEnvironment.CompactResult result = session.RunDaoDatabaseScriptThenCompact(nativeMutation, TimeSpan.FromMinutes(2));
        AssertDaoSuccess(result, "NATIVE_MUTATION_OK");

        await AssertMetadataAsync(session.CompactedPath, tableName, renamedColumn, indexes, seed + 1, parentValues, appendOnly, allowCounterAdvance: true);
        await using (AccessReader compacted = await AccessReader.OpenAsync(session.CompactedPath, new AccessReaderOptions { UseLockFile = false }, Ct))
        {
            foreach ((string columnName, IReadOnlyList<AttachmentRecord> expected) in attachments)
            {
                IReadOnlyList<AttachmentRecord> actual = await compacted.GetAttachmentsAsync(tableName, columnName == renamedColumn ? "RenamedAttachment" : columnName, Ct);
                Assert.Equal(expected.Select(AttachmentIdentity).Order(StringComparer.Ordinal), actual.Select(AttachmentIdentity).Order(StringComparer.Ordinal));
            }

            foreach ((string columnName, IReadOnlyList<MultiValueItem> expected) in values)
            {
                IReadOnlyList<MultiValueItem> actual = await compacted.GetMultiValueItemsAsync(tableName, columnName, Ct);
                Assert.Equal(expected.OrderBy(ItemIdentity), actual.OrderBy(ItemIdentity));
            }

            Assert.Equal([5, 8, 13], Assert.Single(await compacted.GetAttachmentsAsync(tableName, "RenamedFiles", Ct)).FileData);
            Assert.Equal("native-tag", Assert.Single(await compacted.GetMultiValueItemsAsync(tableName, "AddedTags", Ct)).Value);
        }

        string nativeReadback = BuildNativeReadbackScript(tableName, renamedColumn, complex, attachments, values, appendOnly, parentValues, session.WorkDir);
        AssertDaoSuccess(session.RunDaoDatabaseScript(session.CompactedPath, nativeReadback, TimeSpan.FromMinutes(2)), "NATIVE_READBACK_OK");
        int fileIndex = 0;
        foreach (AttachmentRecord attachment in attachments.Values.SelectMany(list => list))
        {
            Assert.Equal(attachment.FileData, await File.ReadAllBytesAsync(Path.Combine(session.WorkDir, $"preserved-{fileIndex++}.bin"), Ct));
        }
    }

    private static RowValues BuildNewRow(DataTable snapshot, TableDef definition, IReadOnlyList<IndexMetadata> indexes, string keyColumn)
    {
        var row = new RowValues();
        HashSet<string> keyColumns = [.. indexes.Where(index => index.HasUniqueFlag).SelectMany(index => index.Columns.Select(column => column.Name))];
        keyColumns.Add(keyColumn);
        foreach (ColumnInfo column in definition.Columns)
        {
            if (column.Type == ColumnType.ComplexType || column.IsAutoNumber)
            {
                continue;
            }

            object value = snapshot.Rows[0][column.Name];
            if (keyColumns.Contains(column.Name))
            {
                value = value is string ? "s8-native" : checked(snapshot.Rows.Cast<DataRow>().Max(existing => Convert.ToInt32(existing[column.Name], CultureInfo.InvariantCulture)) + 1);
            }

            row[column.Name] = value;
        }

        return row;
    }

    private static async Task AssertMetadataAsync(string path, string tableName, string renamedColumn, IReadOnlyList<IndexMetadata> originalIndexes, int counter, ParentSnapshot parentValues, Dictionary<string, bool> appendOnly, bool allowCounterAdvance = false)
    {
        await using (AccessReader reader = await AccessReader.OpenAsync(path, new AccessReaderOptions { UseLockFile = false }, Ct))
        {
            IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync(tableName, Ct);
            Assert.Equal(originalIndexes.Count + 2, indexes.Count);
            foreach (IndexMetadata original in originalIndexes)
            {
                IndexMetadata actual = original.Kind == IndexKind.ForeignKey
                    ? Assert.Single(indexes, index => index.Kind == IndexKind.ForeignKey && index.Columns.Select(column => column.Name).SequenceEqual(original.Columns.Select(column => column.Name)))
                    : Assert.Single(indexes, index => index.Name == original.Name);
                Assert.Equal(original.Kind, actual.Kind);
                Assert.Equal(original.HasUniqueFlag, actual.HasUniqueFlag);
                Assert.Equal(original.IsRequired, actual.IsRequired);
                Assert.Equal(original.IgnoreNulls, actual.IgnoreNulls);
                Assert.Equal(original.Columns.Select(column => column.Name == renamedColumn ? "RenamedAttachment" : column.Name), actual.Columns.Select(column => column.Name));
                Assert.Equal(original.Columns.Select(column => column.IsAscending), actual.Columns.Select(column => column.IsAscending));
            }

            foreach (string columnName in new[] { "RenamedFiles", "AddedTags" })
            {
                IndexMetadata index = Assert.Single(indexes, index => index.Columns.Count == 1 && index.Columns[0].Name == columnName);
                Assert.True(index.HasUniqueFlag);
                Assert.True(index.IsRequired);
                Assert.True(index.Columns[0].IsAscending);
            }

            using DataTable catalog = await reader.ReadTableAsync("MSysObjects", cancellationToken: Ct);
            Assert.DoesNotContain(catalog.Rows.Cast<DataRow>(), row => Convert.ToString(row["Name"], CultureInfo.InvariantCulture)!.StartsWith("~tmp_", StringComparison.Ordinal));
            Assert.DoesNotContain(catalog.Rows.Cast<DataRow>(), row => Convert.ToString(row["Name"], CultureInfo.InvariantCulture)!.EndsWith("_DroppedFiles", StringComparison.Ordinal));
            Assert.DoesNotContain(await reader.GetComplexColumnsAsync(tableName, Ct), column => column.ColumnName == "DroppedFiles");
        }

        await using WriterHarness harness = await WriterHarness.OpenAsync(path, cancellationToken: Ct);
        ResolvedTable table = await harness.Services.Catalog.ResolveRequiredTableAsync(tableName, Ct);
        int actualCounter = await ReadCounterAsync(harness, table.Entry.TDefPage);
        if (allowCounterAdvance)
        {
            Assert.True(actualCounter >= counter);
        }
        else
        {
            Assert.Equal(counter, actualCounter);
        }

        using DataTable snapshot = await harness.Services.Snapshots.ReadTableSnapshotAsync(tableName, Ct);
        Assert.Equal(parentValues.Rows.Count + 1, snapshot.Rows.Count);
        foreach ((string key, object[] expected) in parentValues.Rows)
        {
            DataRow actual = Assert.Single(snapshot.Rows.Cast<DataRow>(), row => Convert.ToString(row[parentValues.KeyColumn], CultureInfo.InvariantCulture) == key);
            Assert.Equal(expected, parentValues.Columns.Select(column => actual[column]).ToArray());
            foreach ((string columnName, int reference) in parentValues.References[key])
            {
                string mappedName = columnName == renamedColumn ? "RenamedAttachment" : columnName;
                Assert.Equal(reference, Assert.IsType<ComplexIdRef>(actual[mappedName]).Id);
            }
        }

        foreach (ColumnInfo column in table.Definition.Columns.Where(column => column.Type == ColumnType.ComplexType))
        {
            int maximumReference = snapshot.Rows.Cast<DataRow>().Max(row => Assert.IsType<ComplexIdRef>(row[column.Name]).Id);
            Assert.InRange(maximumReference, counter, actualCounter);
        }

        Assert.Equal(appendOnly.OrderBy(pair => pair.Key, StringComparer.Ordinal), (await ReadAppendOnlyAsync(harness, table.Entry.TDefPage)).OrderBy(pair => pair.Key, StringComparer.Ordinal));
    }

    private static async Task<int> ReadCounterAsync(WriterHarness harness, long page)
        => BinaryPrimitives.ReadInt32LittleEndian((await harness.Pager.ReadPageCopyAsync(page, Ct)).AsSpan(28, 4));

    private static async Task<Dictionary<string, bool>> ReadAppendOnlyAsync(WriterHarness harness, long page)
    {
        ColumnPropertyBlock? block = await harness.Services.Snapshots.ReadLvPropBlockAsync(page, Ct);
        return block?.Targets.Where(target => target.GetBooleanValue("AppendOnly").HasValue).ToDictionary(target => target.Name, target => target.GetBooleanValue("AppendOnly")!.Value, StringComparer.Ordinal) ?? [];
    }

    private static string AttachmentIdentity(AttachmentRecord attachment)
        => FormattableString.Invariant($"{attachment.ConceptualTableId}:{attachment.FileName}:{attachment.FileType}:{attachment.FileURL}:{attachment.FileTimeStamp:O}:{Convert.ToBase64String(attachment.FileData)}");

    private static string ItemIdentity(MultiValueItem item)
        => FormattableString.Invariant($"{item.ConceptualTableId}:{item.Value}:{item.Modified:O}");

    private static string Quote(string value) => AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(value);

    private static void AssertDaoSuccess(AccessRoundTripEnvironment.CompactResult result, string marker)
    {
        Assert.True(result.ExitCode == 0, $"DAO failed: {result.StdOut}\n{result.StdErr}");
        Assert.Contains(marker, result.StdOut, StringComparison.Ordinal);
    }

    private static string BuildNativeReadbackScript(string tableName, string renamedColumn, IReadOnlyList<ComplexColumnInfo> complex, Dictionary<string, IReadOnlyList<AttachmentRecord>> attachments, Dictionary<string, IReadOnlyList<MultiValueItem>> values, Dictionary<string, bool> appendOnly, ParentSnapshot parentValues, string workDir)
    {
        var script = new List<string>();
        foreach (ComplexColumnInfo column in complex.Where(column => column.Kind != ComplexColumnKind.VersionHistory))
        {
            string columnName = column.ColumnName == renamedColumn ? "RenamedAttachment" : column.ColumnName;
            script.Add($"$rs = $db.OpenRecordset({Quote(tableName)})");
            script.Add("try { while (!$rs.EOF) {");
            foreach ((string key, Dictionary<string, int> references) in parentValues.References)
            {
                script.Add($"if ([string]$rs.Fields({Quote(parentValues.KeyColumn)}).Value -eq {Quote(key)}) {{");
                script.Add($"$child = $rs.Fields({Quote(columnName)}).Value");
                script.Add("try { if (!$child.EOF) { $child.MoveLast(); $child.MoveFirst() }");
                if (column.Kind == ComplexColumnKind.Attachment)
                {
                    AttachmentRecord[] expected = [.. attachments[column.ColumnName].Where(attachment => attachment.ConceptualTableId == references[column.ColumnName])];
                    script.Add(FormattableString.Invariant($"if ($child.RecordCount -ne {expected.Length}) {{ throw 'Lost attachment rows.' }}"));
                    script.Add("while (!$child.EOF) { switch ([string]$child.Fields('FileName').Value) {");
                    foreach (AttachmentRecord attachment in expected)
                    {
                        int attachmentIndex = attachments.Values.SelectMany(list => list).TakeWhile(item => !ReferenceEquals(item, attachment)).Count();
                        script.Add($"{Quote(attachment.FileName)} {{ $child.Fields('FileData').SaveToFile({Quote(Path.Combine(workDir, $"preserved-{attachmentIndex}.bin"))}) }}");
                    }

                    script.Add("default { throw 'Unexpected attachment.' } } $child.MoveNext() }");
                }
                else
                {
                    MultiValueItem[] expected = [.. values[column.ColumnName].Where(value => value.ConceptualTableId == references[column.ColumnName])];
                    script.Add(FormattableString.Invariant($"if ($child.RecordCount -ne {expected.Length}) {{ throw 'Lost complex values.' }}"));
                    script.Add($"$expected = @({string.Join(',', expected.Select(value => Quote(Convert.ToString(value.Value, CultureInfo.InvariantCulture)!)))})");
                    script.Add("$actual = @(); while (!$child.EOF) { $actual += [string]$child.Fields('Value').Value; $child.MoveNext() }");
                    script.Add("if ((($actual | Sort-Object) -join [char]10) -ne (($expected | Sort-Object) -join [char]10)) { throw 'Lost complex value.' }");
                }

                script.Add("} finally { $child.Close() } }");
            }

            script.Add("$rs.MoveNext() } } finally { $rs.Close() }");
        }

        foreach ((string columnName, bool value) in appendOnly.Where(pair => pair.Key.Length > 0))
        {
            script.Add($"if ($db.TableDefs({Quote(tableName)}).Fields({Quote(columnName)}).AppendOnly -ne ${(value ? "true" : "false")}) {{ throw 'Lost AppendOnly property.' }}");
        }

        if (tableName == "Table1")
        {
            script.Add("if (!$db.TableDefs('Table1').Fields('append-memo-data').AppendOnly) { throw 'Lost native append-only Memo.' }");
        }

        script.Add($"$table = $db.TableDefs({Quote(tableName)})");
        script.Add("if (@($table.Fields | Where-Object { $_.Name -eq 'DroppedFiles' }).Count -ne 0) { throw 'Dropped field survived.' }");
        script.Add("Write-Output 'NATIVE_READBACK_OK'");
        return string.Join(Environment.NewLine, script);
    }

    private sealed record ParentSnapshot(string KeyColumn, string[] Columns, Dictionary<string, object[]> Rows, Dictionary<string, Dictionary<string, int>> References);
}
