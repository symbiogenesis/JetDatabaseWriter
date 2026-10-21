namespace JetDatabaseWriter.Tests.ComplexColumns;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Complex parent identities agree with independently executed native DAO predicates.</summary>
public sealed class ComplexParentKeyDaoTests
{
    private const string TableName = "NativeParents";
    private static readonly TimeSpan DaoTimeout = TimeSpan.FromMinutes(2);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Gets Access-authored hosts carrying the two supported ACE text sort versions.</summary>
    public static TheoryData<string, byte> NativeHosts => new()
    {
        { TestDatabases.TestV2007, 0 },
        { TestDatabases.TestV2010, 1 },
    };

    /// <summary>DAO's typed equality identifies the same complex parents before and after compaction.</summary>
    /// <param name="fixture">The Access-authored database copied before native table creation.</param>
    /// <param name="sortVersion">The expected native text sort version.</param>
    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [MemberData(nameof(NativeHosts))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task TypedKeysAndCollations_TargetNativeParentsAndSurviveCompactExtraction(string fixture, byte sortVersion)
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.ComplexParentKeys");
        await File.WriteAllBytesAsync(session.SourcePath, await File.ReadAllBytesAsync(fixture, Ct), Ct);
        ParentCase[] cases = Cases();
        AssertDaoSuccess(session.RunDaoDatabaseScript(session.SourcePath, AuthoringScript(cases, session.SourcePath), DaoTimeout), "NATIVE_PARENTS_CREATED");
        await AssertNativeColumnLayoutsAsync(session.SourcePath, sortVersion);

        byte[] original = await File.ReadAllBytesAsync(session.SourcePath, Ct);
        await using (AccessWriter writer = await session.OpenWriterAsync(Ct))
        {
            foreach (ParentCase item in NonmatchingKeys())
            {
                JetOperationException refused = await Assert.ThrowsAsync<JetOperationException>(async () =>
                    await writer.AddMultiValueItemAsync(TableName, "Items", new Dictionary<string, object?> { [item.Column] = item.Value }, item.Marker, Ct));
                Assert.Equal(JetErrorCode.RowNotFound, refused.ErrorCode);
            }
        }

        Assert.Equal(original, await File.ReadAllBytesAsync(session.SourcePath, Ct));
        await using (AccessWriter writer = await session.OpenWriterAsync(Ct))
        {
            foreach (ParentCase item in cases)
            {
                var key = new Dictionary<string, object?> { [item.Column] = item.Value };
                await writer.AddMultiValueItemAsync(TableName, "Items", key, item.Marker, Ct);
                await writer.AddAttachmentAsync(TableName, "Files", key, new AttachmentInput(item.Marker + ".bin", [1, 2, 3, 4]), Ct);
            }
        }

        await AssertReaderParentsAsync(session.SourcePath, cases);
        await AssertNativeParentsAndExtractionAsync(session, session.SourcePath, cases, "before");
        session.RunDaoCompact();
        await AssertReaderParentsAsync(session.CompactedPath, cases);
        await AssertNativeParentsAndExtractionAsync(session, session.CompactedPath, cases, "after");
    }

    private static ParentCase[] Cases() =>
    [
        new("DateKey", new DateTime(2026, 1, 1, 12, 34, 56), "DateTime", "[datetime]'2026-01-01T12:34:56'", 2, "date"),
        new("GuidKey", Guid.Parse("01234567-89ab-cdef-0123-456789abcdef"), "Guid", "'{01234567-89AB-CDEF-0123-456789ABCDEF}'", 2, "guid"),
        new("DecimalKey", 123.4567m, null, "[decimal]'123.4567'", 2, "decimal"),
        new("IntegerKey", 123, "Long", "123", 2, "integer"),
        new("CurrencyKey", 123.4567m, "Currency", "[decimal]'123.4567'", 2, "currency"),
        new("SingleKey", 1.5d, "Double", "[double]1.5", 2, "single"),
        new("FixedKey", (byte[])[2, 0, 0, 0], "Binary(4)", "[byte[]](2,0,0,0)", 2, "fixed-binary"),
        new("VariableKey", (byte[])[2, 0], "VarBinary(4)", "[byte[]](2,0)", 2, "variable-binary"),
        new("MemoKey", new string('x', 600) + "two", null, "('x' * 600) + 'two'", 2, "memo"),
        new("TextKey", "rÉSuMé", null, null, 2, "text-case"),
        new("EmptyKey", null, null, null, 3, "null"),
        new("EmptyKey", string.Empty, null, null, 4, "empty"),
    ];

    private static ParentCase[] NonmatchingKeys() =>
    [
        new("FixedKey", (byte[])[2], "Binary(4)", "[byte[]](2)", 0, "short-fixed"),
        new("DecimalKey", 123.45671m, null, "[decimal]'123.45671'", 0, "unrounded-decimal"),
        new("IntegerKey", 123.1m, null, "[decimal]'123.1'", 0, "unrounded-integer"),
        new("CurrencyKey", 123.45671m, null, "[decimal]'123.45671'", 0, "unrounded-currency"),
        new("SingleKey", 1.50000001d, "Double", "[double]1.50000001", 0, "unrounded-single"),
    ];

    private static string AuthoringScript(ParentCase[] cases, string path)
    {
        var script = new List<string>
        {
            "$db.Execute('CREATE TABLE [NativeParents] ([Id] LONG CONSTRAINT [PK_NativeParents] PRIMARY KEY, [DateKey] DATETIME, [GuidKey] GUID, [IntegerKey] LONG, [CurrencyKey] CURRENCY, [SingleKey] SINGLE, [FixedKey] BINARY(4), [VariableKey] VARBINARY(4), [MemoKey] MEMO, [TextKey] TEXT(80), [EmptyKey] TEXT(80))',128)",
            "$db.Close()",
            "$connection = New-Object -ComObject ADODB.Connection",
            $"try {{ $connection.Open({Quote("Provider=Microsoft.ACE.OLEDB.16.0;Data Source=" + path)}); $connection.Execute('ALTER TABLE [NativeParents] ADD COLUMN [DecimalKey] DECIMAL(18,4)') | Out-Null }} finally {{ $connection.Close() }}",
            $"$db = $engine.OpenDatabase({Quote(path)})",
            "$table = $db.TableDefs('NativeParents')",
            "$table.Fields('EmptyKey').AllowZeroLength = $true",
            "$table.Fields.Append($table.CreateField('Items',109))",
            "$table.Fields.Append($table.CreateField('Files',101))",
            "$rs = $db.OpenRecordset('NativeParents')",
            "try {",
            "for ($id=1; $id -le 4; $id++) {",
            "$rs.AddNew(); $rs.Fields('Id').Value = $id",
            "if ($id -le 2) {",
            "$rs.Fields('DateKey').Value = [datetime]'2026-01-01T12:34:56'",
            "$rs.Fields('GuidKey').Value = '{01234567-89AB-CDEF-0123-456789ABCDEF}'",
            "$rs.Fields('DecimalKey').Value = [decimal]'123.4567'",
            "$rs.Fields('IntegerKey').Value = 123",
            "$rs.Fields('CurrencyKey').Value = [decimal]'123.4567'",
            "$rs.Fields('SingleKey').Value = [single]1.5",
            "$rs.Fields('FixedKey').Value = [byte[]](2)",
            "$rs.Fields('VariableKey').Value = [byte[]](2,0)",
            "$rs.Fields('MemoKey').Value = ('x' * 600) + 'two'",
            "$rs.Fields('TextKey').Value = 'Résumé'",
            "$rs.Fields('EmptyKey').Value = 'two'",
            "if ($id -eq 1) {",
            "$rs.Fields('DateKey').Value = [datetime]'2026-01-01T12:34:55'",
            "$rs.Fields('GuidKey').Value = '{01234567-89AB-CDEF-0123-456789ABCDEE}'",
            "$rs.Fields('DecimalKey').Value = [decimal]'123.4566'",
            "$rs.Fields('IntegerKey').Value = 122",
            "$rs.Fields('CurrencyKey').Value = [decimal]'123.4566'",
            "$rs.Fields('SingleKey').Value = [single]1.25",
            "$rs.Fields('FixedKey').Value = [byte[]](1)",
            "$rs.Fields('VariableKey').Value = [byte[]](2)",
            "$rs.Fields('MemoKey').Value = ('x' * 600) + 'one'",
            "$rs.Fields('TextKey').Value = 'Different'",
            "$rs.Fields('EmptyKey').Value = 'one'",
            "}",
            "} elseif ($id -eq 4) { $rs.Fields('EmptyKey').Value = '' }",
            "$rs.Update()",
            "}",
            "} finally { $rs.Close() }",
        };
        script.AddRange(cases.Select(NativePredicateScript));
        script.AddRange(NonmatchingKeys().Select(NativePredicateScript));
        script.Add("Write-Output 'NATIVE_PARENTS_CREATED'");
        return string.Join(Environment.NewLine, script);
    }

    private static string NativePredicateScript(ParentCase item)
    {
        // DAO Text parameters turn an empty string into Null and reject long Memo
        // operands. Numeric parameters can also narrow to the column's precision.
        // Literal expressions preserve the caller's unrounded comparison operands.
        string? numeric = item.Value is int or decimal or double ? Convert.ToString(item.Value, CultureInfo.InvariantCulture) : null;
        string predicate = item.Value switch
        {
            null => $"[{item.Column}] IS NULL",
            string text => $"[{item.Column}]='{text.Replace("'", "''", StringComparison.Ordinal)}'",
            _ when numeric is not null => $"[{item.Column}]={numeric}",
            _ => $"[{item.Column}]=[probe]",
        };
        string declaration = item.ParameterType is null || numeric is not null ? string.Empty : $"PARAMETERS [probe] {item.ParameterType}; ";
        string assign = item.ParameterValue is null || item.Value is string || numeric is not null ? string.Empty : $"$query.Parameters('probe').Value = {item.ParameterValue}";
        string check = item.ParentId == 0
            ? $"if (!$match.EOF) {{ throw {Quote("DAO unexpectedly matched " + item.Marker)} }}"
            : $"if ($match.EOF -or $match.Fields('Id').Value -ne {item.ParentId}) {{ throw {Quote("DAO selected the wrong parent for " + item.Marker)} }}; $match.MoveNext(); if (!$match.EOF) {{ throw {Quote("DAO found multiple parents for " + item.Marker)} }}";
        return $$"""
            $query = $db.CreateQueryDef('', {{Quote(declaration + "SELECT [Id] FROM [NativeParents] WHERE " + predicate)}})
            try {
                {{assign}}
                $match = $query.OpenRecordset()
                try {
                    {{check}}
                } finally { $match.Close() }
            } finally { $query.Close() }
            """;
    }

    private static async Task AssertNativeColumnLayoutsAsync(string path, byte sortVersion)
    {
        await using AccessWriter harness = await AccessWriter.OpenAsync(path, new AccessWriterOptions { UseLockFile = false }, Ct);
        ResolvedTable table = await harness.Services.Catalog.ResolveRequiredTableAsync(TableName, Ct);
        ColumnInfo text = Assert.Single(table.Definition.Columns, column => column.Name == "TextKey");
        Assert.Equal(0x0409, text.TextSortOrder.Value);
        Assert.Equal(sortVersion, text.TextSortOrder.Version);
        Assert.True(text.TextSortOrder.HasVersion);
        Assert.True(Assert.Single(table.Definition.Columns, column => column.Name == "FixedKey").IsFixed);
        Assert.False(Assert.Single(table.Definition.Columns, column => column.Name == "VariableKey").IsFixed);
    }

    private static async Task AssertReaderParentsAsync(string path, ParentCase[] cases)
    {
        await using AccessReader reader = await AccessReader.OpenAsync(path, new AccessReaderOptions { UseLockFile = false }, Ct);
        ParentCase[] expected = cases.OrderBy(item => item.Marker, StringComparer.Ordinal).ToArray();
        IReadOnlyList<MultiValueItem> items = await reader.GetMultiValueItemsAsync(TableName, "Items", Ct);
        IReadOnlyList<AttachmentRecord> attachments = await reader.GetAttachmentsAsync(TableName, "Files", Ct);
        var references = new Dictionary<int, int>();
        await foreach (object[] row in reader.Rows(TableName, cancellationToken: Ct))
        {
            int id = Assert.IsType<int>(row[0]);
            if (row[^2] is byte[] values)
            {
                references[id] = Assert.Single(ComplexCellValue.ReadMultiValueItems(values).Select(item => item.ConceptualTableId).Distinct());
            }
        }

        Assert.Equal(expected.Length, items.Count);
        Assert.Equal(expected.Length, attachments.Count);
        foreach (ParentCase item in expected)
        {
            MultiValueItem value = Assert.Single(items, value => Equals(value.Value, item.Marker));
            AttachmentRecord attachment = Assert.Single(attachments, value => value.FileName == item.Marker + ".bin");
            Assert.Equal(references[item.ParentId], value.ConceptualTableId);
            Assert.Equal(value.ConceptualTableId, attachment.ConceptualTableId);
            Assert.Equal((byte[])[1, 2, 3, 4], attachment.FileData);
        }
    }

    private static async Task AssertNativeParentsAndExtractionAsync(AccessRoundTripSession session, string path, ParentCase[] cases, string prefix)
    {
        var script = new List<string>();
        script.AddRange(cases.Select(NativePredicateScript));
        script.AddRange(NonmatchingKeys().Select(NativePredicateScript));
        foreach (IGrouping<int, ParentCase> group in cases.GroupBy(item => item.ParentId))
        {
            string expected = string.Join(",", group.Select(item => Quote(item.Marker)));
            string outputPrefix = Quote(Path.Combine(session.WorkDir, prefix + "-"));
            script.Add($$"""
                $rs = $db.OpenRecordset('SELECT * FROM [NativeParents] WHERE [Id]={{group.Key}}')
                try {
                    $values = $rs.Fields('Items').Value
                    try {
                        $actual = @()
                        while (!$values.EOF) { $actual += [string]$values.Fields('Value').Value; $values.MoveNext() }
                        if ((($actual | Sort-Object) -join '|') -ne ((@({{expected}}) | Sort-Object) -join '|')) { throw 'Native child values selected a different parent.' }
                    } finally { $values.Close() }
                    $files = $rs.Fields('Files').Value
                    try {
                        $actual = @()
                        while (!$files.EOF) {
                            $name = [string]$files.Fields('FileName').Value
                            $actual += $name
                            $files.Fields('FileData').SaveToFile({{outputPrefix}} + $name)
                            $files.MoveNext()
                        }
                        if ((($actual | Sort-Object) -join '|') -ne ((@({{expected}}) | ForEach-Object { $_ + '.bin' } | Sort-Object) -join '|')) { throw 'Native attachments selected a different parent.' }
                    } finally { $files.Close() }
                } finally { $rs.Close() }
                """);
        }

        script.Add("Write-Output 'NATIVE_PARENT_EXTRACTION_OK'");
        AssertDaoSuccess(session.RunDaoDatabaseScript(path, string.Join(Environment.NewLine, script), DaoTimeout), "NATIVE_PARENT_EXTRACTION_OK");
        foreach (ParentCase item in cases)
        {
            Assert.Equal((byte[])[1, 2, 3, 4], await File.ReadAllBytesAsync(Path.Combine(session.WorkDir, prefix + "-" + item.Marker + ".bin"), Ct));
        }
    }

    private static string Quote(string value) => AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(value);

    private static void AssertDaoSuccess(AccessRoundTripEnvironment.CompactResult result, string marker)
    {
        Assert.True(result.ExitCode == 0, $"DAO failed: {result.StdOut}\n{result.StdErr}");
        Assert.Contains(marker, result.StdOut, StringComparison.Ordinal);
    }

    private sealed record ParentCase(string Column, object? Value, string? ParameterType, string? ParameterValue, int ParentId, string Marker);
}
