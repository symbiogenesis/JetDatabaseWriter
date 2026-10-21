namespace JetDatabaseWriter.Tests.ComplexColumns;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Native complex recordsets accept individual writer changes and retain their item identities.</summary>
public sealed class ComplexItemMutationDaoTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task NativeItems_ReplaceRemoveExtractAndCompactPreserveIdentities()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.ComplexItemMutation");
        string input = Path.Combine(session.WorkDir, "native.txt");
        await File.WriteAllBytesAsync(input, [1, 2, 3], Ct);
        AssertDaoSuccess(session.RunDaoCreateDatabaseScript(
            session.SourcePath,
            ";LANGID=0x0409;CP=1252;COUNTRY=0",
            $$"""
            $table = $db.CreateTableDef('Parents')
            $table.Fields.Append($table.CreateField('Id', 4))
            $table.Fields.Append($table.CreateField('Files', 101))
            $table.Fields.Append($table.CreateField('Items', 109))
            $db.TableDefs.Append($table)
            $db.Execute('INSERT INTO [Parents] ([Id]) VALUES (1)', 128)
            $db.Execute('INSERT INTO [Parents] ([Id]) VALUES (2)', 128)
            $rs = $db.OpenRecordset('Parents')
            try {
                while (!$rs.EOF) {
                    $rs.Edit()
                    $files = $rs.Fields('Files').Value
                    $items = $rs.Fields('Items').Value
                    try {
                        foreach ($text in @('one','two')) {
                            $files.AddNew()
                            $files.Fields('FileData').LoadFromFile({{Quote(input)}})
                            $files.Fields('FileName').Value = $text + '.txt'
                            $files.Update()
                            $items.AddNew()
                            $items.Fields('Value').Value = $text
                            $items.Update()
                        }
                    } finally { $files.Close(); $items.Close() }
                    $rs.Update()
                    $rs.MoveNext()
                }
            } finally { $rs.Close() }
            Write-Output 'NATIVE_ITEMS_CREATED'
            """,
            TimeSpan.FromMinutes(2)));

        AttachmentRecord[] originalFiles;
        MultiValueItem[] originalItems;
        await using (AccessReader reader = await AccessReader.OpenAsync(session.SourcePath, new AccessReaderOptions { UseLockFile = false }, Ct))
        {
            originalFiles = [.. await reader.GetAttachmentsAsync("Parents", "Files", Ct)];
            originalItems = [.. await reader.GetMultiValueItemsAsync("Parents", "Items", Ct)];
        }

        int firstReference = originalFiles.Min(file => file.ConceptualTableId);
        AttachmentRecord replaced = Assert.Single(originalFiles, file => file.ConceptualTableId == firstReference && file.FileName == "one.txt");
        AttachmentRecord removed = Assert.Single(originalFiles, file => file.ConceptualTableId == firstReference && file.FileName == "two.txt");
        MultiValueItem replacedValue = Assert.Single(originalItems, item => item.ConceptualTableId == firstReference && Equals(item.Value, "one"));
        MultiValueItem removedValue = Assert.Single(originalItems, item => item.ConceptualTableId == firstReference && Equals(item.Value, "two"));
        byte[] replacement = new byte[20_000];
        for (int index = 0; index < replacement.Length; index++)
        {
            replacement[index] = (byte)(index & 0xFF);
        }

        var key = new Dictionary<string, object?> { ["Id"] = 1 };
        await using (AccessWriter writer = await session.OpenWriterAsync(Ct))
        {
            await writer.ReplaceAttachmentAsync("Parents", "Files", key, replaced.Id, new AttachmentInput("changed.zip", replacement), Ct);
            await writer.RemoveAttachmentAsync("Parents", "Files", key, removed.Id, Ct);
            await writer.ReplaceMultiValueItemAsync("Parents", "Items", key, replacedValue.Id, "changed", Ct);
            await writer.RemoveMultiValueItemAsync("Parents", "Items", key, removedValue.Id, Ct);
        }

        string output = Path.Combine(session.WorkDir, "extracted.zip");
        AssertDaoSuccess(session.RunDaoDatabaseScript(
            session.SourcePath,
            $$"""
            $rs = $db.OpenRecordset('SELECT * FROM [Parents] WHERE [Id]=1')
            try {
                $rs.Edit()
                $files = $rs.Fields('Files').Value
                $items = $rs.Fields('Items').Value
                try {
                    $files.MoveLast(); $files.MoveFirst()
                    $items.MoveLast(); $items.MoveFirst()
                    if ($files.RecordCount -ne 1 -or $items.RecordCount -ne 1) { throw 'Wrong item count.' }
                    if ($files.Fields('FileName').Value -ne 'changed.zip') { throw 'Wrong file.' }
                    if ($items.Fields('Value').Value -ne 'changed') { throw 'Wrong value.' }
                    $files.Fields('FileData').SaveToFile({{Quote(output)}})
                    $items.Edit()
                    $items.Fields('Value').Value = 'native-final'
                    $items.Update()
                } finally { $files.Close(); $items.Close() }
                $rs.Update()
            } finally { $rs.Close() }
            Write-Output 'NATIVE_ITEMS_ACCEPTED'
            """,
            TimeSpan.FromMinutes(2)));
        Assert.Equal(replacement, await File.ReadAllBytesAsync(output, Ct));
        session.RunDaoCompact();
        await using AccessReader compacted = await AccessReader.OpenAsync(session.CompactedPath, new AccessReaderOptions { UseLockFile = false }, Ct);
        IReadOnlyList<AttachmentRecord> filesAfter = await compacted.GetAttachmentsAsync("Parents", "Files", Ct);
        IReadOnlyList<MultiValueItem> itemsAfter = await compacted.GetMultiValueItemsAsync("Parents", "Items", Ct);
        Assert.Equal(3, filesAfter.Count);
        Assert.Equal(3, itemsAfter.Count);
        AttachmentRecord changed = Assert.Single(filesAfter, file => file.Id == replaced.Id);
        Assert.Equal("changed.zip", changed.FileName);
        Assert.Equal(replacement, changed.FileData);
        Assert.Equal("native-final", Assert.Single(itemsAfter, item => item.Id == replacedValue.Id).Value);
        Assert.DoesNotContain(filesAfter, file => file.Id == removed.Id);
        Assert.DoesNotContain(itemsAfter, item => item.Id == removedValue.Id);
        foreach (AttachmentRecord sibling in originalFiles.Where(file => file.ConceptualTableId != firstReference))
        {
            AttachmentRecord actual = Assert.Single(filesAfter, file => file.Id == sibling.Id);
            Assert.Equal(sibling.FileName, actual.FileName);
            Assert.Equal(sibling.FileData, actual.FileData);
        }
    }

    private static string Quote(string value) => AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(value);

    private static void AssertDaoSuccess(AccessRoundTripEnvironment.CompactResult result)
        => Assert.True(result.ExitCode == 0, $"DAO failed: {result.StdOut}\n{result.StdErr}");
}
