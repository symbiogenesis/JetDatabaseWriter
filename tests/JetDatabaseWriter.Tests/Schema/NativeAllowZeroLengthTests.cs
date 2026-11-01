namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.TestSupport;
using Xunit;

/// <summary>DAO authored Text, Memo and Hyperlink fields enforce the same empty-string policy.</summary>
public sealed class NativeAllowZeroLengthTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    public Task Dao36Fields_ManagedWritesAndRewritesKeepEmptyStringPolicy(DatabaseFormat format)
        => VerifyNativeAsync(format, useDao36: true);

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public Task Dao120Fields_ManagedWritesAndRewritesKeepEmptyStringPolicy()
        => VerifyNativeAsync(DatabaseFormat.AceAccdb, useDao36: false);

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task Dao120ComplexItem_EmptyStringPreservesAbsentChildPolicy()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.ComplexAllowZeroLength");
        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        RunNative(
            $$"""
            $db=$engine.CreateDatabase({{source}},';LANGID=0x0409;CP=1252;COUNTRY=0',128)
            try {
                $table=$db.CreateTableDef('Parents')
                $table.Fields.Append($table.CreateField('Id',4))
                $table.Fields.Append($table.CreateField('Items',109))
                $db.TableDefs.Append($table)
                $db.Execute('INSERT INTO Parents(Id) VALUES(1)',128)
                $rows=$db.OpenRecordset('Parents')
                try {
                    $rows.Edit();$items=$rows.Fields('Items').Value
                    if($items.Fields('Value').AllowZeroLength){throw 'Unexpected native child policy.'}
                    $items.AddNew();$items.Fields('Value').Value='';$items.Update()
                    $items.Close();$rows.Update()
                }finally{$rows.Close()}
            }finally{$db.Close()}
            """,
            session.WorkDir,
            useDao36: false);

        int itemId;
        await using (AccessReader reader = await TestDatabase.OpenReaderAsync(session.SourcePath, cancellationToken: Ct))
        {
            itemId = Assert.Single(await reader.GetMultiValueItemsAsync("Parents", "Items", Ct)).Id;
        }

        await using (AccessWriter writer = await session.OpenWriterAsync(Ct))
        {
            var key = new Dictionary<string, object?> { ["Id"] = 1 };
            await writer.ReplaceMultiValueItemAsync("Parents", "Items", key, itemId, "changed", Ct);
            await writer.ReplaceMultiValueItemAsync("Parents", "Items", key, itemId, string.Empty, Ct);
            await writer.CreateTableAsync("Managed", [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Items", typeof(object)) { IsMultiValue = true, MultiValueElementType = typeof(string) }], Ct);
            await writer.InsertRowAsync("Managed", [1, null], Ct);
            await writer.AddMultiValueItemAsync("Managed", "Items", key, string.Empty, Ct);
        }

        session.RunDaoCompact();
        RunNative(
            $$"""
            $db=$engine.OpenDatabase({{AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.CompactedPath)}})
            try {
                foreach($name in @('Parents','Managed')) {
                    $rows=$db.OpenRecordset($name)
                    try {
                        $rows.Edit();$items=$rows.Fields('Items').Value
                        if($items.Fields('Value').Value -is [DBNull] -or [string]$items.Fields('Value').Value -ne ''){throw 'Empty complex item changed.'}
                        if($items.Fields('Value').AllowZeroLength){throw 'Native child property was overwritten.'}
                        $items.Edit();$items.Fields('Value').Value='changed';$items.Update()
                        $items.Edit();$items.Fields('Value').Value='';$items.Update()
                        $items.Close();$rows.Update()
                    }finally{$rows.Close()}
                }
            }finally{$db.Close()}
            """,
            session.WorkDir,
            useDao36: false);
    }

    private static async Task VerifyNativeAsync(DatabaseFormat format, bool useDao36)
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.AllowZeroLength", databaseExtension: format == DatabaseFormat.AceAccdb ? ".accdb" : ".mdb");
        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        int version = format switch
        {
            DatabaseFormat.Jet3Mdb => 32,
            DatabaseFormat.Jet4Mdb => 64,
            DatabaseFormat.AceAccdb => 128,
            _ => throw new ArgumentOutOfRangeException(nameof(format)),
        };
        RunNative(
            $$"""
            $db=$engine.CreateDatabase({{source}},';LANGID=0x0409;CP=1252;COUNTRY=0',{{version}})
            try {
                $db.Execute('CREATE TABLE SQLPlain(Id LONG,TextValue TEXT(40),MemoValue MEMO)',128)
                $db.Execute("INSERT INTO SQLPlain(Id,TextValue,MemoValue) VALUES(1,'','')",128)
                foreach($kind in @('Text','Memo','Hyperlink')) {
                    $table=$db.CreateTableDef('T'+$kind)
                    $table.Fields.Append($table.CreateField('Id',4))
                    foreach($name in @('Refuse','Accept')) {
                        $type=12;if($kind -eq 'Text'){$type=10}
                        $field=$table.CreateField($name,$type,40)
                        if($kind -eq 'Hyperlink'){$field.Attributes=$field.Attributes -bor 32768}
                        $field.AllowZeroLength=($name -eq 'Accept')
                        $field.Required=($name -eq 'Accept')
                        $table.Fields.Append($field)
                    }
                    $db.TableDefs.Append($table)
                    $value='ok';if($kind -eq 'Hyperlink'){$value='#'}
                    $db.Execute(('INSERT INTO T'+$kind+" (Id,Refuse,Accept) VALUES (1,'"+$value+"','')"),128)
                    $refused=$false
                    try{$db.Execute(('UPDATE T'+$kind+" SET Refuse='' WHERE Id=1"),128)}catch{
                        if(($_.Exception.HResult -band 65535) -ne 3315){throw}
                        $refused=$true
                    }
                    if(!$refused){throw 'Native field accepted a forbidden empty string.'}
                }
            }finally{$db.Close()}
            """,
            session.WorkDir,
            useDao36);

        await using (AccessWriter writer = await session.OpenWriterAsync(Ct))
        {
            long page = (await writer.Services.Catalog.GetRequiredCatalogEntryAsync("SQLPlain", Ct)).TDefPage;
            ColumnPropertyBlock? properties = await writer.Services.Snapshots.ReadLvPropBlockAsync(page, Ct);
            Assert.Null(properties?.FindPropertyTarget("TextValue", Constants.ColumnPropertyNames.AllowZeroLength));
            Assert.Null(properties?.FindPropertyTarget("MemoValue", Constants.ColumnPropertyNames.AllowZeroLength));
            await writer.InsertRowAsync("SQLPlain", [2, string.Empty, string.Empty], Ct);
            await writer.RenameColumnAsync("SQLPlain", "MemoValue", "Renamed", Ct);
            await writer.InsertRowAsync("SQLPlain", [3, string.Empty, string.Empty], Ct);
            foreach (string kind in new[] { "Text", "Memo", "Hyperlink" })
            {
                string table = "T" + kind;
                JetConstraintException error = await Assert.ThrowsAsync<JetConstraintException>(async () => await writer.InsertRowAsync(table, [2, string.Empty, string.Empty], Ct));
                Assert.Equal(JetErrorCode.ZeroLengthViolation, error.ErrorCode);
                await writer.InsertRowAsync(table, [2, kind == "Hyperlink" ? Hyperlink.Parse("##") : "ok", string.Empty], Ct);
                if (kind == "Hyperlink")
                {
                    await writer.InsertRowAsync(table, [3, Hyperlink.Parse("###"), string.Empty], Ct);
                }

                await writer.RenameColumnAsync(table, "Accept", "Accepted", Ct);
                await writer.UpdateRowsAsync(table, "Id", 1, new Dictionary<string, object?> { ["Accepted"] = string.Empty }, Ct);
                ColumnDefinition refuse = kind switch
                {
                    "Text" => new ColumnDefinition("Refuse", typeof(string), 40),
                    "Memo" => new ColumnDefinition("Refuse", typeof(string)),
                    _ => new ColumnDefinition("Refuse", typeof(Hyperlink)),
                };
                await writer.CreateTableAsync("Managed" + kind, [new ColumnDefinition("Id", typeof(int)), refuse, refuse.WithName("Accepted") with { AllowZeroLength = true, IsNullable = false }], Ct);
                await writer.InsertRowAsync("Managed" + kind, [1, kind == "Hyperlink" ? Hyperlink.Parse("#") : "ok", string.Empty], Ct);
                await writer.InsertRowAsync("Managed" + kind, [2, kind == "Hyperlink" ? Hyperlink.Parse("##") : "ok", string.Empty], Ct);
                if (kind == "Hyperlink")
                {
                    await writer.InsertRowAsync("Managed" + kind, [3, Hyperlink.Parse("###"), string.Empty], Ct);
                }
            }
        }

        string compacted = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.CompactedPath);
        RunNative(
            $$"""
            foreach($path in @({{source}},{{compacted}})) {
                if($path -eq {{compacted}}){$engine.CompactDatabase({{source}},{{compacted}},';LANGID=0x0409;CP=1252;COUNTRY=0',{{version}})}
                $db=$engine.OpenDatabase($path)
                try {
                    $plain=$db.OpenRecordset('SQLPlain',2)
                    try{
                        $count=0
                        while(!$plain.EOF){
                            if($plain.Fields('TextValue').Value -is [DBNull] -or [string]$plain.Fields('TextValue').Value -ne ''){throw 'Unconstrained Text changed.'}
                            if($plain.Fields('Renamed').Value -is [DBNull] -or [string]$plain.Fields('Renamed').Value -ne ''){throw 'Unconstrained Memo changed.'}
                            $count++;$plain.MoveNext()
                        }
                        if($count -ne 3){throw 'Unconstrained rows were lost.'}
                    }finally{$plain.Close()}
                    foreach($name in @('TText','TMemo','THyperlink','ManagedText','ManagedMemo','ManagedHyperlink')) {
                        $table=$db.TableDefs($name)
                        if($table.Fields('Refuse').AllowZeroLength){throw 'False policy was lost.'}
                        if(!$table.Fields('Accepted').AllowZeroLength){throw 'True policy was lost.'}
                        if(!$table.Fields('Accepted').Required){throw 'Required policy was lost.'}
                        $db.Execute(('UPDATE '+$name+" SET Accepted='' WHERE Id=1"),128)
                        $refused=$false
                        try{$db.Execute(('UPDATE '+$name+" SET Refuse='' WHERE Id=1"),128)}catch{
                            if(($_.Exception.HResult -band 65535) -ne 3315){throw}
                            $refused=$true
                        }
                        if(!$refused){throw 'Native field lost empty-string enforcement.'}
                        $rows=$db.OpenRecordset($name,2)
                        try{
                            $count=0
                            while(!$rows.EOF){
                                if($rows.Fields('Accepted').Value -is [DBNull] -or [string]$rows.Fields('Accepted').Value -ne ''){throw ('Zero-length value changed: '+$name+' Id='+$rows.Fields('Id').Value+' Value=['+$rows.Fields('Accepted').Value+'] Type='+$rows.Fields('Accepted').Value.GetType().Name+' Path='+$path)}
                                if($name.EndsWith('Hyperlink') -and [string]$rows.Fields('Refuse').Value -ne ('#' * [int]$rows.Fields('Id').Value)){throw 'Nonempty hyperlink spelling changed.'}
                                $count++;$rows.MoveNext()
                            }
                            $expected=2;if($name.EndsWith('Hyperlink')){$expected=3}
                            if($count -ne $expected){throw 'Rows were lost.'}
                        }finally{$rows.Close()}
                    }
                }finally{$db.Close()}
            }
            """,
            session.WorkDir,
            useDao36);

        await using AccessReader reader = await TestDatabase.OpenReaderAsync(session.CompactedPath, cancellationToken: Ct);
        foreach (string kind in new[] { "Text", "Memo", "Hyperlink" })
        {
            Assert.True(Assert.Single(await reader.GetColumnMetadataAsync("T" + kind, Ct), c => c.Name == "Accepted").AllowZeroLength);
            Assert.Equal(kind == "Hyperlink" ? 3 : 2, (await reader.Rows("T" + kind, cancellationToken: Ct).ToListAsync(Ct)).Count);
        }
    }

    private static void RunNative(string script, string directory, bool useDao36)
    {
        if (useDao36)
        {
            PowerShellRunResult result = Jet3RoundTripEnvironment.RunEngineScript(script, directory, TimeSpan.FromMinutes(2));
            Assert.True(result.ExitCode == 0 && !result.TimedOut && result.OutputComplete, result.StandardOutput + result.StandardError);
        }
        else
        {
            AccessRoundTripEnvironment.CompactResult result = AccessRoundTripEnvironment.RunDaoEngineScript(script, directory, TimeSpan.FromMinutes(2));
            Assert.True(result.ExitCode == 0, result.StdOut + result.StdErr);
        }
    }
}
