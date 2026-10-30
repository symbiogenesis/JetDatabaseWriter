namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Native calculated indexes are not writable even when empty metadata can be appended.</summary>
public sealed class CalculatedKeyRestrictionTests
{
    [Theory]
    [InlineData("ordinary")]
    [InlineData("unique")]
    [InlineData("primary")]
    [InlineData("shortcut")]
    [InlineData("composite")]
    public async Task CalculatedIndexDeclarations_RefuseBeforeChangingDatabase(string kind)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream, DatabaseFormat.AceAccdb, WriteModes.WriterOptions(WriteMode.Direct), leaveOpen: true, TestContext.Current.CancellationToken);
        byte[] before = stream.ToArray();
        IndexDefinition[] indexes = kind == "shortcut" ? [] : [new("IB", kind == "composite" ? ["N", "B"] : ["B"]) { IsUnique = kind == "unique", IsPrimaryKey = kind == "primary" }];
        JetNotSupportedException exception = await Assert.ThrowsAsync<JetNotSupportedException>(async () => await writer.CreateTableAsync(
            "T",
            [new ColumnDefinition("N", typeof(double)), new ColumnDefinition("B", typeof(byte)) { IsCalculated = true, CalculationExpression = "[N]", IsPrimaryKey = kind == "shortcut" }],
            indexes,
            TestContext.Current.CancellationToken));
        Assert.Equal(JetErrorCode.FeatureNotSupported, exception.ErrorCode);
        Assert.Contains("calculated", exception.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(before, stream.ToArray());
    }

    [Fact]
    public async Task AddCalculatedPrimaryKey_RefusesBeforeChangingDatabase()
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream, DatabaseFormat.AceAccdb, WriteModes.WriterOptions(WriteMode.Direct), leaveOpen: true, TestContext.Current.CancellationToken);
        await writer.CreateTableAsync("T", [new ColumnDefinition("N", typeof(double))], TestContext.Current.CancellationToken);
        byte[] before = stream.ToArray();
        await Assert.ThrowsAsync<JetNotSupportedException>(async () => await writer.AddColumnAsync(
            "T", new ColumnDefinition("B", typeof(byte)) { IsCalculated = true, CalculationExpression = "[N]", IsPrimaryKey = true }, TestContext.Current.CancellationToken));
        Assert.Equal(before, stream.ToArray());
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task CalculatedRelationshipKeys_RefuseBeforeChangingDatabase(bool calculatedParent, bool enforce)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream, DatabaseFormat.AceAccdb, WriteModes.WriterOptions(WriteMode.Direct), leaveOpen: true, TestContext.Current.CancellationToken);
        await writer.CreateTableAsync("P", [new ColumnDefinition("N", typeof(int)), new ColumnDefinition("B", typeof(int)) { IsCalculated = true, CalculationExpression = "[N]" }], TestContext.Current.CancellationToken);
        await writer.CreateTableAsync("C", [new ColumnDefinition("N", typeof(int)), new ColumnDefinition("B", typeof(int)) { IsCalculated = true, CalculationExpression = "[N]" }], TestContext.Current.CancellationToken);
        byte[] before = stream.ToArray();
        JetOperationException exception = await Assert.ThrowsAsync<JetOperationException>(async () => await writer.CreateRelationshipAsync(
            new RelationshipDefinition("FK", "P", calculatedParent ? "B" : "N", "C", calculatedParent ? "N" : "B") { EnforceReferentialIntegrity = enforce }, TestContext.Current.CancellationToken));
        Assert.Equal(JetErrorCode.KeyColumnInRelationship, exception.ErrorCode);
        Assert.Equal(before, stream.ToArray());
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task Dao120_EmptyCalculatedIndexMetadataSurvivesWhileWritesRefuse()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.CalculatedKeyRestrictions");
        string path = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        AccessRoundTripEnvironment.CompactResult native = session.RunDaoEngineScript(
            $$"""
            $db=$engine.CreateDatabase({{path}},';LANGID=0x0409;CP=1252;COUNTRY=0',128)
            try {
                foreach($type in @(2,4,7)) {
                    $name='T'+$type
                    $td=$db.CreateTableDef($name)
                    $td.Fields.Append($td.CreateField('N',$type))
                    $db.TableDefs.Append($td)
                    $field=$td.CreateField('B',$type);$field.Expression='[N]';$td.Fields.Append($field)
                    $index=$td.CreateIndex('IB');$index.Fields.Append($index.CreateField('B'));$td.Indexes.Append($index)
                    foreach($literal in @('1','Null','300')) {
                        $refused=$false
                        try {$db.Execute('INSERT INTO '+$name+' (N) VALUES ('+$literal+')',128)}
                        catch {if(@($engine.Errors|ForEach-Object Number) -notcontains 3251){throw};$refused=$true}
                        if(-not $refused){throw 'Native calculated index accepted a row'}
                    }
                    foreach($sql in @(('UPDATE '+$name+' SET N=2'),('DELETE FROM '+$name))) {
                        $db.Execute($sql,128)
                        if($db.RecordsAffected -ne 0){throw 'Empty native mutation affected a row'}
                    }
                }
            } finally {$db.Close()}
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(native.ExitCode == 0, native.StdOut + native.StdErr);
        byte[] before = await File.ReadAllBytesAsync(session.SourcePath, TestContext.Current.CancellationToken);
        await using (AccessWriter writer = await AccessWriter.OpenAsync(
            session.SourcePath, WriteModes.WriterOptions(WriteMode.Direct), TestContext.Current.CancellationToken))
        {
            foreach (string table in new[] { "T2", "T4", "T7" })
            {
                await Assert.ThrowsAsync<JetNotSupportedException>(async () => await writer.InsertRowAsync(table, [1d, DBNull.Value], TestContext.Current.CancellationToken));
                Assert.Equal(0, await writer.UpdateRowsAsync(table, RowCriteria.All(), new RowValues { ["N"] = 2 }, TestContext.Current.CancellationToken));
                Assert.Equal(0, await writer.DeleteRowsAsync(table, RowCriteria.All(), TestContext.Current.CancellationToken));
            }
        }

        Assert.Equal(before, await File.ReadAllBytesAsync(session.SourcePath, TestContext.Current.CancellationToken));
        await using (AccessReader reader = await AccessReader.OpenAsync(
            session.SourcePath, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken))
        {
            Assert.Empty(await reader.Rows("T4", cancellationToken: TestContext.Current.CancellationToken).ToArrayAsync(TestContext.Current.CancellationToken));
        }

        await using (AccessWriter writer = await AccessWriter.OpenAsync(
            session.SourcePath, WriteModes.WriterOptions(WriteMode.Direct), TestContext.Current.CancellationToken))
        {
            await writer.AddColumnAsync("T4", new ColumnDefinition("Extra", typeof(int)), TestContext.Current.CancellationToken);
        }

        AccessRoundTripEnvironment.CompactResult preserved = AccessRoundTripEnvironment.RunDaoDatabaseScript(
            session.SourcePath,
            """
            if($db.TableDefs('T4').Indexes('IB').Fields(0).Name -ne 'B'){throw 'Calculated index metadata was lost'}
            $db.TableDefs('T4').Indexes.Delete('IB')
            $db.Execute('INSERT INTO T4(N) VALUES(1)',128)
            """,
            session.WorkDir,
            TimeSpan.FromMinutes(1));
        Assert.True(preserved.ExitCode == 0, preserved.StdOut + preserved.StdErr);
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task Dao120_CalculatedRelationshipKeysRefuseOnEitherSideAndWithoutEnforcement()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.CalculatedForeignKeys");
        string path = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        AccessRoundTripEnvironment.CompactResult result = session.RunDaoEngineScript(
            $$"""
            $db=$engine.CreateDatabase({{path}},';LANGID=0x0409;CP=1252;COUNTRY=0',128)
            try {
                foreach($attributes in @(0,2)) {
                    foreach($calculatedParent in @($false,$true)) {
                        $name='F'+$attributes+[int]$calculatedParent
                        foreach($side in @('P','C')) {
                            $td=$db.CreateTableDef($name+$side);$td.Fields.Append($td.CreateField('N',4));$db.TableDefs.Append($td)
                            if(($side -eq 'P') -eq $calculatedParent) {
                                $field=$td.CreateField('B',4);$field.Expression='[N]';$td.Fields.Append($field)
                            }
                            if($side -eq 'P' -and $attributes -eq 0) {
                                $index=$td.CreateIndex('PK');$index.Unique=$true
                                $index.Fields.Append($index.CreateField($(if($calculatedParent){'B'}else{'N'})));$td.Indexes.Append($index)
                            }
                        }
                        $relation=$db.CreateRelation($name,$name+'P',$name+'C',$attributes)
                        $field=$relation.CreateField($(if($calculatedParent){'B'}else{'N'}))
                        $field.ForeignName=$(if($calculatedParent){'N'}else{'B'});$relation.Fields.Append($field)
                        $refused=$false
                        try{$db.Relations.Append($relation)}
                        catch{if(@($engine.Errors|ForEach-Object Number) -notcontains 3888){throw};$refused=$true}
                        if(-not $refused){throw 'Native calculated relationship was accepted'}
                    }
                }
            } finally {$db.Close()}
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(result.ExitCode == 0, result.StdOut + result.StdErr);
    }
}
