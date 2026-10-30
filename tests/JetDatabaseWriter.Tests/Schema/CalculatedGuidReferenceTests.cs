namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Schema.Expressions;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>GUID references retain their binary identity while participating in native String Variant operations.</summary>
public sealed class CalculatedGuidReferenceTests
{
    [Theory]
    [InlineData("[G]+\"!\"", "{00112233-4455-6677-8899-AABBCCDDEEFF}!")]
    [InlineData("\"!\"+[G]", "!{00112233-4455-6677-8899-AABBCCDDEEFF}")]
    [InlineData("[G]+[G]", "{00112233-4455-6677-8899-AABBCCDDEEFF}{00112233-4455-6677-8899-AABBCCDDEEFF}")]
    [InlineData("TypeName([G])", "String")]
    [InlineData("VarType([G])", 8)]
    [InlineData("[G]=\"{00112233-4455-6677-8899-AABBCCDDEEFF}\"", true)]
    [InlineData("[G]=\"{00112233-4455-6677-8899-aabbccddeeff}\"", true)]
    [InlineData("Len([G])", 38)]
    public void GuidReferences_UseNativeTextOperations(string expression, object expected)
    {
        var definition = new TableDef { Columns = [new ColumnInfo { Name = "G", Type = ColumnType.GuidType }] };
        ColumnConstraint[] constraints = [new() { Name = "G", StorageType = ColumnType.GuidType, ClrType = typeof(Guid) }];
        var context = new CalculatedExpressionEvaluationContext(definition, constraints, [Guid.Parse("00112233-4455-6677-8899-aabbccddeeff")]);
        Assert.Equal(expected, CalculatedExpressionParser.Parse(expression, isCalculatedColumn: false).Evaluate(context));
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task Dao120_GuidStringOperationsRetainGuidBinaryCachesAfterWriterUpdate()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.GuidReferences");
        string path = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        AccessRoundTripEnvironment.CompactResult created = session.RunDaoEngineScript(
            $$$"""
            $db = $engine.CreateDatabase({{{path}}}, ';LANGID=0x0409;CP=1252;COUNTRY=0',128)
            try {
                $db.Execute('CREATE TABLE T (Id LONG,G GUID)')
                $tdf = $db.TableDefs('T')
                foreach ($case in @(@('Prefix',10,'[G]+"!"'),@('DoubleGuid',10,'[G]+[G]'),@('EqualsText',1,'[G]="{00112233-4455-6677-8899-AABBCCDDEEFF}"'),@('B',9,'[G]'),@('CG',15,'[G]'),@('PrefixCalculatedGuid',10,'[CG]+"!"'))) {
                    $field = $tdf.CreateField($case[0],$case[1],255)
                    $field.Expression = $case[2]
                    $tdf.Fields.Append($field)
                }
                $db.Execute('INSERT INTO T(Id,G) VALUES (1,{guid {00112233-4455-6677-8899-AABBCCDDEEFF}})')
                $rs = $db.OpenRecordset('SELECT TypeName([G]) AS T,VarType([G]) AS V,TypeName([CG]) AS CT,VarType([CG]) AS CV FROM T')
                try { if ($rs.Fields('T').Value -ne 'String' -or $rs.Fields('CT').Value -ne 'String' -or $rs.Fields('V').Value -ne 8 -or $rs.Fields('CV').Value -ne 8) { throw 'Native GUID reference subtype changed' } }
                finally { $rs.Close() }
            } finally { $db.Close() }
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(created.ExitCode == 0, created.StdOut + created.StdErr);
        await using (AccessWriter writer = await AccessWriter.OpenAsync(
            session.SourcePath, new AccessWriterOptions { UseLockFile = false }, TestContext.Current.CancellationToken))
        {
            Assert.Equal(1, await writer.UpdateRowsAsync("T", "Id", 1, new Dictionary<string, object?> { ["Id"] = 2 }, TestContext.Current.CancellationToken));
        }

        string compacted = session.CreateDatabasePath("compacted");
        AccessRoundTripEnvironment.CompactResult checkedCaches = AccessRoundTripEnvironment.RunDaoDatabaseScriptThenCompact(
            session.SourcePath,
            compacted,
            """
            $rs = $db.OpenRecordset('T',2)
            try {
                $expected = '{00112233-4455-6677-8899-AABBCCDDEEFF}'
                if ($rs.Fields('Prefix').Value -ne ($expected+'!') -or $rs.Fields('PrefixCalculatedGuid').Value -ne ($expected+'!')) { throw 'Writer GUID + text differs' }
                if ($rs.Fields('DoubleGuid').Value -ne ($expected+$expected) -or -not $rs.Fields('EqualsText').Value) { throw 'Writer GUID String Variant operations differ' }
                if ([Convert]::ToHexString($rs.Fields('B').Value) -ne '33221100554477668899AABBCCDDEEFF') { throw 'Writer lost GUID binary identity' }
                $rs.Edit(); $rs.Fields('Id').Value = 3; $rs.Update()
            } finally { $rs.Close() }
            """,
            session.WorkDir,
            TimeSpan.FromMinutes(1));
        Assert.True(checkedCaches.ExitCode == 0, checkedCaches.StdOut + checkedCaches.StdErr);
        await using AccessReader reader = await AccessReader.OpenAsync(
            compacted, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken);
        await foreach (object[] row in reader.Rows("T", cancellationToken: TestContext.Current.CancellationToken))
        {
            Assert.Equal("{00112233-4455-6677-8899-AABBCCDDEEFF}!", row[2]);
            Assert.Equal("33221100554477668899AABBCCDDEEFF", Convert.ToHexString(Assert.IsType<byte[]>(row[5])));
            Assert.Equal("{00112233-4455-6677-8899-AABBCCDDEEFF}!", row[7]);
        }
    }
}
