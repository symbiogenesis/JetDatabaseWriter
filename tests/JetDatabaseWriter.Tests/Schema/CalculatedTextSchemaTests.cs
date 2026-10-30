namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>New calculated Text fields use the native width instead of a caller hint.</summary>
public sealed class CalculatedTextSchemaTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(0)]
    [InlineData(3)]
    [InlineData(80)]
    [InlineData(243)]
    [InlineData(255)]
    [InlineData(int.MaxValue)]
    public async Task NewCalculatedText_UsesNativeWidthAndRetainsItThroughSchemaRewrite(int requestedWidth)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct))
        {
            await writer.CreateTableAsync("T", [new("Id", typeof(int)), new("Source", typeof(string), maxLength: 255), new("Cached", typeof(string), requestedWidth) { IsCalculated = true, CalculatedResultType = (byte)ColumnType.TextType, CalculationExpression = "[Source]" }], Ct);
            await writer.InsertRowAsync("T", new RowValues { ["Id"] = 1, ["Source"] = new string('A', 243) }, Ct);
            await writer.AddColumnAsync("T", new ColumnDefinition("Added", typeof(int)), Ct);
        }

        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct);
        ColumnMetadata metadata = Assert.Single(await reader.GetColumnMetadataAsync("T", Ct), column => column.Name == "Cached");
        Assert.Equal(486, metadata.MaxLength);
        Assert.Equal(new string('A', 243), Assert.Single(await reader.Rows("T", cancellationToken: Ct).ToArrayAsync(Ct))[2]);
    }

    [Fact]
    public void PreservedCalculatedText_KeepsItsNativeDescriptorWidth()
    {
        var original = new ColumnInfo { Name = "Cached", Type = ColumnType.TextType, Size = 183, ExtraFlags = Constants.CalculatedColumn.ExtFlagMask, CalculatedResultType = ColumnType.TextType };
        var column = new ColumnDefinition("Cached", typeof(string), maxLength: 3) { IsCalculated = true, CalculationExpression = "1", SourceColumn = original };
        TableDef result = TDefPageBuilder.BuildTableDefinition([column], JetFormat.ForNewDatabase(DatabaseFormat.AceAccdb));
        Assert.Equal(183, result.Columns[0].Size);
    }

    [Fact(Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason, SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable), SkipType = typeof(AccessRoundTripEnvironment))]
    public async Task NativeCalculatedText_WidthMatchesWriterAndSurvivesNativeUpdateCompact()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.CalculatedTextWidth");
        AccessRoundTripEnvironment.CompactResult authored = session.RunDaoEngineScript(
            $$"""
            $db=$engine.CreateDatabase({{AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath)}},';LANGID=0x0409;CP=1252;COUNTRY=0',128)
            try {
                $db.Execute('CREATE TABLE [Native]([Source] TEXT(255))',128)
                $db.TableDefs.Refresh()
                $field=$db.TableDefs('Native').CreateField('Cached',10,3)
                $field.Expression='[Source]'
                $db.TableDefs('Native').Fields.Append($field)
                if($db.TableDefs('Native').Fields('Cached').Size -ne 243){throw 'Native calculated Text width changed'}
            } finally {$db.Close()}
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(authored.ExitCode == 0, authored.StdOut + authored.StdErr);
        await using (AccessWriter writer = await AccessWriter.OpenAsync(session.SourcePath, new AccessWriterOptions { UseLockFile = false }, Ct))
        {
            await writer.CreateTableAsync("Written", [new("Source", typeof(string), maxLength: 255), new("Cached", typeof(string), maxLength: 3) { IsCalculated = true, CalculationExpression = "[Source]" }], Ct);
            await writer.InsertRowAsync("Written", new RowValues { ["Source"] = new string('A', 243) }, Ct);
        }

        AccessRoundTripEnvironment.CompactResult accepted = session.RunDaoDatabaseScriptThenCompact(
            """
            if($db.TableDefs('Written').Fields('Cached').Size -ne $db.TableDefs('Native').Fields('Cached').Size){throw 'Writer declared a different calculated width'}
            $rs=$db.OpenRecordset('Written')
            try {if($rs.Fields('Cached').Value.Length -ne 243){throw 'Writer truncated the calculated cache'}} finally {$rs.Close()}
            $db.Execute('UPDATE [Written] SET [Source]="updated"',128)
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(accepted.ExitCode == 0, accepted.StdOut + accepted.StdErr);
    }
}
