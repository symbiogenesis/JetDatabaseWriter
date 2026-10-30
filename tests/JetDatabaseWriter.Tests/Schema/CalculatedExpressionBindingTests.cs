namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Expressions;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Calculated declarations bind every field before schema or row mutation.</summary>
public sealed class CalculatedExpressionBindingTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData("[Missing]")]
    [InlineData("Missing")]
    [InlineData("vbTrue")]
    [InlineData("IIf(True,1,[Missing])")]
    [InlineData("[Cached]")]
    [InlineData("IIf(False,[Cached],1)")]
    public async Task Create_InvalidBindingRefusesBeforeMutation(string expression)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct);
        byte[] before = stream.ToArray();
        ArgumentException error = await Assert.ThrowsAsync<ArgumentException>(async () => await writer.CreateTableAsync("T", [new("N", typeof(int)), Calculated("Cached", expression)], Ct));
        Assert.Equal("columns", error.ParamName);
        Assert.Equal(before, stream.ToArray());
    }

    [Theory]
    [InlineData("[Missing]")]
    [InlineData("vbTrue")]
    [InlineData("IIf(False,[Cached],1)")]
    public async Task Add_InvalidBindingRefusesBeforeMutation(string expression)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct);
        await writer.CreateTableAsync("T", [new("N", typeof(int))], Ct);
        byte[] before = stream.ToArray();
        ArgumentException error = await Assert.ThrowsAsync<ArgumentException>(async () => await writer.AddColumnAsync("T", Calculated("Cached", expression), Ct));
        Assert.Equal("column", error.ParamName);
        Assert.Equal(before, stream.ToArray());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Create_IndirectCycleRefusesEvenInUnusedBranch(bool unusedBranch)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct);
        byte[] before = stream.ToArray();
        await Assert.ThrowsAsync<ArgumentException>(async () => await writer.CreateTableAsync("T", [Calculated("A", unusedBranch ? "IIf(False,[B],1)" : "[B]"), Calculated("B", "[A]")], Ct));
        Assert.Equal(before, stream.ToArray());
    }

    [Theory]
    [InlineData(ColumnType.BinaryType)]
    [InlineData(ColumnType.OleType)]
    [InlineData(ColumnType.ComplexType)]
    public async Task Create_ProhibitedSourceTypeRefusesEvenInUnusedBranch(ColumnType sourceType)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct);
        ColumnDefinition source = sourceType == ColumnType.ComplexType ? new ColumnDefinition("Source", typeof(byte[])) { IsAttachment = true } : new ColumnDefinition("Source", typeof(byte[]), sourceType == ColumnType.BinaryType ? 16 : 0);
        byte[] before = stream.ToArray();
        await Assert.ThrowsAsync<ArgumentException>(async () => await writer.CreateTableAsync("T", [source, Calculated("Cached", "IIf(True,1,Len([Source]))")], Ct));
        Assert.Equal(before, stream.ToArray());
    }

    [Theory]
    [InlineData(ColumnType.BinaryType)]
    public async Task Create_ProhibitedCalculatedSourceUsesEffectiveResultType(ColumnType sourceType)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct);
        var source = new ColumnDefinition("Source", typeof(byte[]), 16) { IsCalculated = true, CalculatedResultType = (byte)sourceType, CalculationExpression = "[N]" };
        byte[] before = stream.ToArray();
        await Assert.ThrowsAsync<ArgumentException>(async () => await writer.CreateTableAsync("T", [new("N", typeof(int)), source, Calculated("Cached", "IIf(True,1,Len([Source]))")], Ct));
        Assert.Equal(before, stream.ToArray());
    }

    [Fact]
    public async Task BareConstantSpelling_BindsFieldAndSurvivesRename()
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct))
        {
            await writer.CreateTableAsync("T", [new("vbTrue", typeof(int)), Calculated("Cached", "vbTrue+1"), Calculated("Keyword", "Yes+No+On+Off")], Ct);
            await writer.InsertRowAsync("T", new RowValues { ["vbTrue"] = 5 }, Ct);
            await writer.RenameColumnAsync("T", "vbTrue", "Amount", Ct);
            byte[] before = stream.ToArray();
            await Assert.ThrowsAsync<JetOperationException>(async () => await writer.DropColumnAsync("T", "Amount", Ct));
            Assert.Equal(before, stream.ToArray());
        }

        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct);
        object[] row = Assert.Single(await reader.Rows("T", cancellationToken: Ct).ToArrayAsync(Ct));
        Assert.Equal(6, row[1]);
        Assert.Equal(-2, row[2]);
        Assert.Equal("[Amount]+1", Assert.Single(await reader.GetColumnMetadataAsync("T", Ct), column => column.Name == "Cached").CalculationExpression);
    }

    [Theory]
    [InlineData(ColumnType.BinaryType, false)]
    [InlineData(ColumnType.OleType, false)]
    [InlineData(ColumnType.ComplexType, false)]
    [InlineData(ColumnType.BinaryType, true)]
    [InlineData(ColumnType.OleType, true)]
    public void PersistedPlan_RejectsProhibitedReferenceBeforeEvaluatingUnusedBranch(ColumnType sourceType, bool calculatedSource)
    {
        var table = new TableDef { Columns = [new ColumnInfo { Name = "Source" }, new ColumnInfo { Name = "Cached" }] };
        var context = new CalculatedExpressionEvaluationContext(
            table,
            [new() { Name = "Source", StorageType = calculatedSource ? ColumnType.TextType : sourceType, IsCalculated = calculatedSource, CalculatedResultType = sourceType, CalculationExpression = "1", ClrType = typeof(byte[]) }, new() { Name = "Cached", IsCalculated = true, ClrType = typeof(int), CalculationExpression = "IIf(True,1,Len([Source]))" }],
            [new byte[] { 1 }, DBNull.Value],
            "T");
        Assert.Throws<JetNotSupportedException>(() => context.EvaluateColumn(1));
    }

    [Fact]
    public void PersistedPlan_RejectsCycleBeforeEvaluatingUnusedBranch()
    {
        var table = new TableDef { Columns = [new ColumnInfo { Name = "A" }, new ColumnInfo { Name = "B" }] };
        var context = new CalculatedExpressionEvaluationContext(
            table,
            [new() { Name = "A", IsCalculated = true, ClrType = typeof(int), CalculationExpression = "IIf(False,[B],1)" }, new() { Name = "B", IsCalculated = true, ClrType = typeof(int), CalculationExpression = "[A]" }],
            [DBNull.Value, DBNull.Value],
            "T");
        Assert.Throws<JetOperationException>(() => context.EvaluateColumn(0));
    }

    [Fact]
    public void GenericExpression_KeepsBuiltinConstants()
    {
        CalculatedExpressionNode plan = CalculatedExpressionParser.Parse("vbTrue", isCalculatedColumn: false);
        Assert.Equal(-1, Assert.IsType<int>(plan.Evaluate(new CalculatedExpressionEvaluationContext(new TableDef(), [], []))));
    }

    [Theory]
    [InlineData("Alias.N")]
    [InlineData("[Old].[N]")]
    public void PersistedPlan_PreservesBareAliasAndStaleBracketedQualifier(string expression)
    {
        var table = new TableDef { Columns = [new ColumnInfo { Name = "N" }, new ColumnInfo { Name = "Cached" }] };
        var context = new CalculatedExpressionEvaluationContext(
            table,
            [new() { Name = "N", StorageType = ColumnType.LongIntegerType, ClrType = typeof(int) }, new() { Name = "Cached", IsCalculated = true, ClrType = typeof(int), CalculationExpression = expression }],
            [7, DBNull.Value],
            "New");
        Assert.Equal(expression == "Alias.N" ? 7 : DBNull.Value, context.EvaluateColumn(1));
    }

    [Fact]
    public async Task Add_ProhibitedSourceRefusesBeforeMutation()
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct);
        await writer.CreateTableAsync("T", [new("Source", typeof(byte[]))], Ct);
        byte[] before = stream.ToArray();
        await Assert.ThrowsAsync<ArgumentException>(async () => await writer.AddColumnAsync("T", Calculated("Cached", "IIf(True,1,Len([Source]))"), Ct));
        Assert.Equal(before, stream.ToArray());
    }

    [Fact]
    public async Task MemoSource_RemainsAvailable()
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct))
        {
            await writer.CreateTableAsync("T", [new("Source", typeof(string)), Calculated("Cached", "Len([Source])")], Ct);
            await writer.InsertRowAsync("T", new RowValues { ["Source"] = "memo" }, Ct);
        }

        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct);
        Assert.Equal(4, Assert.Single(await reader.Rows("T", cancellationToken: Ct).ToArrayAsync(Ct))[1]);
    }

    [Fact(Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason, SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable), SkipType = typeof(AccessRoundTripEnvironment))]
    public async Task CompleteAcyclicColumnSet_NativeCanReadUpdateAndCompactForwardDependency()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.CalculatedForwardBinding");
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(session.SourcePath, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, Ct))
        {
            await writer.CreateTableAsync("T", [new("N", typeof(int)), Calculated("A", "[B]+1"), Calculated("B", "[N]+1")], Ct);
            await writer.InsertRowAsync("T", new RowValues { ["N"] = 1 }, Ct);
        }

        AccessRoundTripEnvironment.CompactResult accepted = session.RunDaoDatabaseScriptThenCompact(
            """
            $rs=$db.OpenRecordset('T');try{if($rs.Fields('A').Value -ne 3){throw 'Forward reference cache was not accepted'}}finally{$rs.Close()}
            $db.Execute('UPDATE [T] SET [N]=4',128)
            $rs=$db.OpenRecordset('T');try{if($rs.Fields('A').Value -ne 6 -or $rs.Fields('B').Value -ne 5){throw 'Forward dependency did not recalculate'}}finally{$rs.Close()}
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(accepted.ExitCode == 0, accepted.StdOut + accepted.StdErr);
    }

    [Fact(Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason, SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable), SkipType = typeof(AccessRoundTripEnvironment))]
    public async Task NativeDeclaration_BindsFieldNamesAndRejectsInvalidReferences()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.CalculatedBinding");
        AccessRoundTripEnvironment.CompactResult result = session.RunDaoEngineScript(
            $$$"""
            $db=$engine.CreateDatabase({{{AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath)}}},';LANGID=0x0409;CP=1252;COUNTRY=0',128)
            try {
                $db.Execute('CREATE TABLE [T]([vbTrue] LONG,[Yes] LONG,[Blob] LONGBINARY)',128)
                $db.TableDefs.Refresh();$td=$db.TableDefs('T')
                $td.Fields.Append($td.CreateField('Files',101))
                foreach($item in @(@('Unknown','[Missing]',3799),@('Self','[Self]',3866),@('BinarySource','IIf(True,1,Len([Blob]))',3885),@('ComplexSource','IIf(True,1,Len([Files]))',3885))) {
                    $f=$td.CreateField($item[0],4);$f.Expression=$item[1];$refused=$false
                    try {$td.Fields.Append($f)} catch {if(@($engine.Errors|ForEach-Object Number) -notcontains $item[2]){throw};$refused=$true}
                    if(-not $refused){throw ('Native declaration accepted '+$item[1])}
                }
                foreach($item in @(@('Bare','vbTrue+1'),@('Keyword','Yes+No+On+Off'))) {$f=$td.CreateField($item[0],4);$f.Expression=$item[1];$td.Fields.Append($f)}
                $db.Execute('INSERT INTO [T]([vbTrue],[Yes]) VALUES(5,9)',128)
                $rs=$db.OpenRecordset('T');try{if($rs.Fields('Bare').Value -ne 6 -or $rs.Fields('Keyword').Value -ne -2){throw 'Native binding changed'}}finally{$rs.Close()}
            } finally {$db.Close()}
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(result.ExitCode == 0, result.StdOut + result.StdErr);
    }

    private static ColumnDefinition Calculated(string name, string expression)
        => new(name, typeof(int)) { IsCalculated = true, CalculationExpression = expression };
}
