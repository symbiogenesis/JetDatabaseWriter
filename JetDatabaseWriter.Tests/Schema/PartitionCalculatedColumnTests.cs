namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Partition is accepted in stored rules but refused in table calculated fields.</summary>
public sealed class PartitionCalculatedColumnTests
{
    [Theory]
    [InlineData("Partition([Id],0,10,2)")]
    [InlineData("partition ([Id],0,10,2)")]
    [InlineData("IIf([Id]>0,Partition([Id],0,10,2),\"\")")]
    public async Task CreateAndAddPartitionCalculation_RefuseWithoutChangingImage(string expression)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        byte[] beforeCreate = stream.ToArray();
        await Assert.ThrowsAsync<NotSupportedException>(async () => await writer.CreateTableAsync(
            "Rejected",
            [new ColumnDefinition("Id", typeof(int)), CalculatedColumn(expression)],
            TestContext.Current.CancellationToken));
        Assert.Equal(beforeCreate, stream.ToArray());

        await writer.CreateTableAsync("T", [new ColumnDefinition("Id", typeof(int))], TestContext.Current.CancellationToken);
        byte[] beforeAdd = stream.ToArray();
        await Assert.ThrowsAsync<NotSupportedException>(async () => await writer.AddColumnAsync(
            "T", CalculatedColumn(expression), TestContext.Current.CancellationToken));
        Assert.Equal(beforeAdd, stream.ToArray());
        await writer.InsertRowAsync("T", [1], TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task PersistedPartitionCalculation_InsertAndUpdateRefuseWithoutChangingImage()
    {
        const string placeholderExpression = "Xartition([Id],0,10,2)";
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("T", [new ColumnDefinition("Id", typeof(int)), CalculatedColumn(placeholderExpression)], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("T", [1, " 0: 1"], TestContext.Current.CancellationToken);
        }

        // Model a stored expression without relying on CreateTable to accept it.
        byte[] image = stream.ToArray();
        int expressionOffset = image.AsSpan().IndexOf(Encoding.Unicode.GetBytes(placeholderExpression));
        Assert.True(expressionOffset >= 0, "The persisted expression must be present in the fixture.");
        image[expressionOffset] = (byte)'P';
        stream.Position = 0;
        await stream.WriteAsync(image, TestContext.Current.CancellationToken);
        stream.Position = 0;
        await using AccessWriter reopened = await AccessWriter.OpenAsync(
            stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        await Assert.ThrowsAsync<NotSupportedException>(async () => await reopened.InsertRowAsync(
            "T", [2, DBNull.Value], TestContext.Current.CancellationToken));
        Assert.Equal(image, stream.ToArray());
        await Assert.ThrowsAsync<NotSupportedException>(async () => await reopened.UpdateRowsAsync(
            "T", "Id", 1, new Dictionary<string, object?> { ["Id"] = 2 }, TestContext.Current.CancellationToken));
        Assert.Equal(image, stream.ToArray());
    }

    [Fact]
    public async Task PartitionTextAndFieldName_AreAcceptedInCalculatedExpression()
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        await writer.CreateTableAsync(
            "T",
            [new ColumnDefinition("Partition", typeof(string)), CalculatedColumn("[Partition] & \"Partition(1,0,10,2)\" & 'partition(1,0,10,2)'")],
            TestContext.Current.CancellationToken);
        await writer.InsertRowAsync("T", ["value", DBNull.Value], TestContext.Current.CancellationToken);
        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(
            stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        int count = 0;
        await foreach (object[] row in reader.Rows("T", cancellationToken: TestContext.Current.CancellationToken))
        {
            Assert.Equal("valuePartition(1,0,10,2)partition(1,0,10,2)", row[1]);
            count++;
        }

        Assert.Equal(1, count);
    }

    [Fact]
    public async Task PartitionStoredDefault_ReopenedWriterEvaluatesRange()
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "T",
                [new ColumnDefinition("Range", typeof(string)) { DefaultValueExpression = "Partition(2,0,10,2)" }],
                TestContext.Current.CancellationToken);
        }

        stream.Position = 0;
        await using (AccessWriter reopened = await AccessWriter.OpenAsync(
            stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await reopened.InsertRowAsync("T", [DbDefault.Value], TestContext.Current.CancellationToken);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(
            stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        int count = 0;
        await foreach (object[] row in reader.Rows("T", cancellationToken: TestContext.Current.CancellationToken))
        {
            Assert.Equal(" 2: 3", row[0]);
            count++;
        }

        Assert.Equal(1, count);
    }

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    [InlineData("Partition([Id],0,10,2)")]
    [InlineData("partition ([Id],0,10,2)")]
    [InlineData("IIf([Id]>0,Partition([Id],0,10,2),\"\")")]
    public async Task DaoPartitionCalculatedField_RefusesAppendAndPreservesFields(string expression)
    {
        ArgumentNullException.ThrowIfNull(expression);
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.PartitionCalculated");
        string path = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        string quotedExpression = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(expression);
        AccessRoundTripEnvironment.CompactResult result = session.RunDaoEngineScript(
            $$"""
            $db = $engine.CreateDatabase({{path}}, ';LANGID=0x0409;CP=1252;COUNTRY=0')
            try {
                $db.Execute('CREATE TABLE [T] ([Id] LONG)')
                $db.TableDefs.Refresh()
                $tdf = $db.TableDefs('T')
                $field = $tdf.CreateField('Calculated', 10)
                $field.Expression = {{quotedExpression}}
                $rejected = $false
                try { $tdf.Fields.Append($field) }
                catch {
                    foreach ($daoError in $engine.Errors) {
                        if ($daoError.Number -eq 3859) { $rejected = $true }
                    }
                }
                if (!$rejected) { throw 'DAO did not refuse Partition with error 3859.' }
                Write-Output ('FIELDS_AFTER_REFUSAL=' + $tdf.Fields.Count)
                $valid = $tdf.CreateField('Valid', 4)
                $valid.Expression = '[Id]+1'
                $tdf.Fields.Append($valid)
                $db.Execute('INSERT INTO [T] ([Id]) VALUES (2)', 128)
                $rs = $db.OpenRecordset('SELECT [Valid] FROM [T]')
                try { Write-Output ('VALID=' + [string]$rs.Fields('Valid').Value) }
                finally { $rs.Close() }
                Write-Output 'PARTITION_REFUSED=3859'
            } finally { $db.Close() }
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(result.ExitCode == 0, $"DAO failed: {result.StdOut}\n{result.StdErr}");
        Assert.Contains("FIELDS_AFTER_REFUSAL=1", result.StdOut, StringComparison.Ordinal);
        Assert.Contains("VALID=3", result.StdOut, StringComparison.Ordinal);
        Assert.Contains("PARTITION_REFUSED=3859", result.StdOut, StringComparison.Ordinal);
    }

    private static ColumnDefinition CalculatedColumn(string expression)
        => new("Calculated", typeof(string)) { IsCalculated = true, CalculationExpression = expression };
}
