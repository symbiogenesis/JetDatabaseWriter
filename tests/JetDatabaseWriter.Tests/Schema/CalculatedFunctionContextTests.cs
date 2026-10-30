namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Expressions;
using Xunit;

/// <summary>Table calculations reject functions that native Access reserves for other contexts.</summary>
public sealed class CalculatedFunctionContextTests
{
    [Theory]
    [InlineData("Switch([Id]>0,\"yes\",True,\"no\")")]
    [InlineData("switch$ ([Id]>0,\"yes\",True,\"no\")")]
    [InlineData("IIf([Id]>0,Switch(True,\"yes\"),\"no\")")]
    [InlineData("DLookup(\"Name\",\"People\")")]
    [InlineData("DCount(\"*\",\"People\")")]
    [InlineData("DSum(\"Score\",\"People\")")]
    [InlineData("DAvg(\"Score\",\"People\")")]
    [InlineData("DMin(\"Score\",\"People\")")]
    [InlineData("DMax(\"Score\",\"People\")")]
    [InlineData("IIf([Id]>0,dlookup$ (\"Name\",\"People\"),\"\")")]
    public void Function_RefusedOnlyInCalculatedField(string expression)
    {
        JetNotSupportedException exception = Assert.Throws<JetNotSupportedException>(() => CalculatedExpressionParser.Parse(expression));
        Assert.Equal(JetErrorCode.UnsupportedExpression, exception.ErrorCode);
        Assert.Contains("calculated columns", exception.Message, StringComparison.Ordinal);
        Assert.NotNull(CalculatedExpressionParser.Parse(expression, isCalculatedColumn: false));
    }

    [Theory]
    [InlineData("Switch([Id]>0,\"yes\",True,\"no\")")]
    [InlineData("IIf([Id]>0,Switch(True,\"yes\"),\"no\")")]
    [InlineData("DLookup(\"Name\",\"People\")")]
    [InlineData("DCount(\"*\",\"People\")")]
    [InlineData("DSum(\"Score\",\"People\")")]
    [InlineData("DAvg(\"Score\",\"People\")")]
    [InlineData("DMin(\"Score\",\"People\")")]
    [InlineData("DMax(\"Score\",\"People\")")]
    public async Task CreateAndAddCalculation_RefuseWithoutChangingImage(string expression)
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        byte[] before = stream.ToArray();
        var calculation = new ColumnDefinition("Calculated", typeof(string)) { IsCalculated = true, CalculationExpression = expression };
        await Assert.ThrowsAsync<JetNotSupportedException>(async () => await writer.CreateTableAsync(
            "Rejected", [new ColumnDefinition("Id", typeof(int)), calculation], TestContext.Current.CancellationToken));
        Assert.Equal(before, stream.ToArray());

        await writer.CreateTableAsync("T", [new ColumnDefinition("Id", typeof(int))], TestContext.Current.CancellationToken);
        before = stream.ToArray();
        await Assert.ThrowsAsync<JetNotSupportedException>(async () => await writer.AddColumnAsync("T", calculation, TestContext.Current.CancellationToken));
        Assert.Equal(before, stream.ToArray());
        await writer.InsertRowAsync("T", [1], TestContext.Current.CancellationToken);
    }

    [Theory]
    [InlineData("[Switch] & \"DLookup(1)\" & 'Switch(True,1)'")]
    [InlineData("[DLookup] & \"Switch(True,1)\"")]
    [InlineData("IIf([Id]>0,\"Switch\",\"DCount\")")]
    public void FunctionTextAndFieldNames_RemainValid(string expression)
        => Assert.NotNull(CalculatedExpressionParser.Parse(expression));
}
