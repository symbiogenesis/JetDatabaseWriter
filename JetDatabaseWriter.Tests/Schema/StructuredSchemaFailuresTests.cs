namespace JetDatabaseWriter.Tests.Schema;

using System;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Schema.Expressions;
using JetDatabaseWriter.Schema.Models;
using Xunit;

/// <summary>Expression failures expose codes and context without message parsing.</summary>
public sealed class StructuredSchemaFailuresTests
{
    [Fact]
    public void UnsupportedCalculation_HasStructuredCode()
    {
        JetNotSupportedException failure = Assert.Throws<JetNotSupportedException>(() => CalculatedExpressionParser.Parse("CByte(1)"));
        Assert.Equal(JetErrorCode.UnsupportedExpression, failure.ErrorCode);
    }

    [Fact]
    public void ResultOverflow_HasColumnContextAndOriginalCause()
    {
        var cause = new OverflowException("conversion failed");
        var column = new ColumnConstraint { Name = "Total", ClrType = typeof(int), CalculationExpression = "[Price] * 2" };
        JetLimitationException failure = Assert.IsType<JetLimitationException>(CalculatedExpressionErrors.ForColumn(column, "Orders", cause, storing: true, long.MaxValue));
        Assert.Equal(JetErrorCode.NumericOverflow, failure.ErrorCode);
        Assert.Equal("Orders", failure.ErrorInfo.TableName);
        Assert.Equal("Total", failure.ErrorInfo.ColumnName);
        Assert.Same(cause, failure.InnerException);
    }
}
