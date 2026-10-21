namespace JetDatabaseWriter.Tests.Writer;

using System;
using JetDatabaseWriter.Exceptions;
using Xunit;

/// <summary>Contracts for structured database failures.</summary>
public sealed class ExceptionModelContractTests
{
    [Fact]
    public void ConstraintFailure_PreservesMessageAndContext()
    {
        var info = new JetErrorInfo { TableName = "T", ColumnName = "Id", IndexName = "PrimaryKey" };
        var error = new JetConstraintException(JetErrorCode.UniqueViolation, "duplicate", info);
        Assert.IsType<InvalidOperationException>(error, exactMatch: false);
#pragma warning disable CA1859 // This assertion verifies the public exception interface contract.
        IJetException structured = error;
#pragma warning restore CA1859 // This assertion verifies the public exception interface contract.
        Assert.Equal(JetErrorCode.UniqueViolation, structured.ErrorCode);
        Assert.Same(info, structured.ErrorInfo);
        Assert.Equal("duplicate", error.Message);
    }

    [Fact]
    public void ArgumentFailure_PreservesParameterAndInnerException()
    {
        var inner = new InvalidOperationException("cause");
        var error = new JetObjectNotFoundException(JetErrorCode.ColumnNotFound, "missing", "columnName", new JetErrorInfo { ColumnName = "Id" }, inner);
        Assert.IsType<ArgumentException>(error, exactMatch: false);
        Assert.Equal("columnName", error.ParamName);
        Assert.Same(inner, error.InnerException);
        Assert.Equal(new ArgumentException("missing", "columnName").Message, error.Message);
        Assert.Equal(JetErrorCode.ColumnNotFound, error.ErrorCode);
    }

    [Fact]
    public void ErrorCodes_HaveStableGroupNumbers()
    {
        Assert.Equal(101, (int)JetErrorCode.TableNotFound);
        Assert.Equal(110, (int)JetErrorCode.RelationshipTargetNotFound);
        Assert.Equal(301, (int)JetErrorCode.UniqueViolation);
        Assert.Equal(308, (int)JetErrorCode.TableValidationRuleViolation);
        Assert.Equal(808, (int)JetErrorCode.UnsupportedTextCollation);
        Assert.Equal(903, (int)JetErrorCode.LinkedTableHasNoIndexes);
        Assert.False(Enum.IsDefined(typeof(JetErrorCode), 902));
    }

    [Fact]
    public void ConstraintFailure_ReportsCascadeDepthCode()
    {
        const string message = "Foreign-key cascade depth exceeded 64. Possible cyclic relationship.";
        var error = new JetConstraintException(JetErrorCode.CascadeDepthExceeded, message);
        Assert.Equal(message, error.Message);
        Assert.Equal(JetErrorCode.CascadeDepthExceeded, error.ErrorCode);
    }

    [Fact]
    public void LimitationFailure_ReportsIndexMaintenanceReason()
    {
        const string message = "The indexes of table 'T' cannot be maintained: the index descriptor is damaged.";
        var info = new JetErrorInfo { TableName = "T", Reason = "the index descriptor is damaged" };
        var error = new JetLimitationException(JetErrorCode.IndexesUnmaintainable, message, info);
        Assert.Equal(message, error.Message);
        Assert.Same(info, error.ErrorInfo);
    }
}
