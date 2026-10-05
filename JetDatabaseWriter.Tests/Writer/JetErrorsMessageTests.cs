namespace JetDatabaseWriter.Tests.Writer;

using JetDatabaseWriter.Exceptions;
using Xunit;

/// <summary>Golden message contracts for the structured error factories.</summary>
public sealed class JetErrorsMessageTests
{
    [Fact]
    public void ConstraintFactory_PreservesTheGoldenMessage()
    {
        const string message = "Foreign-key cascade depth exceeded 64. Possible cyclic relationship.";
        JetConstraintException error = JetErrors.Constraint(JetErrorCode.CascadeDepthExceeded, message);
        Assert.Equal(message, error.Message);
        Assert.Equal(JetErrorCode.CascadeDepthExceeded, error.ErrorCode);
    }

    [Fact]
    public void LimitationFactory_PreservesTheGoldenMessageAndReason()
    {
        const string message = "The indexes of table 'T' cannot be maintained: the index descriptor is damaged.";
        var info = new JetErrorInfo { TableName = "T", Reason = "the index descriptor is damaged" };
        JetLimitationException error = JetErrors.Limitation(JetErrorCode.IndexesUnmaintainable, message, info);
        Assert.Equal(message, error.Message);
        Assert.Same(info, error.ErrorInfo);
    }
}
