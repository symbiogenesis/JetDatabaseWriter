namespace JetDatabaseWriter.Exceptions;

using System;

/// <summary>A structured database failure with a stable code and available context.</summary>
/// <param name="errorCode">The stable failure code.</param>
/// <param name="message">The failure message.</param>
/// <param name="errorInfo">The available context.</param>
/// <param name="innerException">The underlying failure.</param>
public sealed class JetValidationRuleException(JetErrorCode errorCode, string message, JetErrorInfo? errorInfo = null, Exception? innerException = null)
    : ArgumentException(message, innerException), IJetException
{
    /// <summary>Initializes a new instance of the <see cref="JetValidationRuleException"/> class.</summary>
    public JetValidationRuleException()
        : this(JetErrorCode.None, string.Empty)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="JetValidationRuleException"/> class.</summary>
    /// <param name="message">The failure message.</param>
    public JetValidationRuleException(string message)
        : this(JetErrorCode.None, message)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="JetValidationRuleException"/> class.</summary>
    /// <param name="message">The failure message.</param>
    /// <param name="innerException">The underlying failure.</param>
    public JetValidationRuleException(string message, Exception innerException)
        : this(JetErrorCode.None, message, innerException: innerException)
    {
    }

    /// <inheritdoc/>
    public JetErrorCode ErrorCode { get; } = errorCode;

    /// <inheritdoc/>
    public JetErrorInfo ErrorInfo { get; } = JetDatabaseWriter.Infrastructure.JetFailureMetrics.Context(errorCode, errorInfo);
}
