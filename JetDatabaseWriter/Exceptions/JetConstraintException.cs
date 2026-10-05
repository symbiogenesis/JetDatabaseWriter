namespace JetDatabaseWriter.Exceptions;

using System;

/// <summary>A structured database failure compatible with <see cref="InvalidOperationException"/>.</summary>
/// <param name="errorCode">The stable failure code.</param>
/// <param name="message">The failure message.</param>
/// <param name="errorInfo">The available context.</param>
/// <param name="innerException">The underlying failure.</param>
public sealed class JetConstraintException(JetErrorCode errorCode, string message, JetErrorInfo? errorInfo = null, Exception? innerException = null)
    : InvalidOperationException(message, innerException), IJetException
{
    /// <summary>Initializes a new instance of the <see cref="JetConstraintException"/> class.</summary>
    public JetConstraintException()
        : this(JetErrorCode.None, string.Empty)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="JetConstraintException"/> class.</summary>
    /// <param name="message">The failure message.</param>
    public JetConstraintException(string message)
        : this(JetErrorCode.None, message)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="JetConstraintException"/> class.</summary>
    /// <param name="message">The failure message.</param>
    /// <param name="innerException">The underlying failure.</param>
    public JetConstraintException(string message, Exception innerException)
        : this(JetErrorCode.None, message, innerException: innerException)
    {
    }

    /// <inheritdoc/>
    public JetErrorCode ErrorCode { get; } = errorCode;

    /// <inheritdoc/>
    public JetErrorInfo ErrorInfo { get; } = errorInfo ?? new JetErrorInfo();
}
