namespace JetDatabaseWriter.Exceptions;

using System;

/// <summary>A structured database failure compatible with <see cref="InvalidOperationException"/>.</summary>
/// <param name="errorCode">The stable failure code.</param>
/// <param name="message">The failure message.</param>
/// <param name="errorInfo">The available context.</param>
/// <param name="innerException">The underlying failure.</param>
public sealed class JetOperationException(JetErrorCode errorCode, string message, JetErrorInfo? errorInfo = null, Exception? innerException = null)
    : InvalidOperationException(message, innerException), IJetException
{
    /// <summary>Initializes a new instance of the <see cref="JetOperationException"/> class.</summary>
    public JetOperationException()
        : this(JetErrorCode.None, string.Empty)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="JetOperationException"/> class.</summary>
    /// <param name="message">The failure message.</param>
    public JetOperationException(string message)
        : this(JetErrorCode.None, message)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="JetOperationException"/> class.</summary>
    /// <param name="message">The failure message.</param>
    /// <param name="innerException">The underlying failure.</param>
    public JetOperationException(string message, Exception innerException)
        : this(JetErrorCode.None, message, innerException: innerException)
    {
    }

    /// <inheritdoc/>
    public JetErrorCode ErrorCode { get; } = errorCode;

    /// <inheritdoc/>
    public JetErrorInfo ErrorInfo { get; } = errorInfo ?? new JetErrorInfo();
}
