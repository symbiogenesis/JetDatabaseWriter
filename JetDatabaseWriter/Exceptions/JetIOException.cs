namespace JetDatabaseWriter.Exceptions;

using System;

/// <summary>A structured database failure with a stable code and available context.</summary>
/// <param name="errorCode">The stable failure code.</param>
/// <param name="message">The failure message.</param>
/// <param name="errorInfo">The available context.</param>
/// <param name="innerException">The underlying failure.</param>
public sealed class JetIOException(JetErrorCode errorCode, string message, JetErrorInfo? errorInfo = null, Exception? innerException = null)
    : System.IO.IOException(message, innerException), IJetException
{
    /// <summary>Initializes a new instance of the <see cref="JetIOException"/> class.</summary>
    public JetIOException()
        : this(JetErrorCode.None, string.Empty)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="JetIOException"/> class.</summary>
    /// <param name="message">The failure message.</param>
    public JetIOException(string message)
        : this(JetErrorCode.None, message)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="JetIOException"/> class.</summary>
    /// <param name="message">The failure message.</param>
    /// <param name="innerException">The underlying failure.</param>
    public JetIOException(string message, Exception innerException)
        : this(JetErrorCode.None, message, innerException: innerException)
    {
    }

    /// <inheritdoc/>
    public JetErrorCode ErrorCode { get; } = errorCode;

    /// <inheritdoc/>
    public JetErrorInfo ErrorInfo { get; } = JetDatabaseWriter.Infrastructure.JetFailureMetrics.Context(errorCode, errorInfo);
}
