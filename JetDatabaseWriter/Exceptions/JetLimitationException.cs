namespace JetDatabaseWriter.Exceptions;

using System;

/// <summary>
/// Exception thrown when a JET database limitation is encountered that prevents correct data reading.
/// </summary>
public sealed class JetLimitationException : Exception, IJetException
{
    /// <summary>Initializes a new instance of the <see cref="JetLimitationException"/> class.</summary>
    /// <param name="errorCode">The stable failure code.</param>
    /// <param name="message">The failure message.</param>
    /// <param name="errorInfo">The available context.</param>
    /// <param name="innerException">The underlying failure.</param>
    public JetLimitationException(JetErrorCode errorCode, string message, JetErrorInfo? errorInfo = null, Exception? innerException = null)
        : base(message, innerException)
    {
        this.ErrorCode = errorCode;
        this.ErrorInfo = errorInfo ?? new JetErrorInfo();
    }

    /// <inheritdoc/>
    public JetErrorCode ErrorCode { get; }

    /// <inheritdoc/>
    public JetErrorInfo ErrorInfo { get; } = new();

    public JetLimitationException()
    {
    }

    public JetLimitationException(string message)
        : base(message)
    {
    }

    public JetLimitationException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
