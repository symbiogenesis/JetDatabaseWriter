namespace JetDatabaseWriter.Exceptions;

using System;

/// <summary>A structured database failure compatible with <see cref="System.IO.IOException"/>.</summary>
/// <param name="errorCode">The stable failure code.</param>
/// <param name="message">The failure message.</param>
/// <param name="errorInfo">The available context.</param>
/// <param name="innerException">The underlying failure.</param>
#pragma warning disable RCS1194 // Structured constructors preserve the BCL contract; serialization and unrelated base overloads are not part of the database error API.
public sealed class JetLockException(JetErrorCode errorCode, string message, JetErrorInfo? errorInfo = null, Exception? innerException = null)
    : System.IO.IOException(message, innerException), IJetException
{
    /// <summary>Initializes a new instance of the <see cref="JetLockException"/> class.</summary>
    public JetLockException()
        : this(JetErrorCode.None, string.Empty)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="JetLockException"/> class.</summary>
    /// <param name="message">The failure message.</param>
    public JetLockException(string message)
        : this(JetErrorCode.None, message)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="JetLockException"/> class.</summary>
    /// <param name="message">The failure message.</param>
    /// <param name="innerException">The underlying failure.</param>
    public JetLockException(string message, Exception innerException)
        : this(JetErrorCode.None, message, innerException: innerException)
    {
    }

    /// <inheritdoc/>
    public JetErrorCode ErrorCode { get; } = errorCode;

    /// <inheritdoc/>
    public JetErrorInfo ErrorInfo { get; } = errorInfo ?? new JetErrorInfo();
}
