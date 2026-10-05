namespace JetDatabaseWriter.Exceptions;

using System;

/// <summary>A structured database failure compatible with <see cref="ArgumentException"/>.</summary>
/// <param name="errorCode">The stable failure code.</param>
/// <param name="message">The failure message.</param>
/// <param name="paramName">The offending parameter, when applicable.</param>
/// <param name="errorInfo">The available context.</param>
/// <param name="innerException">The underlying failure.</param>
#pragma warning disable RCS1194 // Structured constructors preserve the BCL contract; serialization and unrelated base overloads are not part of the database error API.
public sealed class JetArgumentException(JetErrorCode errorCode, string message, string? paramName = null, JetErrorInfo? errorInfo = null, Exception? innerException = null)
    : ArgumentException(message, paramName, innerException), IJetException
{
    /// <summary>Initializes a new instance of the <see cref="JetArgumentException"/> class.</summary>
    public JetArgumentException()
        : this(JetErrorCode.None, string.Empty)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="JetArgumentException"/> class.</summary>
    /// <param name="message">The failure message.</param>
    public JetArgumentException(string message)
        : this(JetErrorCode.None, message)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="JetArgumentException"/> class.</summary>
    /// <param name="message">The failure message.</param>
    /// <param name="innerException">The underlying failure.</param>
    public JetArgumentException(string message, Exception innerException)
        : this(JetErrorCode.None, message, innerException: innerException)
    {
    }

    /// <inheritdoc/>
    public JetErrorCode ErrorCode { get; } = errorCode;

    /// <inheritdoc/>
    public JetErrorInfo ErrorInfo { get; } = errorInfo ?? new JetErrorInfo();
}
