namespace JetDatabaseWriter.Indexes;

using System;

#pragma warning disable RCS1194 // This internal capacity signal does not expose serialization or unrelated base overloads.

/// <summary>Signals valid index entries that exceed the available format capacity.</summary>
internal sealed class IndexCapacityException : ArgumentOutOfRangeException
{
    /// <summary>Initializes a new instance of the <see cref="IndexCapacityException"/> class.</summary>
    public IndexCapacityException()
        : this(string.Empty, "Index page capacity exceeded.")
    {
    }

    /// <summary>Initializes a new instance of the <see cref="IndexCapacityException"/> class.</summary>
    /// <param name="message">The capacity failure.</param>
    public IndexCapacityException(string message)
        : this(string.Empty, message)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="IndexCapacityException"/> class.</summary>
    /// <param name="parameterName">The input whose encoded size exceeds capacity.</param>
    /// <param name="message">The format capacity failure.</param>
    public IndexCapacityException(string parameterName, string message)
        : base(parameterName, message)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="IndexCapacityException"/> class.</summary>
    /// <param name="message">The capacity failure.</param>
    /// <param name="innerException">The cause of the capacity failure.</param>
    public IndexCapacityException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
