namespace JetDatabaseWriter.Indexes;

using System;

/// <summary>Signals valid index entries that exceed the available format capacity.</summary>
/// <param name="parameterName">The input whose encoded size exceeds capacity.</param>
/// <param name="message">The format capacity failure.</param>
internal sealed class IndexCapacityException(string parameterName, string message)
    : ArgumentOutOfRangeException(parameterName, message)
{
}
