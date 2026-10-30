namespace JetDatabaseWriter.Schema.Expressions;

using System;
using JetDatabaseWriter.Models;

/// <summary>Unwinds a function's evaluated argument without converting a native error.</summary>
/// <param name="error">The cached error being propagated.</param>
#pragma warning disable CA1032, CA1064 // Private evaluator control transfer always requires its native error value.
internal sealed class CalculatedErrorPropagationException(CalculatedError error) : Exception
{
    internal CalculatedError Error { get; } = error;
}
#pragma warning restore CA1032, CA1064
