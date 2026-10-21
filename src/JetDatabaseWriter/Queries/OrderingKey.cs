namespace JetDatabaseWriter.Queries;

using System.Linq.Expressions;

/// <summary>One ordering key: the compiled selector source and its sort direction.</summary>
/// <param name="KeySelector">The key-selector lambda whose result drives the sort.</param>
/// <param name="Descending">Whether the key sorts in descending order.</param>
internal readonly record struct OrderingKey(LambdaExpression KeySelector, bool Descending);
