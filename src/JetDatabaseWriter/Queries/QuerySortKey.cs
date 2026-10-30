namespace JetDatabaseWriter.Queries;

using System;
using System.Collections.Generic;

/// <summary>One prepared ordering key, retaining its selector, comparer and direction.</summary>
/// <typeparam name="T">The elements being ordered.</typeparam>
/// <param name="Selector">Projects an element to its boxed key.</param>
/// <param name="Comparer">The pipeline's default or explicitly supplied key comparer.</param>
/// <param name="Descending">Whether the key sorts in descending order.</param>
internal readonly record struct QuerySortKey<T>(Func<T, object?> Selector, IComparer<object?> Comparer, bool Descending);
