namespace JetDatabaseWriter.Indexes.Models;

/// <summary>
/// Encoded range predicate for an internal index scan.
/// </summary>
/// <param name="Lower">The lower bound.</param>
/// <param name="Upper">The upper bound.</param>
/// <param name="RequiredPrefix">The encoded prefix every matching key must start with, or <see langword="null"/>.</param>
internal readonly record struct EncodedIndexRange(
    EncodedIndexBound Lower,
    EncodedIndexBound Upper,
    byte[]? RequiredPrefix = null);
