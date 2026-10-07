namespace JetDatabaseWriter.Schema;

/// <summary>Removes the native string terminator from modeled persisted expressions.</summary>
internal static class PersistedExpressionText
{
    /// <summary>Removes one final NUL while preserving expression content and raw property bytes.</summary>
    /// <param name="text">The decoded persisted expression.</param>
    /// <returns>The expression without its optional native terminator.</returns>
    internal static string? Normalize(string? text)
        => text is { Length: > 0 } && text[^1] == '\0' ? text[..^1] : text;
}
