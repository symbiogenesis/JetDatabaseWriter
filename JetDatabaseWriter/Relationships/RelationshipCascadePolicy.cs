namespace JetDatabaseWriter.Relationships;

using JetDatabaseWriter.Exceptions;

internal static class RelationshipCascadePolicy
{
    /// <summary>
    /// Maximum recursion depth for cascade-delete / cascade-update chains.
    /// Guards against pathological self-referential cycles. Real-world Access
    /// schemas almost never exceed depth 3.
    /// </summary>
    internal const int MaxDepth = 64;

    public static void ThrowIfDepthExceeded(int depth)
    {
        if (depth > MaxDepth)
        {
            throw JetErrors.Constraint(JetErrorCode.CascadeDepthExceeded, $"Foreign-key cascade depth exceeded {MaxDepth}. Possible cyclic relationship.");
        }
    }
}
