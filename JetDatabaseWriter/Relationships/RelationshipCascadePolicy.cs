namespace JetDatabaseWriter.Relationships;

using System;

internal static class RelationshipCascadePolicy
{
    public static void ThrowIfDepthExceeded(int depth)
    {
        if (depth > AccessWriter.CascadeMaxDepth)
        {
            throw new InvalidOperationException(
                $"Foreign-key cascade depth exceeded {AccessWriter.CascadeMaxDepth}. Possible cyclic relationship.");
        }
    }
}
