namespace JetDatabaseWriter.Scaffold;

/// <summary>
/// A scaffolded navigation property derived from a foreign-key relationship: either
/// a reference to the parent entity or a collection of child entities.
/// </summary>
/// <param name="IsCollection">
/// <see langword="true"/> for a child collection (the parent side of the relationship);
/// <see langword="false"/> for a parent reference (the child / FK side).
/// </param>
/// <param name="TargetClassName">The related entity's C# class name.</param>
/// <param name="PreferredName">The preferred property name before de-duplication.</param>
internal sealed record ScaffoldNavigation(bool IsCollection, string TargetClassName, string PreferredName);
