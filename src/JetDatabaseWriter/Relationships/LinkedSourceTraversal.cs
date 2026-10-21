namespace JetDatabaseWriter.Relationships;

using System.Collections.Generic;

/// <summary>Immutable ancestry for one linked read, separate from concurrent sibling reads.</summary>
/// <param name="Depth">The number of Access source opens in this read.</param>
/// <param name="Ancestors">Database path and requested table pairs already visited.</param>
internal sealed record LinkedSourceTraversal(int Depth, IReadOnlyList<KeyValuePair<string, string>> Ancestors);
