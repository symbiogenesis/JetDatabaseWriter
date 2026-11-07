namespace JetDatabaseWriter.Models;

using JetDatabaseWriter.Enums;

/// <summary>
/// Reports progress during an <see cref="AccessDatabaseCompact"/> operation.
/// </summary>
/// <param name="Phase">The current execution phase.</param>
/// <param name="CompletedSteps">The number of completed units in the current phase or overall pipeline.</param>
/// <param name="TotalSteps">The total expected units in the current phase or overall pipeline.</param>
/// <param name="CurrentTable">The table currently being processed, when applicable.</param>
public sealed record AccessCompactProgress(
    AccessCompactPhase Phase,
    int CompletedSteps,
    int TotalSteps,
    string? CurrentTable = null);
