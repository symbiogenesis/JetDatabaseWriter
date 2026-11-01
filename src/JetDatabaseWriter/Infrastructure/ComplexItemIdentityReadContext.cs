namespace JetDatabaseWriter.Infrastructure;

using System;
using System.Threading;

/// <summary>Shares scalar complex-item inspection limits across nested and authorized linked reads.</summary>
/// <param name="limit">The aggregate record-inspection allowance.</param>
internal sealed class ComplexItemIdentityReadContext(int limit)
{
    private readonly AsyncLocal<ComplexItemIdentityBudget?> current = new();

    /// <summary>Gets the active allowance, or an independent allowance for an internal read.</summary>
    internal ComplexItemIdentityBudget Budget => this.current.Value ?? new(limit);

    /// <summary>Starts an independent allowance when this flow is outside an operation.</summary>
    /// <returns>A lease restoring the preceding scope.</returns>
    internal Lease Enter()
    {
        ComplexItemIdentityBudget? previous = this.current.Value;
        ComplexItemIdentityBudget budget = previous ?? new(limit);
        this.current.Value = budget;
        return new(this, previous, budget);
    }

    private void Restore(ComplexItemIdentityBudget? previous) => this.current.Value = previous;

    /// <summary>Restores the preceding flow-local allowance.</summary>
    /// <param name="owner">The shared read context.</param>
    /// <param name="previous">The allowance active before entry.</param>
    /// <param name="budget">The allowance retained across iterator yields.</param>
    internal readonly struct Lease(ComplexItemIdentityReadContext? owner, ComplexItemIdentityBudget? previous, ComplexItemIdentityBudget? budget) : IDisposable
    {
        /// <summary>Restores the allowance after an iterator resumes under its consumer's context.</summary>
        internal void Resume() => owner?.Restore(budget);

        /// <summary>Restores the preceding scope.</summary>
        public void Dispose() => owner?.Restore(previous);
    }
}
