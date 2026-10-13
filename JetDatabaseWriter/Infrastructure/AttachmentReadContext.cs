namespace JetDatabaseWriter.Infrastructure;

using System;
using System.Threading;
using JetDatabaseWriter.ComplexColumns.Models;

/// <summary>Shares attachment limits across nested reads and linked sources in one operation.</summary>
/// <param name="contentLimit">The aggregate declared-content limit.</param>
/// <param name="workLimit">The aggregate byte-processing limit.</param>
internal sealed class AttachmentReadContext(long contentLimit, long workLimit)
{
    private readonly AsyncLocal<AttachmentWrapper.ContentBudget?> current = new();

    /// <summary>Gets the active operation's budget, or an independent budget for an internal read.</summary>
    internal AttachmentWrapper.ContentBudget Budget => this.current.Value ?? new(contentLimit, workLimit);

    /// <summary>Starts an independent budget when this flow is outside an operation.</summary>
    /// <returns>A lease restoring the preceding scope.</returns>
    internal Lease Enter()
    {
        AttachmentWrapper.ContentBudget? previous = this.current.Value;
        AttachmentWrapper.ContentBudget budget = previous ?? new(contentLimit, workLimit);
        this.current.Value = budget;
        return new(this, previous, budget);
    }

    private void Restore(AttachmentWrapper.ContentBudget? previous) => this.current.Value = previous;

    /// <summary>Restores the preceding flow-local budget.</summary>
    /// <param name="owner">The shared read context.</param>
    /// <param name="previous">The budget active before entry.</param>
    /// <param name="budget">The budget retained across iterator yields.</param>
    internal readonly struct Lease(AttachmentReadContext? owner, AttachmentWrapper.ContentBudget? previous, AttachmentWrapper.ContentBudget? budget) : IDisposable
    {
        /// <summary>Restores the active budget after an iterator resumes under its consumer's context.</summary>
        internal void Resume() => owner?.Restore(budget);

        /// <summary>Restores the preceding scope.</summary>
        public void Dispose() => owner?.Restore(previous);
    }
}
