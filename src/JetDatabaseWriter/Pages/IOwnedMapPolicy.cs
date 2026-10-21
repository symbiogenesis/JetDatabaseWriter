namespace JetDatabaseWriter.Pages;

using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Pages.Models;

/// <summary>
/// Decides whose owned-page usage maps the writer may extend:
/// <see cref="DataPageInserter"/> asks before it marks a data page it appended
/// in its table's map. What the policy has decided is writer state: a
/// transaction captures it when it begins and restores it if it rolls back,
/// because inside the transaction it can come to name tables the rollback
/// discards. Implemented by <see cref="CatalogOwnedMapPolicy"/>.
/// </summary>
internal interface IOwnedMapPolicy
{
    /// <summary>
    /// Returns whether the writer may mark the data pages it appends to the
    /// table at <paramref name="tdefPageNumber"/> in that table's owned-page
    /// usage map.
    /// </summary>
    /// <param name="tdefPageNumber">The table's TDEF page number.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns><see langword="true"/> when the writer may extend the table's owned-page map.</returns>
    public ValueTask<bool> CanMaintainAsync(long tdefPageNumber, CancellationToken cancellationToken);

    /// <summary>
    /// Records that this writer created the owned-page usage map of the table
    /// at <paramref name="tdefPageNumber"/>, so the data pages it appends may
    /// be marked in it, whatever the policy decided for that page before.
    /// </summary>
    /// <param name="tdefPageNumber">The table's TDEF page number.</param>
    public void RegisterWritable(long tdefPageNumber);

    /// <summary>Captures what the policy has decided so far.</summary>
    /// <returns>The state to pass to <see cref="Restore"/>.</returns>
    public OwnedMapPolicyState Capture();

    /// <summary>Puts the policy's decisions back to <paramref name="state"/>.</summary>
    /// <param name="state">A state from <see cref="Capture"/>.</param>
    public void Restore(OwnedMapPolicyState state);
}
