namespace JetDatabaseWriter.Schema;

using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Pages.Paging;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// The writer's in-place TDEF write-backs: fields patched at logical offsets
/// of a table's TDEF chain, such as a real index's <c>first_dp</c> root
/// pointer or <c>used_pages</c> pointer, which sit on a continuation page in
/// a wide table, are written back over the chain's own pages. It reads the
/// chain through <see cref="TableDefReader"/> and writes through the writer's
/// <see cref="Pager"/>, so inside a transaction both go through the journal.
/// Only the writer's object graph holds one.
/// </summary>
/// <param name="pager">The writer's page file, which the chain's pages are read from and written to.</param>
/// <param name="tableDefs">Reads a chain with its physical pages, over the same <paramref name="pager"/>.</param>
internal sealed class TDefWriter(Pager pager, TableDefReader tableDefs)
{
    /// <summary>
    /// Writes a chain read by <see cref="TableDefReader.ReadTDefChainAsync"/>
    /// back in place, mapping each logical byte to the physical page that
    /// holds it. Only pages whose bytes changed are written.
    /// </summary>
    /// <param name="chain">The chain whose <see cref="LogicalTDefChain.Bytes"/> were patched.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task that completes when the changed pages are written.</returns>
    internal ValueTask WriteChainInPlaceAsync(LogicalTDefChain chain, CancellationToken cancellationToken = default)
        => chain.WriteInPlaceAsync(pager.ReadPageAsync, PageBuffers.Return, pager.WritePageAsync, cancellationToken);

    /// <summary>
    /// Patches one 32-bit field at a logical offset of the TDEF chain rooted
    /// at <paramref name="tdefPage"/> and writes the page that holds it.
    /// </summary>
    /// <param name="tdefPage">The first TDEF page.</param>
    /// <param name="logicalOffset">The field's offset in the logical TDEF buffer.</param>
    /// <param name="value">The value to write.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A task that completes when the field is written.</returns>
    internal async ValueTask WriteInt32Async(long tdefPage, int logicalOffset, int value, CancellationToken cancellationToken = default)
    {
        LogicalTDefChain chain = await tableDefs.ReadTDefChainAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        Wi32(chain.Bytes, logicalOffset, value);
        await this.WriteChainInPlaceAsync(chain, cancellationToken).ConfigureAwait(false);
    }
}
