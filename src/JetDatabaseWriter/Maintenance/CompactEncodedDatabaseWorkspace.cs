namespace JetDatabaseWriter.Maintenance;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

/// <summary>Exclusive private stores whose returned handles transfer to the codec coordinator.</summary>
/// <param name="OpenDecrypted">Independently opens immutable readable seekable staging after its writer is closed.</param>
/// <param name="CreateEncoded">Creates empty readable writable seekable encoded storage distinct from staging and the original.</param>
/// <param name="OpenEncoded">Independently opens immutable readable seekable encoded storage after its writer is closed.</param>
/// <remarks>
/// Factories run serially. Backing storage must be distinct even through wrappers and aliases; direct stream and
/// file-path aliases are refused, while other alias detection remains the caller's responsibility. Factory work,
/// stream storage, cryptographic provider/runtime overhead and filesystem caches are outside coordinator admission.
/// Every returned handle is owned and closed by the coordinator, including on failure. Partial output stays private
/// for caller-controlled diagnosis and cleanup; these factories never publish or replace a database.
/// </remarks>
internal sealed record CompactEncodedDatabaseWorkspace(
    Func<CancellationToken, ValueTask<Stream>> OpenDecrypted,
    Func<CancellationToken, ValueTask<Stream>> CreateEncoded,
    Func<CancellationToken, ValueTask<Stream>> OpenEncoded);
