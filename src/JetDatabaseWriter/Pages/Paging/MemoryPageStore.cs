namespace JetDatabaseWriter.Pages.Paging;

using System.IO;
using JetDatabaseWriter.Interfaces;

/// <summary>Storage over a decrypted in-memory database image.</summary>
internal sealed class MemoryPageStore : StreamPageStore
{
    /// <summary>Initializes a new instance of the <see cref="MemoryPageStore"/> class.</summary>
    /// <param name="stream">The image stream.</param>
    /// <param name="leaveOpen">Whether the caller owns the image.</param>
    /// <param name="recoveryLease">An optional persistent caller-stream recovery lease.</param>
    internal MemoryPageStore(MemoryStream stream, bool leaveOpen, IPersistentStreamRecoveryLease? recoveryLease = null)
        : base(stream, leaveOpen, recoveryLease)
    {
    }
}
