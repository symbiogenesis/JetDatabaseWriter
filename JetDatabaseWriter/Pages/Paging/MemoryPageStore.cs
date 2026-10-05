namespace JetDatabaseWriter.Pages.Paging;

using System.IO;

/// <summary>Storage over a decrypted in-memory database image.</summary>
internal sealed class MemoryPageStore : StreamPageStore
{
    /// <summary>Initializes a new instance of the <see cref="MemoryPageStore"/> class.</summary>
    /// <param name="stream">The image stream.</param>
    /// <param name="leaveOpen">Whether the caller owns the image.</param>
    internal MemoryPageStore(MemoryStream stream, bool leaveOpen)
        : base(stream, leaveOpen)
    {
    }
}