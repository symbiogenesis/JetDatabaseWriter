namespace JetDatabaseWriter;

using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Interfaces;

/// <summary>
/// Abstract base class for Access database readers and writers. Exposes the
/// detected format of the open database; page I/O and format parsing live in
/// the internal database file that each reader or writer owns and hands to its
/// services.
/// </summary>
public abstract class AccessBase : IAccessBase
{
    /// <summary>
    /// Initializes a new instance of the <see cref="AccessBase"/> class.
    /// </summary>
    /// <param name="database">The open database file this reader or writer owns.</param>
    private protected AccessBase(DatabaseFile database) => this.Database = database;

    /// <inheritdoc/>
    public DatabaseFormat DatabaseFormat => this.Database.Format;

    /// <inheritdoc/>
    public int PageSize => this.Database.PageSizeBytes;

    /// <inheritdoc/>
    public int CodePage => this.Database.CodePage;

    /// <summary>Gets the open database file this reader or writer owns.</summary>
    private protected DatabaseFile Database { get; }

    /// <inheritdoc/>
    public abstract ValueTask DisposeAsync();
}
