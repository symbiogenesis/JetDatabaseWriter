namespace JetDatabaseWriter.EntityFrameworkCore.Query.Internal;

using System;
using JetDatabaseWriter.EntityFrameworkCore.Storage.Internal;
using Microsoft.EntityFrameworkCore.Query;

/// <summary>
/// Carries per-query state and the scoped <see cref="IJetClientConnection"/> during query execution.
/// </summary>
public sealed class JetQueryContext : QueryContext
{
    /// <summary>
    /// Initializes a new instance of the <see cref="JetQueryContext"/> class.
    /// </summary>
    /// <param name="dependencies">Parameter object containing dependencies for this service.</param>
    /// <param name="connection">The scoped client connection.</param>
    public JetQueryContext(
        QueryContextDependencies dependencies,
        IJetClientConnection connection)
        : base(dependencies)
    {
        ArgumentNullException.ThrowIfNull(connection);

        this.Connection = connection;
    }

    /// <summary>
    /// Gets the client connection for the current context scope.
    /// </summary>
    public IJetClientConnection Connection { get; }
}
