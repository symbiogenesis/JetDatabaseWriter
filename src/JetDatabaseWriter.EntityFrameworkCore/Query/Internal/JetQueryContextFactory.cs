namespace JetDatabaseWriter.EntityFrameworkCore.Query.Internal;

using System;
using JetDatabaseWriter.EntityFrameworkCore.Storage.Internal;
using Microsoft.EntityFrameworkCore.Query;

/// <summary>
/// Creates <see cref="JetQueryContext"/> instances for query execution.
/// </summary>
public sealed class JetQueryContextFactory : IQueryContextFactory
{
    private readonly QueryContextDependencies dependencies;
    private readonly IJetClientConnection connection;

    /// <summary>
    /// Initializes a new instance of the <see cref="JetQueryContextFactory"/> class.
    /// </summary>
    /// <param name="dependencies">Parameter object containing dependencies for this service.</param>
    /// <param name="connection">The scoped client connection.</param>
    public JetQueryContextFactory(
        QueryContextDependencies dependencies,
        IJetClientConnection connection)
    {
        ArgumentNullException.ThrowIfNull(dependencies);
        ArgumentNullException.ThrowIfNull(connection);

        this.dependencies = dependencies;
        this.connection = connection;
    }

    /// <inheritdoc/>
    public QueryContext Create()
        => new JetQueryContext(this.dependencies, this.connection);
}
