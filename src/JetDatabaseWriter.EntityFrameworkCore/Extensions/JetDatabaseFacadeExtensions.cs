namespace Microsoft.EntityFrameworkCore;

using System;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.EntityFrameworkCore.Storage.Internal;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;

/// <summary>
/// Extension methods on <see cref="DatabaseFacade"/> for migrating <see cref="JetDatabaseWriter"/> schemas.
/// </summary>
public static class JetDatabaseFacadeExtensions
{
    /// <summary>
    /// Synchronously migrates the database schema to match the current EF Core model.
    /// Synchronous migration is not supported by <see cref="JetDatabaseWriter.EntityFrameworkCore"/>; use <see cref="MigrateAsync"/> instead.
    /// </summary>
    /// <param name="databaseFacade">The <see cref="DatabaseFacade"/> for the context.</param>
    /// <exception cref="NotSupportedException">Always thrown because synchronous I/O is not supported.</exception>
    public static void Migrate(this DatabaseFacade databaseFacade)
    {
        ArgumentNullException.ThrowIfNull(databaseFacade);

        throw new NotSupportedException(
            "Synchronous Migrate is not supported by JetDatabaseWriter.EntityFrameworkCore. Use MigrateAsync instead.");
    }

    /// <summary>
    /// Asynchronously creates the database if it does not exist, or incrementally migrates an existing database schema
    /// to match the current EF Core model by creating missing tables, adding missing columns, creating missing indexes
    /// and foreign-key relationships, and inserting any missing seed data.
    /// </summary>
    /// <param name="databaseFacade">The <see cref="DatabaseFacade"/> for the context.</param>
    /// <param name="cancellationToken">A <see cref="CancellationToken"/> to observe while waiting for the task to complete.</param>
    /// <returns>A task representing the asynchronous migration operation.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the current provider is not <see cref="JetDatabaseWriter.EntityFrameworkCore"/>.</exception>
    public static Task MigrateAsync(
        this DatabaseFacade databaseFacade,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(databaseFacade);

        IDatabaseCreator creator = databaseFacade.GetService<IDatabaseCreator>();
        if (creator is not JetDatabaseCreator jetCreator)
        {
            throw new InvalidOperationException(
                "MigrateAsync requires the JetDatabaseWriter.EntityFrameworkCore database provider.");
        }

        return jetCreator.MigrateAsync(cancellationToken);
    }
}
