namespace JetDatabaseWriter.EntityFrameworkCore.Infrastructure;

using System;
using JetDatabaseWriter.EntityFrameworkCore.Infrastructure.Internal;
using JetDatabaseWriter.Enums;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

/// <summary>
/// Allows <see cref="JetDatabaseWriter"/> specific configuration to be performed on <see cref="DbContextOptions"/>.
/// </summary>
public sealed class JetDbContextOptionsBuilder
{
    private readonly DbContextOptionsBuilder optionsBuilder;

    /// <summary>
    /// Initializes a new instance of the <see cref="JetDbContextOptionsBuilder"/> class.
    /// </summary>
    /// <param name="optionsBuilder">The core options builder.</param>
    public JetDbContextOptionsBuilder(DbContextOptionsBuilder optionsBuilder)
    {
        ArgumentNullException.ThrowIfNull(optionsBuilder);

        this.optionsBuilder = optionsBuilder;
    }

    /// <summary>
    /// Configures the target JET/ACE database format used when creating a new database via <c>EnsureCreatedAsync</c>.
    /// </summary>
    /// <param name="format">The database format (<see cref="DatabaseFormat.AceAccdb"/>, <see cref="DatabaseFormat.Jet4Mdb"/>, or <see cref="DatabaseFormat.Jet3Mdb"/>).</param>
    /// <returns>The same builder instance so that multiple calls can be chained.</returns>
    public JetDbContextOptionsBuilder WithFormat(DatabaseFormat format)
        => this.WithOption(e => e.WithFormat(format));

    /// <summary>
    /// Configures the database password used when opening or creating an encrypted Access database.
    /// </summary>
    /// <param name="password">The database password, or <see langword="null"/> for unencrypted databases.</param>
    /// <returns>The same builder instance so that multiple calls can be chained.</returns>
    public JetDbContextOptionsBuilder WithPassword(string? password)
        => this.WithOption(e => e.WithPassword(password));

    /// <summary>
    /// Configures the <see cref="AccessReaderOptions"/> template used when opening <see cref="AccessReader"/> instances.
    /// </summary>
    /// <param name="readerOptions">The reader options template.</param>
    /// <returns>The same builder instance so that multiple calls can be chained.</returns>
    public JetDbContextOptionsBuilder WithReaderOptions(AccessReaderOptions readerOptions)
    {
        ArgumentNullException.ThrowIfNull(readerOptions);

        return this.WithOption(e => e.WithReaderOptions(readerOptions));
    }

    /// <summary>
    /// Configures the <see cref="AccessWriterOptions"/> template used when opening <see cref="AccessWriter"/> instances.
    /// </summary>
    /// <param name="writerOptions">The writer options template.</param>
    /// <returns>The same builder instance so that multiple calls can be chained.</returns>
    public JetDbContextOptionsBuilder WithWriterOptions(AccessWriterOptions writerOptions)
    {
        ArgumentNullException.ThrowIfNull(writerOptions);

        return this.WithOption(e => e.WithWriterOptions(writerOptions));
    }

    /// <summary>
    /// Configures whether a caller-supplied stream is left open when the context is disposed.
    /// </summary>
    /// <param name="leaveOpen">Whether to leave the stream open.</param>
    /// <returns>The same builder instance so that multiple calls can be chained.</returns>
    public JetDbContextOptionsBuilder LeaveStreamOpen(bool leaveOpen = true)
        => this.WithOption(e => e.WithLeaveStreamOpen(leaveOpen));

    private JetDbContextOptionsBuilder WithOption(Func<JetOptionsExtension, JetOptionsExtension> setAction)
    {
        JetOptionsExtension extension = this.optionsBuilder.Options.FindExtension<JetOptionsExtension>()
            ?? new JetOptionsExtension();
        ((IDbContextOptionsBuilderInfrastructure)this.optionsBuilder).AddOrUpdateExtension(setAction(extension));
        return this;
    }
}
