namespace JetDatabaseWriter.EntityFrameworkCore.Infrastructure.Internal;

using System;
using System.IO;
using JetDatabaseWriter.EntityFrameworkCore.Extensions;
using JetDatabaseWriter.Enums;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Stores options for configuring the <see cref="JetDatabaseWriter"/> Entity Framework Core provider.
/// </summary>
public sealed class JetOptionsExtension : IDbContextOptionsExtension
{
    /// <summary>
    /// Initializes a new instance of the <see cref="JetOptionsExtension"/> class.
    /// </summary>
    public JetOptionsExtension()
    {
        this.Format = DatabaseFormat.AceAccdb;
        this.LeaveStreamOpen = true;
    }

    private JetOptionsExtension(JetOptionsExtension copyFrom)
    {
        ArgumentNullException.ThrowIfNull(copyFrom);

        this.DatabasePath = copyFrom.DatabasePath;
        this.DatabaseStream = copyFrom.DatabaseStream;
        this.StreamFactory = copyFrom.StreamFactory;
        this.LeaveStreamOpen = copyFrom.LeaveStreamOpen;
        this.Format = copyFrom.Format;
        this.Password = copyFrom.Password;
        this.ReaderOptions = copyFrom.ReaderOptions;
        this.WriterOptions = copyFrom.WriterOptions;
    }

    /// <summary>
    /// Gets the file path of the Access database when configured to use a file on disk.
    /// </summary>
    public string? DatabasePath { get; private init; }

    /// <summary>
    /// Gets the caller-supplied stream when configured to use an in-memory or custom stream.
    /// </summary>
    public Stream? DatabaseStream { get; private init; }

    /// <summary>
    /// Gets the stream factory when configured to open streams via a delegate.
    /// </summary>
    public Func<Stream>? StreamFactory { get; private init; }

    /// <summary>
    /// Gets a value indicating whether caller-supplied streams are left open when the connection is disposed.
    /// </summary>
    public bool LeaveStreamOpen { get; private init; }

    /// <summary>
    /// Gets the target JET/ACE database format used when creating a new database.
    /// </summary>
    public DatabaseFormat Format { get; private init; }

    /// <summary>
    /// Gets the database password, or <see langword="null"/> for unencrypted databases.
    /// </summary>
    public string? Password { get; private init; }

    /// <summary>
    /// Gets optional <see cref="AccessReaderOptions"/> template used when opening <see cref="AccessReader"/> instances.
    /// </summary>
    public AccessReaderOptions? ReaderOptions { get; private init; }

    /// <summary>
    /// Gets optional <see cref="AccessWriterOptions"/> template used when opening <see cref="AccessWriter"/> instances.
    /// </summary>
    public AccessWriterOptions? WriterOptions { get; private init; }

    /// <inheritdoc/>
    public DbContextOptionsExtensionInfo Info => field ??= new JetOptionsExtensionInfo(this);

    /// <summary>
    /// Returns a copy of this extension configured with the specified <paramref name="databasePath"/>.
    /// </summary>
    /// <param name="databasePath">The file path to the Access database.</param>
    /// <returns>A new <see cref="JetOptionsExtension"/> instance.</returns>
    public JetOptionsExtension WithDatabasePath(string databasePath)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(databasePath);

        return new JetOptionsExtension(this)
        {
            DatabasePath = databasePath,
            DatabaseStream = null,
            StreamFactory = null,
        };
    }

    /// <summary>
    /// Returns a copy of this extension configured with the specified <paramref name="databaseStream"/>.
    /// </summary>
    /// <param name="databaseStream">The seekable stream backing the Access database.</param>
    /// <param name="leaveOpen">Whether to leave the stream open when the context is disposed.</param>
    /// <returns>A new <see cref="JetOptionsExtension"/> instance.</returns>
    public JetOptionsExtension WithDatabaseStream(Stream databaseStream, bool leaveOpen = true)
    {
        ArgumentNullException.ThrowIfNull(databaseStream);

        return new JetOptionsExtension(this)
        {
            DatabasePath = null,
            DatabaseStream = databaseStream,
            StreamFactory = null,
            LeaveStreamOpen = leaveOpen,
        };
    }

    /// <summary>
    /// Returns a copy of this extension configured with the specified <paramref name="streamFactory"/>.
    /// </summary>
    /// <param name="streamFactory">A factory that returns the seekable stream backing the Access database.</param>
    /// <param name="leaveOpen">Whether to leave streams created by the factory open.</param>
    /// <returns>A new <see cref="JetOptionsExtension"/> instance.</returns>
    public JetOptionsExtension WithStreamFactory(Func<Stream> streamFactory, bool leaveOpen = false)
    {
        ArgumentNullException.ThrowIfNull(streamFactory);

        return new JetOptionsExtension(this)
        {
            DatabasePath = null,
            DatabaseStream = null,
            StreamFactory = streamFactory,
            LeaveStreamOpen = leaveOpen,
        };
    }

    /// <summary>
    /// Returns a copy of this extension with <see cref="LeaveStreamOpen"/> set to <paramref name="leaveOpen"/>.
    /// </summary>
    /// <param name="leaveOpen">Whether to leave the stream open.</param>
    /// <returns>A new <see cref="JetOptionsExtension"/> instance.</returns>
    public JetOptionsExtension WithLeaveStreamOpen(bool leaveOpen)
        => new(this) { LeaveStreamOpen = leaveOpen };

    /// <summary>
    /// Returns a copy of this extension with <see cref="Format"/> set to <paramref name="format"/>.
    /// </summary>
    /// <param name="format">The target database format.</param>
    /// <returns>A new <see cref="JetOptionsExtension"/> instance.</returns>
    public JetOptionsExtension WithFormat(DatabaseFormat format)
        => new(this) { Format = format };

    /// <summary>
    /// Returns a copy of this extension with <see cref="Password"/> set to <paramref name="password"/>.
    /// </summary>
    /// <param name="password">The database password, or <see langword="null"/>.</param>
    /// <returns>A new <see cref="JetOptionsExtension"/> instance.</returns>
    public JetOptionsExtension WithPassword(string? password)
        => new(this) { Password = password };

    /// <summary>
    /// Returns a copy of this extension with <see cref="ReaderOptions"/> set to <paramref name="readerOptions"/>.
    /// </summary>
    /// <param name="readerOptions">The reader options template.</param>
    /// <returns>A new <see cref="JetOptionsExtension"/> instance.</returns>
    public JetOptionsExtension WithReaderOptions(AccessReaderOptions readerOptions)
    {
        ArgumentNullException.ThrowIfNull(readerOptions);

        return new JetOptionsExtension(this) { ReaderOptions = readerOptions };
    }

    /// <summary>
    /// Returns a copy of this extension with <see cref="WriterOptions"/> set to <paramref name="writerOptions"/>.
    /// </summary>
    /// <param name="writerOptions">The writer options template.</param>
    /// <returns>A new <see cref="JetOptionsExtension"/> instance.</returns>
    public JetOptionsExtension WithWriterOptions(AccessWriterOptions writerOptions)
    {
        ArgumentNullException.ThrowIfNull(writerOptions);

        return new JetOptionsExtension(this) { WriterOptions = writerOptions };
    }

    /// <inheritdoc/>
    public void ApplyServices(IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        _ = services.AddEntityFrameworkJetDatabaseWriter();
    }

    /// <inheritdoc/>
    public void Validate(IDbContextOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (string.IsNullOrWhiteSpace(this.DatabasePath)
            && this.DatabaseStream is null
            && this.StreamFactory is null)
        {
            throw new InvalidOperationException(
                "A database file path, Stream, or stream factory must be configured for the JetDatabaseWriter EF Core provider.");
        }
    }
}
