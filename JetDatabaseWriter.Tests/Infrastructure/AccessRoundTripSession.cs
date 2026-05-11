namespace JetDatabaseWriter.Tests.Infrastructure;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Owns a temporary workspace for DAO round-trip tests, including optional
/// Northwind fixture seeding and compacted-output paths.
/// </summary>
internal sealed class AccessRoundTripSession : IAsyncDisposable
{
    private static readonly TimeSpan DefaultCompactTimeout = TimeSpan.FromMinutes(2);
    private readonly TimeSpan _compactTimeout;

    private AccessRoundTripSession(string workDir, string sourcePath, string compactedPath, TimeSpan compactTimeout)
    {
        WorkDir = workDir;
        SourcePath = sourcePath;
        CompactedPath = compactedPath;
        _compactTimeout = compactTimeout;
    }

    /// <summary>Gets the temporary working directory for scripts and databases.</summary>
    public string WorkDir { get; }

    /// <summary>Gets the primary source database path in the temporary workspace.</summary>
    public string SourcePath { get; }

    /// <summary>Gets the compacted-output database path in the temporary workspace.</summary>
    public string CompactedPath { get; }

    /// <summary>
    /// Creates an empty temporary session. Callers can use
    /// <see cref="CreateDatabasePath"/> for DAO-authored databases or
    /// <see cref="CopyNorthwindAsync"/> for one or more fixture copies.
    /// </summary>
    /// <param name="tempDirectoryName">Directory name under the system temp path.</param>
    /// <param name="compactTimeout">Timeout to use for <see cref="RunDaoCompact"/>.</param>
    /// <returns>Temporary round-trip session.</returns>
    public static AccessRoundTripSession CreateEmpty(
        string tempDirectoryName = "JetDatabaseWriter.Tests.RoundTrip",
        TimeSpan? compactTimeout = null)
    {
        string workDir = Path.Combine(Path.GetTempPath(), tempDirectoryName, Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(workDir);

        return new AccessRoundTripSession(
            workDir,
            Path.Combine(workDir, "source.accdb"),
            Path.Combine(workDir, "compacted.accdb"),
            compactTimeout ?? DefaultCompactTimeout);
    }

    /// <summary>
    /// Creates a temporary session and copies the Northwind fixture to
    /// <see cref="SourcePath"/>.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <param name="tempDirectoryName">Directory name under the system temp path.</param>
    /// <param name="compactTimeout">Timeout to use for <see cref="RunDaoCompact"/>.</param>
    /// <returns>Temporary round-trip session seeded with Northwind.</returns>
    public static async Task<AccessRoundTripSession> CreateFromNorthwindAsync(
        CancellationToken cancellationToken,
        string tempDirectoryName = "JetDatabaseWriter.Tests.RoundTrip",
        TimeSpan? compactTimeout = null)
    {
        AccessRoundTripSession session = CreateEmpty(tempDirectoryName, compactTimeout);
        try
        {
            await CopyNorthwindToAsync(session.SourcePath, cancellationToken).ConfigureAwait(false);
            return session;
        }
        catch
        {
            await session.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>Creates a unique ACCDB path inside the temporary workspace.</summary>
    /// <param name="prefix">Filename prefix.</param>
    /// <returns>Unique ACCDB path.</returns>
    public string CreateDatabasePath(string prefix) =>
        Path.Combine(WorkDir, $"{prefix}_{Guid.NewGuid():N}.accdb");

    /// <summary>Copies the Northwind fixture to a unique ACCDB path in the workspace.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Path to the copied fixture.</returns>
    public async Task<string> CopyNorthwindAsync(CancellationToken cancellationToken)
    {
        string destinationPath = CreateDatabasePath("nw");
        await CopyNorthwindToAsync(destinationPath, cancellationToken).ConfigureAwait(false);
        return destinationPath;
    }

    /// <summary>Opens <see cref="SourcePath"/> with lock files disabled.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Writer for the session source database.</returns>
    public ValueTask<AccessWriter> OpenWriterAsync(CancellationToken cancellationToken = default) =>
        AccessWriter.OpenAsync(SourcePath, new AccessWriterOptions { UseLockFile = false }, cancellationToken);

    /// <summary>Runs DAO CompactDatabase from <see cref="SourcePath"/> to <see cref="CompactedPath"/>.</summary>
    public void RunDaoCompact()
    {
        AccessRoundTripEnvironment.CompactResult result = AccessRoundTripEnvironment.RunDaoCompact(
            SourcePath,
            CompactedPath,
            _compactTimeout);

        if (result.ExitCode != 0 || !File.Exists(CompactedPath))
        {
            throw new Xunit.Sdk.XunitException(
                $"""
                DAO CompactDatabase failed (exit={result.ExitCode}).
                --- stdout ---
                {result.StdOut}
                --- stderr ---
                {result.StdErr}
                """);
        }
    }

    /// <summary>Runs a DAO engine script in this session's temporary workspace.</summary>
    /// <param name="engineScript">Script body that uses <c>$engine</c>.</param>
    /// <param name="timeout">Maximum wait for the PowerShell host to exit.</param>
    /// <returns>Process exit code, captured stdout, captured stderr.</returns>
    public AccessRoundTripEnvironment.CompactResult RunDaoEngineScript(string engineScript, TimeSpan timeout) =>
        AccessRoundTripEnvironment.RunDaoEngineScript(engineScript, WorkDir, timeout);

    /// <summary>Runs a DAO database script in this session's temporary workspace.</summary>
    /// <param name="databasePath">Database path to open.</param>
    /// <param name="databaseScript">Script body that uses <c>$db</c>.</param>
    /// <param name="timeout">Maximum wait for the PowerShell host to exit.</param>
    /// <returns>Process exit code, captured stdout, captured stderr.</returns>
    public AccessRoundTripEnvironment.CompactResult RunDaoDatabaseScript(string databasePath, string databaseScript, TimeSpan timeout) =>
        AccessRoundTripEnvironment.RunDaoDatabaseScript(databasePath, databaseScript, WorkDir, timeout);

    /// <summary>Runs a DAO create-database script in this session's temporary workspace.</summary>
    /// <param name="databasePath">Database path to create.</param>
    /// <param name="attributes">DAO create-database attributes string.</param>
    /// <param name="databaseScript">Script body that uses <c>$db</c>.</param>
    /// <param name="timeout">Maximum wait for the PowerShell host to exit.</param>
    /// <returns>Process exit code, captured stdout, captured stderr.</returns>
    public AccessRoundTripEnvironment.CompactResult RunDaoCreateDatabaseScript(
        string databasePath,
        string attributes,
        string databaseScript,
        TimeSpan timeout) =>
        AccessRoundTripEnvironment.RunDaoCreateDatabaseScript(databasePath, attributes, databaseScript, WorkDir, timeout);

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        try
        {
            if (Directory.Exists(WorkDir))
            {
                Directory.Delete(WorkDir, recursive: true);
            }
        }
        catch (IOException)
        {
            // Best-effort cleanup; the temp folder is short-lived per run.
        }
        catch (UnauthorizedAccessException)
        {
            // Best-effort cleanup; the temp folder is short-lived per run.
        }

        return ValueTask.CompletedTask;
    }

    private static async Task CopyNorthwindToAsync(string destinationPath, CancellationToken cancellationToken)
    {
        await using (FileStream source = File.OpenRead(TestDatabases.NorthwindTraders))
        await using (FileStream destination = File.Create(destinationPath))
        {
            await source.CopyToAsync(destination, cancellationToken).ConfigureAwait(false);
        }

        File.SetAttributes(destinationPath, File.GetAttributes(destinationPath) & ~FileAttributes.ReadOnly);
    }
}
