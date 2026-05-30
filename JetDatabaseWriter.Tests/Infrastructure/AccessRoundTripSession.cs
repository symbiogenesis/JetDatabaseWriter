namespace JetDatabaseWriter.Tests.Infrastructure;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Owns a temporary workspace for DAO round-trip tests, including optional
/// Northwind fixture seeding and compacted-output paths.
/// </summary>
/// <remarks>
/// Use <see cref="CreateFromNorthwindAsync"/> or <see cref="CreateDaoAccdbAsync"/>
/// when a test needs an Access/DAO-authored source of truth. A plain
/// <see cref="CreateEmpty"/> session has no fixture authority until the caller
/// seeds it through Access/DAO, or intentionally writes a library-created
/// database as the subject under test.
/// </remarks>
internal sealed class AccessRoundTripSession : IAsyncDisposable
{
    private const string DaoCreateDatabaseAttributes = ";LANGID=0x0409;CP=1252;COUNTRY=0";

    private static readonly TimeSpan DefaultCompactTimeout = TimeSpan.FromMinutes(2);
    private static readonly TimeSpan DefaultDaoCreateTimeout = TimeSpan.FromMinutes(1);
    private readonly string databaseExtension;
    private readonly TimeSpan compactTimeout;

    private AccessRoundTripSession(string workDir, string sourcePath, string compactedPath, TimeSpan compactTimeout, string databaseExtension)
    {
        this.WorkDir = workDir;
        this.SourcePath = sourcePath;
        this.CompactedPath = compactedPath;
        this.databaseExtension = databaseExtension;
        this.compactTimeout = compactTimeout;
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
    /// <see cref="CopyNorthwindAsync"/> for one or more fixture copies. If the
    /// source path is later created by <see cref="AccessWriter"/>, that file is
    /// writer output under test, not a trusted fixture.
    /// </summary>
    /// <param name="tempDirectoryName">Directory name under the system temp path.</param>
    /// <param name="compactTimeout">Timeout to use for <see cref="RunDaoCompact"/>.</param>
    /// <param name="databaseExtension">Database file extension for source, compacted, and generated paths.</param>
    /// <returns>Temporary round-trip session.</returns>
    public static AccessRoundTripSession CreateEmpty(
        string tempDirectoryName = "JetDatabaseWriter.Tests.RoundTrip",
        TimeSpan? compactTimeout = null,
        string databaseExtension = ".accdb")
    {
        string workDir = Path.Combine(Path.GetTempPath(), tempDirectoryName, Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(workDir);
        string normalizedExtension = NormalizeDatabaseExtension(databaseExtension);

        return new AccessRoundTripSession(
            workDir,
            Path.Combine(workDir, "source" + normalizedExtension),
            Path.Combine(workDir, "compacted" + normalizedExtension),
            compactTimeout ?? DefaultCompactTimeout,
            normalizedExtension);
    }

    /// <summary>
    /// Creates a temporary session and copies the Access-authored Northwind
    /// fixture to <see cref="SourcePath"/>.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <param name="tempDirectoryName">Directory name under the system temp path.</param>
    /// <param name="compactTimeout">Timeout to use for <see cref="RunDaoCompact"/>.</param>
    /// <returns>Temporary round-trip session seeded with a trusted Access-authored host.</returns>
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

    /// <summary>
    /// Creates a temporary session and initializes <see cref="SourcePath"/>
    /// as a DAO-authored ACCDB.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <param name="tempDirectoryName">Directory name under the system temp path.</param>
    /// <param name="compactTimeout">Timeout to use for <see cref="RunDaoCompact"/>.</param>
    /// <param name="createTimeout">Timeout to use for DAO <c>CreateDatabase</c>.</param>
    /// <returns>Temporary round-trip session seeded with a trusted DAO-authored host.</returns>
    public static async Task<AccessRoundTripSession> CreateDaoAccdbAsync(
        CancellationToken cancellationToken,
        string tempDirectoryName = "JetDatabaseWriter.Tests.RoundTrip",
        TimeSpan? compactTimeout = null,
        TimeSpan? createTimeout = null)
    {
        cancellationToken.ThrowIfCancellationRequested();
        AccessRoundTripSession session = CreateEmpty(tempDirectoryName, compactTimeout);
        try
        {
            AccessRoundTripEnvironment.CompactResult result = session.RunDaoCreateDatabaseScript(
                session.SourcePath,
                DaoCreateDatabaseAttributes,
                "Write-Output 'DAO_CREATE=OK'",
                createTimeout ?? DefaultDaoCreateTimeout);

            if (result.ExitCode != 0 || !File.Exists(session.SourcePath))
            {
                throw new Xunit.Sdk.XunitException(
                    $"""
                    DAO CreateDatabase failed (exit={result.ExitCode}).
                    --- stdout ---
                    {result.StdOut}
                    --- stderr ---
                    {result.StdErr}
                    """);
            }

            cancellationToken.ThrowIfCancellationRequested();
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
        Path.Combine(this.WorkDir, $"{prefix}_{Guid.NewGuid():N}{this.databaseExtension}");

    /// <summary>Copies the Northwind fixture to a unique ACCDB path in the workspace.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Path to the copied fixture.</returns>
    public async Task<string> CopyNorthwindAsync(CancellationToken cancellationToken)
    {
        string destinationPath = this.CreateDatabasePath("nw");
        await CopyNorthwindToAsync(destinationPath, cancellationToken).ConfigureAwait(false);
        return destinationPath;
    }

    /// <summary>Opens <see cref="SourcePath"/> with lock files disabled.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Writer for the session source database.</returns>
    public ValueTask<AccessWriter> OpenWriterAsync(CancellationToken cancellationToken = default) =>
        AccessWriter.OpenAsync(this.SourcePath, new AccessWriterOptions { UseLockFile = false }, cancellationToken);

    /// <summary>Runs DAO CompactDatabase from <see cref="SourcePath"/> to <see cref="CompactedPath"/>.</summary>
    /// <exception cref="Xunit.Sdk.XunitException">Thrown when DAO CompactDatabase fails or does not produce the compacted file.</exception>
    public void RunDaoCompact()
    {
        AccessRoundTripEnvironment.CompactResult result = AccessRoundTripEnvironment.RunDaoCompact(
            this.SourcePath,
            this.CompactedPath,
            this.compactTimeout);

        if (result.ExitCode != 0 || !File.Exists(this.CompactedPath))
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
        AccessRoundTripEnvironment.RunDaoEngineScript(engineScript, this.WorkDir, timeout);

    /// <summary>Runs a DAO database script in this session's temporary workspace.</summary>
    /// <param name="databasePath">Database path to open.</param>
    /// <param name="databaseScript">Script body that uses <c>$db</c>.</param>
    /// <param name="timeout">Maximum wait for the PowerShell host to exit.</param>
    /// <returns>Process exit code, captured stdout, captured stderr.</returns>
    public AccessRoundTripEnvironment.CompactResult RunDaoDatabaseScript(string databasePath, string databaseScript, TimeSpan timeout) =>
        AccessRoundTripEnvironment.RunDaoDatabaseScript(databasePath, databaseScript, this.WorkDir, timeout);

    /// <summary>Runs a DAO database script against <see cref="SourcePath" />, then compacts to <see cref="CompactedPath" /> in the same host.</summary>
    /// <param name="databaseScript">Script body that uses <c>$db</c>.</param>
    /// <param name="timeout">Maximum wait for the PowerShell host to exit.</param>
    /// <returns>Process exit code, captured stdout, captured stderr.</returns>
    public AccessRoundTripEnvironment.CompactResult RunDaoDatabaseScriptThenCompact(string databaseScript, TimeSpan timeout) =>
        AccessRoundTripEnvironment.RunDaoDatabaseScriptThenCompact(this.SourcePath, this.CompactedPath, databaseScript, this.WorkDir, timeout);

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
        AccessRoundTripEnvironment.RunDaoCreateDatabaseScript(databasePath, attributes, databaseScript, this.WorkDir, timeout);

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        try
        {
            if (Directory.Exists(this.WorkDir))
            {
                Directory.Delete(this.WorkDir, recursive: true);
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

    private static string NormalizeDatabaseExtension(string databaseExtension)
    {
        if (string.IsNullOrWhiteSpace(databaseExtension))
        {
            throw new ArgumentException("Database extension must be non-empty.", nameof(databaseExtension));
        }

        return databaseExtension.StartsWith('.') ? databaseExtension : "." + databaseExtension;
    }
}
