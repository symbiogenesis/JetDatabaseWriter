namespace JetDatabaseWriter;

using System;
using System.IO;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Maintenance;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Transactions;

/// <summary>
/// Provides Compact &amp; Repair operations for Microsoft Access Jet3 (<c>.mdb</c>), Jet4 (<c>.mdb</c>),
/// and ACE (<c>.accdb</c>) databases.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="AccessDatabaseCompact"/> inventories all catalog objects, local tables, system tables,
/// and complex flat tables, rebuilds table rows, long-value (<c>LVAL</c>) chains, B-tree indexes,
/// and usage maps into contiguous pages, reclaims proven old data/LVAL/index/map/freed pages,
/// and independently reopens and validates the compacted database before publishing it.
/// </para>
/// <para>
/// When damaged catalog headers, unassigned physical rows, or opaque pages are encountered,
/// <see cref="CompactToAsync"/> and <see cref="CompactAsync"/> can write a validated salvage copy
/// when <see cref="AccessCompactOptions.AllowPartialRecovery"/> is <see langword="true"/>, while
/// <see cref="CompactInPlaceAsync"/> always requires complete ownership and zero salvage diagnostics
/// (<see cref="AccessCompactReport.ReplacementAuthorized"/>) before replacing the original file.
/// </para>
/// </remarks>
public static class AccessDatabaseCompact
{
    /// <summary>
    /// Compacts and repairs a database from <paramref name="source"/> into <paramref name="destination"/>.
    /// </summary>
    /// <param name="source">The readable, seekable source database stream, left open.</param>
    /// <param name="destination">The readable, writable, seekable destination stream, left open.</param>
    /// <param name="options">Optional compact configuration, password, and resource budgets.</param>
    /// <param name="cancellationToken">Cancels discovery, construction, validation, and encoding.</param>
    /// <returns>An <see cref="AccessCompactReport"/> summarizing space reclamation, validation, and any diagnostics.</returns>
    /// <exception cref="ArgumentException">The source or destination stream is invalid or shares backing storage.</exception>
    /// <exception cref="JetCorruptDataException">The source database is corrupt or partial recovery is not authorized.</exception>
    /// <exception cref="JetLimitationException">A resource budget or format limit is exceeded.</exception>
    public static async ValueTask<AccessCompactReport> CompactAsync(
        Stream source,
        Stream destination,
        AccessCompactOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        DiagnosticTrace.ThrowIfReentrant();
        Guard.NotNull(source, nameof(source));
        Guard.NotNull(destination, nameof(destination));
        Guard.RequireReadableSeekableStream(source, nameof(source));
        if (!destination.CanRead || !destination.CanWrite || !destination.CanSeek)
        {
            throw new ArgumentException("Destination stream must be readable, writable, and seekable.", nameof(destination));
        }

        if (ReferenceEquals(source, destination)
            || (source is FileStream srcFile && destination is FileStream dstFile
                && string.Equals(srcFile.Name, dstFile.Name, StringComparison.OrdinalIgnoreCase)))
        {
            throw new ArgumentException("Source and destination streams must have distinct backing storage.", nameof(destination));
        }

        options ??= new AccessCompactOptions();
        options.Validate();
        cancellationToken.ThrowIfCancellationRequested();

        long originalSourcePosition = source.Position;
        try
        {
            return await ExecuteCompactCoreAsync(
                source,
                string.Empty,
                options,
                inPlace: false,
                async (finalArtifactPath, token) =>
                {
                    destination.SetLength(0);
                    destination.Position = 0;
                    await using FileStream artifact = FileStreamFactory.Open(
                        finalArtifactPath,
                        FileMode.Open,
                        FileAccess.Read,
                        FileShare.Read,
                        FileOptions.Asynchronous | FileOptions.SequentialScan);
                    await artifact.CopyToAsync(destination, 81920, token).ConfigureAwait(false);
                    await destination.FlushAsync(token).ConfigureAwait(false);
                    destination.Position = 0;
                },
                cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            if (source.CanSeek)
            {
                source.Position = originalSourcePosition;
            }
        }
    }

    /// <summary>
    /// Compacts and repairs the database at <paramref name="sourcePath"/> into a separate file at <paramref name="destinationPath"/>.
    /// </summary>
    /// <param name="sourcePath">The path to the source database file.</param>
    /// <param name="destinationPath">The path to the destination database file (must be distinct from <paramref name="sourcePath"/>).</param>
    /// <param name="options">Optional compact configuration, password, and resource budgets.</param>
    /// <param name="cancellationToken">Cancels discovery, construction, validation, and replacement.</param>
    /// <returns>An <see cref="AccessCompactReport"/> summarizing space reclamation, validation, and any diagnostics.</returns>
    /// <exception cref="ArgumentException">The paths or options are invalid.</exception>
    /// <exception cref="JetCorruptDataException">The source database is corrupt or partial recovery is not authorized.</exception>
    /// <exception cref="JetLimitationException">A resource budget or format limit is exceeded.</exception>
    public static async ValueTask<AccessCompactReport> CompactToAsync(
        string sourcePath,
        string destinationPath,
        AccessCompactOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        DiagnosticTrace.ThrowIfReentrant();
        Guard.NotNullOrEmpty(sourcePath, nameof(sourcePath));
        Guard.NotNullOrEmpty(destinationPath, nameof(destinationPath));
        cancellationToken.ThrowIfCancellationRequested();

        sourcePath = Path.GetFullPath(sourcePath);
        destinationPath = Path.GetFullPath(destinationPath);
        if (string.Equals(sourcePath, destinationPath, StringComparison.OrdinalIgnoreCase))
        {
            throw new ArgumentException("CompactToAsync requires a destination path distinct from sourcePath; use CompactInPlaceAsync for in-place replacement.", nameof(destinationPath));
        }

        options ??= new AccessCompactOptions();
        options.Validate();
        _ = EncryptionReplacementIntent.Recover(sourcePath, cancellationToken: cancellationToken);
        _ = EncryptionReplacementIntent.Recover(destinationPath, cancellationToken: cancellationToken);
        Guard.RequireExistingDatabaseFile(sourcePath, nameof(sourcePath));

        var lockFile = new LockFileCoordinator(
            sourcePath,
            nameof(AccessDatabaseCompact),
            options.CreateLockFileSettings(respectExisting: true),
            recoverableWriter: true);
        FileStream? sourceStream = null;
        try
        {
            FileShare sharing = Path.DirectorySeparatorChar == '\\' ? FileShare.Read : FileShare.Read;
            sourceStream = FileStreamFactory.Open(
                sourcePath,
                FileMode.Open,
                FileAccess.Read,
                sharing,
                FileOptions.Asynchronous | FileOptions.SequentialScan);
            _ = RecoverableWriterLock.Recover(sourcePath);
            PersistentRollbackJournal.RejectHotJournal(sourceStream);
            lockFile.Acquire();

            return await ExecuteCompactCoreAsync(
                sourceStream,
                sourcePath,
                options,
                inPlace: false,
                async (finalArtifactPath, token) =>
                {
                    string? directory = Path.GetDirectoryName(destinationPath);
                    if (!string.IsNullOrEmpty(directory))
                    {
                        _ = Directory.CreateDirectory(directory);
                    }

                    if (File.Exists(destinationPath))
                    {
                        await EncryptionFileReplacement.ReplaceAsync(
                            destinationPath,
                            async (staging, writeToken) =>
                            {
                                await using FileStream artifact = FileStreamFactory.Open(
                                    finalArtifactPath,
                                    FileMode.Open,
                                    FileAccess.Read,
                                    FileShare.Read,
                                    FileOptions.Asynchronous | FileOptions.SequentialScan);
                                await artifact.CopyToAsync(staging, 81920, writeToken).ConfigureAwait(false);
                            },
                            token).ConfigureAwait(false);
                    }
                    else
                    {
                        string stagingPath = destinationPath + ".compact-stage." + Guid.NewGuid().ToString("N") + ".tmp";
                        try
                        {
                            await using (FileStream artifact = FileStreamFactory.Open(
                                finalArtifactPath,
                                FileMode.Open,
                                FileAccess.Read,
                                FileShare.Read,
                                FileOptions.Asynchronous | FileOptions.SequentialScan))
                            await using (FileStream staging = FileStreamFactory.Open(
                                stagingPath,
                                FileMode.CreateNew,
                                FileAccess.Write,
                                FileShare.None,
                                FileOptions.Asynchronous | FileOptions.SequentialScan))
                            {
                                await artifact.CopyToAsync(staging, 81920, token).ConfigureAwait(false);
                                await staging.FlushAsync(token).ConfigureAwait(false);
                                token.ThrowIfCancellationRequested();
#pragma warning disable CA1849 // FileStream has no asynchronous Flush(flushToDisk: true).
                                staging.Flush(flushToDisk: true);
#pragma warning restore CA1849
                            }

                            File.Move(stagingPath, destinationPath);
                        }
                        catch
                        {
                            try
                            {
                                if (File.Exists(stagingPath))
                                {
                                    File.Delete(stagingPath);
                                }
                            }
                            catch (IOException)
                            {
                            }
                            catch (UnauthorizedAccessException)
                            {
                            }

                            throw;
                        }
                    }
                },
                cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            try
            {
                if (sourceStream is not null)
                {
                    await sourceStream.DisposeAsync().ConfigureAwait(false);
                }
            }
            finally
            {
                lockFile.Dispose();
            }
        }
    }

    /// <summary>
    /// Compacts and repairs the database at <paramref name="path"/> and atomically replaces the original file
    /// only after complete ownership, reclamation, and independent reopened validation succeed.
    /// </summary>
    /// <param name="path">The path to the database file to compact in place.</param>
    /// <param name="options">Optional compact configuration, password, and resource budgets.</param>
    /// <param name="cancellationToken">Cancels discovery, construction, validation, and replacement before commit.</param>
    /// <returns>An <see cref="AccessCompactReport"/> summarizing space reclamation and validation.</returns>
    /// <exception cref="ArgumentException">The path or options are invalid, or <see cref="AccessCompactOptions.AllowPartialRecovery"/> is set.</exception>
    /// <exception cref="JetCorruptDataException">The source database is corrupt or replacement is not authorized.</exception>
    /// <exception cref="JetLimitationException">A resource budget or format limit is exceeded.</exception>
    public static async ValueTask<AccessCompactReport> CompactInPlaceAsync(
        string path,
        AccessCompactOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        DiagnosticTrace.ThrowIfReentrant();
        Guard.NotNullOrEmpty(path, nameof(path));
        cancellationToken.ThrowIfCancellationRequested();

        path = Path.GetFullPath(path);
        options ??= new AccessCompactOptions();
        options.Validate();
        if (options.AllowPartialRecovery)
        {
            throw new ArgumentException("AllowPartialRecovery is not permitted for CompactInPlaceAsync; use CompactToAsync to write a separate salvage copy.", nameof(options));
        }

        _ = EncryptionReplacementIntent.Recover(path, cancellationToken: cancellationToken);
        Guard.RequireExistingDatabaseFile(path, nameof(path));

        var lockFile = new LockFileCoordinator(
            path,
            nameof(AccessDatabaseCompact),
            options.CreateLockFileSettings(respectExisting: true),
            recoverableWriter: true);
        FileStream? sourceStream = null;
        try
        {
            FileShare sharing = Path.DirectorySeparatorChar == '\\' ? FileShare.Delete : FileShare.None;
            sourceStream = FileStreamFactory.Open(
                path,
                FileMode.Open,
                FileAccess.Read,
                sharing,
                FileOptions.Asynchronous | FileOptions.SequentialScan);
            _ = RecoverableWriterLock.Recover(path);
            PersistentRollbackJournal.RejectHotJournal(sourceStream);
            lockFile.Acquire();

            return await ExecuteCompactCoreAsync(
                sourceStream,
                path,
                options,
                inPlace: true,
                (finalArtifactPath, token) =>
                    EncryptionFileReplacement.ReplaceAsync(
                        path,
                        async (staging, writeToken) =>
                        {
                            await using FileStream artifact = FileStreamFactory.Open(
                                finalArtifactPath,
                                FileMode.Open,
                                FileAccess.Read,
                                FileShare.Read,
                                FileOptions.Asynchronous | FileOptions.SequentialScan);
                            await artifact.CopyToAsync(staging, 81920, writeToken).ConfigureAwait(false);
                        },
                        token,
                        sourceStream),
                cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            try
            {
                if (sourceStream is not null)
                {
                    await sourceStream.DisposeAsync().ConfigureAwait(false);
                }
            }
            finally
            {
                lockFile.Dispose();
            }
        }
    }

    private static async ValueTask<AccessCompactReport> ExecuteCompactCoreAsync(
        Stream sourceStream,
        string sourcePath,
        AccessCompactOptions options,
        bool inPlace,
        Func<string, CancellationToken, ValueTask> publishAsync,
        CancellationToken cancellationToken)
    {
        sourceStream.Position = 0;
        byte[] header = await EncryptionManager.ReadOpenHeaderPageAsync(sourceStream, cancellationToken).ConfigureAwait(false);
        try
        {
            AccessEncryptionFormat encryptionFormat = EncryptionConverter.Detect(header);
            bool isEncrypted = encryptionFormat != AccessEncryptionFormat.None;
            var database = DatabaseFile.ForReader(
                sourceStream,
                header,
                options.Password,
                sourcePath,
                leaveOpen: true,
                options,
                cancellationToken: cancellationToken);
            try
            {
                CompactDiscoveryBudget discoveryBudget = CreateDiscoveryBudget(options);
                CompactActiveTableBudget activeTableBudget = new()
                {
                    Work = discoveryBudget,
                };

                CompactDatabasePlannerResult plan = await CompactDatabasePlanner.PlanAsync(
                    database.Pages,
                    database.Format,
                    discoveryBudget,
                    activeTableBudget,
                    cancellationToken).ConfigureAwait(false);

                if (!plan.Reclamation.IsComplete)
                {
                    if (inPlace)
                    {
                        throw new JetCorruptDataException(
                            JetErrorCode.CorruptPage,
                            "In-place Compact & Repair requires complete ownership and zero salvage diagnostics; use CompactToAsync with AllowPartialRecovery to write a separate salvage copy.");
                    }

                    if (!options.AllowPartialRecovery)
                    {
                        throw new JetCorruptDataException(
                            JetErrorCode.CorruptPage,
                            "Compact & Repair discovered damaged ownership, unassigned rows, or opaque storage; enable AllowPartialRecovery on CompactToAsync to write a separate salvage copy.");
                    }
                }

                string tempRoot = !string.IsNullOrEmpty(sourcePath)
                    ? Path.GetDirectoryName(sourcePath) ?? Path.GetTempPath()
                    : Path.GetTempPath();
                string workDir = Path.Combine(tempRoot, ".jdw-compact-" + Guid.NewGuid().ToString("N"));
                _ = Directory.CreateDirectory(workDir);
                try
                {
                    string PathFor(CompactPinnedDatabaseFile role, long owner)
                        => Path.Combine(workDir, $"{role}-{owner}.bin");

                    var workspace = new CompactPinnedDatabaseWorkspace(
                        (role, owner, token) =>
                        {
                            token.ThrowIfCancellationRequested();
                            return new ValueTask<Stream>(
                                FileStreamFactory.Open(
                                    PathFor(role, owner),
                                    FileMode.CreateNew,
                                    FileAccess.ReadWrite,
                                    FileShare.None));
                        },
                        (role, owner, token) =>
                        {
                            token.ThrowIfCancellationRequested();
                            return new ValueTask<Stream>(
                                FileStreamFactory.Open(
                                    PathFor(role, owner),
                                    FileMode.Open,
                                    FileAccess.Read,
                                    FileShare.Read));
                        },
                        static (_, _) => default);

                    CompactPinnedDatabaseBuilderBudget builderBudget = CreateBuilderBudget(options, discoveryBudget, activeTableBudget);
                    CompactPinnedDatabaseBuilderResult buildResult = await CompactPinnedDatabaseBuilder.BuildAndValidateAsync(
                        database.Pages,
                        database.Format,
                        plan.Tables,
                        workspace,
                        builderBudget,
                        cancellationToken,
                        plan.Reclamation).ConfigureAwait(false);

                    if (!buildResult.DatabaseValidated)
                    {
                        throw new JetCorruptDataException(JetErrorCode.CorruptPage, "Compacted database failed independent reopened validation.");
                    }

                    if (inPlace && !buildResult.PublicationAuthorized)
                    {
                        throw new JetCorruptDataException(JetErrorCode.CorruptPage, "Compacted database is not authorized for in-place replacement.");
                    }

                    if (!inPlace && !buildResult.PublicationAuthorized && !options.AllowPartialRecovery)
                    {
                        throw new JetCorruptDataException(JetErrorCode.CorruptPage, "Compacted database requires AllowPartialRecovery to publish partial salvage output.");
                    }

                    string decryptedDestinationPath = PathFor(CompactPinnedDatabaseFile.Destination, 0);
                    string finalArtifactPath = decryptedDestinationPath;
                    long extraReadBytes = 0;
                    long extraWrittenBytes = 0;

                    if (isEncrypted)
                    {
                        string encodedDestinationPath = Path.Combine(workDir, "Encoded-Destination.bin");
                        var encodedWorkspace = new CompactEncodedDatabaseWorkspace(
                            token =>
                            {
                                token.ThrowIfCancellationRequested();
                                return new ValueTask<Stream>(
                                    FileStreamFactory.Open(
                                        decryptedDestinationPath,
                                        FileMode.Open,
                                        FileAccess.Read,
                                        FileShare.Read));
                            },
                            token =>
                            {
                                token.ThrowIfCancellationRequested();
                                return new ValueTask<Stream>(
                                    FileStreamFactory.Open(
                                        encodedDestinationPath,
                                        FileMode.CreateNew,
                                        FileAccess.ReadWrite,
                                        FileShare.None));
                            },
                            token =>
                            {
                                token.ThrowIfCancellationRequested();
                                return new ValueTask<Stream>(
                                    FileStreamFactory.Open(
                                        encodedDestinationPath,
                                        FileMode.Open,
                                        FileAccess.Read,
                                        FileShare.Read));
                            });

                        var encodedBudget = new CompactEncodedDatabaseBudget
                        {
                            MaxPages = options.MaxPages,
                            MaxOutputBytes = checked(options.MaxPages * database.Format.PageSize),
                            MaxReadBytes = options.MaxReadBytes,
                            MaxWriteBytes = options.MaxWriteBytes,
                            MaxWorkingBytes = options.MaxWorkingBytes,
                            MaxEncryptionSpinCount = options.MaxEncryptionSpinCount,
                            MaxTotalEncryptionSpinCount = Math.Max(2L * options.MaxEncryptionSpinCount, 2_000_000L),
                            MaxEncryptionInfoBytes = options.MaxEncryptionInfoBytes,
                        };

                        CompactEncodedDatabaseResult encodedResult = await CompactEncodedDatabaseCopy.EncodeAndValidateAsync(
                            header.AsMemory(0, database.Format.PageSize),
                            options.Password,
                            encodedWorkspace,
                            encodedBudget,
                            cancellationToken).ConfigureAwait(false);
                        extraReadBytes = encodedResult.ReadBytes;
                        extraWrittenBytes = encodedResult.WrittenBytes;
                        finalArtifactPath = encodedDestinationPath;
                    }

                    Enums.DatabaseFormat formatKind = database.Format.Kind;
                    int pageSize = database.Format.PageSize;
                    long sourcePages = database.Pages.PageCount;

                    await publishAsync(finalArtifactPath, cancellationToken).ConfigureAwait(false);

                    return new AccessCompactReport
                    {
                        Format = formatKind,
                        PageSize = pageSize,
                        SourcePages = sourcePages,
                        DestinationPages = buildResult.Validation.Allocations.DestinationPages,
                        RetainedPrefixPages = plan.Reclamation.RetainedPrefixPages,
                        ReclaimedPages = plan.Reclamation.ReclaimedPages,
                        ScrubbedFreePages = plan.Reclamation.ScrubbedPrefixPageCount,
                        TruncatedSourceTailPages = plan.Reclamation.TruncatedTailPages,
                        RebuiltTableCount = buildResult.Validation.Tables,
                        CatalogObjectCount = buildResult.Validation.CatalogObjects,
                        ActiveRowCount = plan.ActiveRowCount,
                        RecoveredCatalogRowCount = plan.RecoveredCatalogRowCount,
                        UnassignedRowCount = Math.Max(plan.UnassignedRowCount, buildResult.Validation.UnassignedRows),
                        ReadBytes = checked(buildResult.ReadBytes + buildResult.Validation.ReadBytes + extraReadBytes),
                        WrittenBytes = checked(buildResult.WrittenBytes + extraWrittenBytes),
                        IsEncrypted = isEncrypted,
                        IsComplete = plan.Reclamation.IsComplete && plan.UnassignedRowCount == 0 && buildResult.Validation.UnassignedRows == 0,
                        DatabaseValidated = buildResult.DatabaseValidated,
                        ReclamationAuthorized = buildResult.ReclamationAuthorized,
                        ReplacementAuthorized = buildResult.PublicationAuthorized,
                        Diagnostics = plan.Diagnostics,
                    };
                }
                finally
                {
                    TryDeleteWorkspaceDirectory(workDir);
                }
            }
            finally
            {
                await database.DisposeAsync().ConfigureAwait(false);
            }
        }
        finally
        {
            CryptographicOperations.ZeroMemory(header);
        }
    }

    private static CompactDiscoveryBudget CreateDiscoveryBudget(AccessCompactOptions options)
        => new()
        {
            MaxPages = options.MaxPages,
            MaxObjects = Math.Max(4_096L, options.MaxTables * 16L),
            MaxCatalogObjects = Math.Min(65_536L, Math.Max(4_096L, options.MaxTables * 16L)),
            MaxDefinitionBytes = options.MaxTableDefinitionBytes,
            MaxSchemaBytes = options.MaxSchemaTraversalBytes,
            MaxWorkingBytes = options.MaxWorkingBytes,
        };

    private static CompactPinnedDatabaseBuilderBudget CreateBuilderBudget(
        AccessCompactOptions options,
        CompactDiscoveryBudget discoveryBudget,
        CompactActiveTableBudget activeTableBudget)
    {
        var tableBudget = new CompactPinnedTableAssemblyBudget
        {
            Discovery = discoveryBudget,
            MaxPages = options.MaxPages,
            MaxOutputBytes = checked(options.MaxPages * Constants.PageSizes.Jet4),
            MaxReadBytes = options.MaxReadBytes,
            MaxWriteBytes = options.MaxWriteBytes,
            MaxWorkingBytes = options.MaxWorkingBytes,
        };

        var globalMapBudget = new CompactGlobalMapBudget
        {
            MaxPages = options.MaxPages,
            MaxReadBytes = options.MaxReadBytes,
            MaxWriteBytes = options.MaxWriteBytes,
            MaxWorkingBytes = options.MaxWorkingBytes,
        };

        var allocationBudget = new CompactGlobalAllocationBudget
        {
            MaxPages = options.MaxPages,
            MaxReadBytes = options.MaxReadBytes,
            MaxWorkingBytes = options.MaxWorkingBytes,
        };

        var validationBudget = new CompactPinnedDatabaseValidatorBudget
        {
            MaxPages = options.MaxPages,
            MaxTables = options.MaxTables,
            MaxReadBytes = options.MaxReadBytes,
            MaxWorkingBytes = options.MaxWorkingBytes,
            Tables = tableBudget,
            GlobalMap = globalMapBudget,
            Allocation = allocationBudget,
            ActiveTables = activeTableBudget,
        };

        return new CompactPinnedDatabaseBuilderBudget
        {
            MaxPages = options.MaxPages,
            MaxOutputBytes = checked(options.MaxPages * Constants.PageSizes.Jet4),
            MaxTables = options.MaxTables,
            MaxReadBytes = options.MaxReadBytes,
            MaxWriteBytes = options.MaxWriteBytes,
            MaxWorkingBytes = options.MaxWorkingBytes,
            Tables = tableBudget,
            ActiveTables = activeTableBudget,
            GlobalMap = globalMapBudget,
            Allocation = allocationBudget,
            Validation = validationBudget,
        };
    }

    private static void TryDeleteWorkspaceDirectory(string workDir)
    {
        try
        {
            if (Directory.Exists(workDir))
            {
                Directory.Delete(workDir, recursive: true);
            }
        }
        catch (IOException)
        {
            // Best-effort cleanup of private temporary workspace.
        }
        catch (UnauthorizedAccessException)
        {
            // Best-effort cleanup of private temporary workspace.
        }
    }
}
