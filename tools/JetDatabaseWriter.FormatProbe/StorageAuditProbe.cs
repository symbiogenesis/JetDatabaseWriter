namespace JetDatabaseWriter.FormatProbe;

using System.Buffers;
using System.CommandLine;
using System.Text.Json;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Maintenance;
using JetDatabaseWriter.Pages.Paging;

/// <summary>Writes a bounded, read-only inventory without claiming relocation or repair.</summary>
internal static class StorageAuditProbe
{
    private const int TerminalReserve = 4096;

    internal static async Task<int> RunCliAsync(string[] args)
    {
        var input = new Option<string>("--input") { Required = true, Description = "Absolute source MDB/ACCDB path; opened read-only." };
        var output = new Option<string>("--output") { Required = true, Description = "Absolute new NDJSON report path; existing files are refused." };
        var maxPages = new Option<long>("--max-pages") { DefaultValueFactory = _ => 1_048_576, Description = "Maximum physical source pages." };
        var maxObjects = new Option<int>("--max-objects") { DefaultValueFactory = _ => 65_536, Description = "Maximum catalog objects." };
        var maxReferences = new Option<long>("--max-references") { DefaultValueFactory = _ => 4_194_304, Description = "Maximum inspected references." };
        var maxDiagnostics = new Option<int>("--max-diagnostics") { DefaultValueFactory = _ => 1024, Description = "Maximum uncertainty diagnostics." };
        var maxSchemaBytes = new Option<int>("--max-schema-bytes") { DefaultValueFactory = _ => 64 * 1024 * 1024, Description = "Maximum aggregate logical definition bytes; not a total heap limit." };
        var maxReportBytes = new Option<long>("--max-report-bytes") { DefaultValueFactory = _ => 64 * 1024 * 1024, Description = "Maximum UTF-8 report bytes, including terminal status." };
        var maxPageReads = new Option<long>("--max-page-reads") { DefaultValueFactory = _ => 8_388_608, Description = "Maximum aggregate reads, including repeated and fallback reads." };
        var maxMapSteps = new Option<long>("--max-map-steps") { DefaultValueFactory = _ => 268_435_456, Description = "Maximum inspected map bytes and directory slots, including empty ones." };
        var maxRecords = new Option<long>("--max-records") { DefaultValueFactory = _ => 8_388_608, Description = "Maximum inventory observations." };
        var maxWorkingBytes = new Option<long>("--max-working-bytes") { DefaultValueFactory = _ => 512 * 1024 * 1024, Description = "Conservative scan-owned storage reservation, including collections and scratch; not a CLR heap measurement." };
        var passwordEnvironment = new Option<string?>("--password-environment") { Description = "Name of an environment variable containing the source password; its value is never reported." };
        var command = new RootCommand("Read-only physical/catalog/TDEF/map inventory. Every region remains pinned; rows, LVAL contents and index trees are not validated.")
        {
            input,
            output,
            maxPages,
            maxObjects,
            maxReferences,
            maxDiagnostics,
            maxSchemaBytes,
            maxReportBytes,
            maxPageReads,
            maxMapSteps,
            maxRecords,
            maxWorkingBytes,
            passwordEnvironment,
        };
        command.SetAction(async (parse, cancellationToken) =>
        {
            string sourcePath = parse.GetRequiredValue(input);
            string reportPath = parse.GetRequiredValue(output);
            long pageLimit = parse.GetValue(maxPages);
            int objectLimit = parse.GetValue(maxObjects);
            long referenceLimit = parse.GetValue(maxReferences);
            int diagnosticLimit = parse.GetValue(maxDiagnostics);
            int schemaLimit = parse.GetValue(maxSchemaBytes);
            long reportLimit = parse.GetValue(maxReportBytes);
            long readLimit = parse.GetValue(maxPageReads);
            long mapStepLimit = parse.GetValue(maxMapSteps);
            long recordLimit = parse.GetValue(maxRecords);
            long workingLimit = parse.GetValue(maxWorkingBytes);
            string? passwordVariable = parse.GetValue(passwordEnvironment);
            if (!Path.IsPathFullyQualified(sourcePath) || !Path.IsPathFullyQualified(reportPath)
                || HasEnvironmentToken(sourcePath) || HasEnvironmentToken(reportPath)
                || pageLimit <= 0 || objectLimit <= 0 || referenceLimit <= 0 || diagnosticLimit <= 0 || schemaLimit <= 0 || reportLimit < TerminalReserve * 2
                || readLimit <= 0 || mapStepLimit <= 0 || recordLimit <= 0 || workingLimit <= 0)
            {
                await Console.Error.WriteLineAsync("Use absolute paths without unresolved environment tokens and positive limits; --max-report-bytes must be at least 8192.");
                return 2;
            }

            string? password = passwordVariable is null ? null : Environment.GetEnvironmentVariable(passwordVariable);
            if (passwordVariable is not null && password is null)
            {
                await Console.Error.WriteLineAsync("The requested password environment variable is absent.");
                return 2;
            }

            var budget = new CompactTraversalBudget
            {
                MaxPages = pageLimit,
                MaxObjects = objectLimit,
                MaxReferences = referenceLimit,
                MaxDiagnostics = diagnosticLimit,
                MaxSchemaBytes = schemaLimit,
                MaxPageReads = readLimit,
                MaxMapSteps = mapStepLimit,
                MaxRecords = recordLimit,
                MaxWorkingBytes = workingLimit,
            };
            return await RunAsync(sourcePath, reportPath, budget, reportLimit, password.AsMemory(), cancellationToken).ConfigureAwait(false);
        });
        return await command.Parse(args).InvokeAsync(cancellationToken: CancellationToken.None).ConfigureAwait(false);
    }

    private static async Task<int> RunAsync(string sourcePath, string reportPath, CompactTraversalBudget budget, long reportLimit, ReadOnlyMemory<char> password, CancellationToken cancellationToken)
    {
        try
        {
            await using var source = new FileStream(sourcePath, FileMode.Open, FileAccess.Read, FileShare.Read, 64 * 1024, FileOptions.Asynchronous);
            PersistentRollbackJournal.RejectHotJournal(source);
            byte[] header = await EncryptionManager.ReadOpenHeaderPageAsync(source, cancellationToken).ConfigureAwait(false);
            if (header[0] != 0 || header[1] != 1 || header[2] != 0 || header[3] != 0)
            {
                throw new JetCorruptDataException(JetErrorCode.MalformedValue, "The source has no native JET/ACE file magic.", new JetErrorInfo { PageNumber = 0 });
            }

            var format = JetFormat.FromHeader(header);
            if (!header.AsSpan(4, format.HeaderSignature.Length).SequenceEqual(format.HeaderSignature))
            {
                throw new JetCorruptDataException(JetErrorCode.MalformedValue, "The source has no matching native JET/ACE signature.", new JetErrorInfo { PageNumber = 0 });
            }

            var options = new AccessReaderOptions
            {
                UseLockFile = false,
                Password = password,
                MaxTableDefinitionBytes = Math.Min(16 * 1024 * 1024, budget.MaxSchemaBytes),
                MaxSchemaTraversalBytes = budget.MaxSchemaBytes,
            };
            await using var database = DatabaseFile.ForReader(source, header, password, sourcePath, leaveOpen: true, options, cancellationToken);
            if (source.Length % database.Format.PageSize != 0)
            {
                throw new JetCorruptDataException(JetErrorCode.MalformedValue, "The source ends with an incomplete database page.");
            }

            await using (var report = new FileStream(reportPath, FileMode.CreateNew, FileAccess.Write, FileShare.Read, 64 * 1024, FileOptions.Asynchronous))
            {
                var log = new AuditLog(report, reportLimit);
                try
                {
                    var headerRecord = new
                    {
                        Kind = "audit-header",
                        SchemaVersion = 1,
                        CoverageScope = "physical-pages/catalog/TDEF-chains/usage-maps",
                        RelocationProven = false,
                        ValuesValidated = false,
                        SourceFormat = database.Format.Kind.ToString(),
                        SourceBytes = source.Length,
                        database.Format.PageSize,
                        Limits = budget,
                        MaxReportBytes = reportLimit,
                    };
                    await log.WriteAsync(headerRecord, cancellationToken).ConfigureAwait(false);
                    await CompactStorageInventory.ScanAsync(database.Pages, database.Format, database.TableDefs, budget, log.WriteAsync, cancellationToken).ConfigureAwait(false);
                    await report.FlushAsync(cancellationToken).ConfigureAwait(false);
                    await log.WriteTerminalAsync(new { Kind = "audit-terminal", InventoryCompleted = true, RelocationProven = false, ValuesValidated = false, Records = log.RecordCount }, cancellationToken).ConfigureAwait(false);
                    await report.FlushAsync(cancellationToken).ConfigureAwait(false);
                }
                catch (Exception error)
                {
                    await log.TryWriteFailureAsync(error).ConfigureAwait(false);
                    throw;
                }
            }

            Console.WriteLine($"Inventory written to {reportPath}. Coverage is limited to physical pages, catalog, TDEF chains and maps; all regions remain pinned.");
            return 0;
        }
        catch (OperationCanceledException)
        {
            await Console.Error.WriteLineAsync("Inventory cancelled; any report is incomplete. The source was opened read-only.");
            return 3;
        }
        catch (Exception error)
        {
            string code = error is IJetException structured ? structured.ErrorCode.ToString() : error.GetType().Name;
            await Console.Error.WriteLineAsync($"Inventory failed ({code}); any report is incomplete. The source was opened read-only.");
            return 1;
        }
    }

    private static bool HasEnvironmentToken(string path)
    {
        for (int start = path.IndexOf('%', StringComparison.Ordinal); start >= 0; start = path.IndexOf('%', start + 1))
        {
            if (path.IndexOf('%', start + 1) > start + 1)
            {
                return true;
            }
        }

        return false;
    }

    private sealed class AuditLog(FileStream stream, long maxBytes)
    {
        private readonly RecordBuffer buffer = new();

        internal long RecordCount { get; private set; }

        internal ValueTask WriteAsync<T>(T record, CancellationToken cancellationToken)
            => this.WriteCoreAsync(record, maxBytes - TerminalReserve, cancellationToken);

        internal ValueTask WriteTerminalAsync<T>(T record, CancellationToken cancellationToken)
            => this.WriteCoreAsync(record, maxBytes, cancellationToken);

        internal async ValueTask TryWriteFailureAsync(Exception error)
        {
            try
            {
                var failureRecord = new
                {
                    Kind = "audit-terminal",
                    InventoryCompleted = false,
                    RelocationProven = false,
                    ValuesValidated = false,
                    Records = this.RecordCount,
                    ErrorCode = error is IJetException structured ? structured.ErrorCode.ToString() : error.GetType().Name,
                    (error as IJetException)?.ErrorInfo.PageNumber,
                };
                await this.WriteTerminalAsync(failureRecord, CancellationToken.None).ConfigureAwait(false);
                await stream.FlushAsync(CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception reportError)
            {
                error.Data["StorageAuditReportFailure"] = reportError.GetType().Name;
            }
        }

        private async ValueTask WriteCoreAsync<T>(T record, long limit, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // The inventory schema has only bounded names and fixed diagnostic
            // text. Refuse a record before serialization can grow an unbounded
            // temporary array, and reserve its newline inside the byte limit.
            int capacity = checked((int)Math.Min(16 * 1024, Math.Max(0, limit - stream.Position - 1)));
            this.buffer.Reset(capacity);
            var writer = new Utf8JsonWriter(this.buffer);
            try
            {
                JsonSerializer.Serialize(writer, record);
            }
            finally
            {
                await writer.DisposeAsync().ConfigureAwait(false);
            }

            ReadOnlyMemory<byte> bytes = this.buffer.CompleteLine();
            long start = stream.Position;
            try
            {
                await stream.WriteAsync(bytes, cancellationToken).ConfigureAwait(false);
                this.RecordCount++;
            }
            catch (Exception error)
            {
                try
                {
                    stream.SetLength(start);
                    stream.Position = start;
                }
                catch (Exception truncateError)
                {
                    error.Data["StorageAuditReportTruncationFailure"] = truncateError.GetType().Name;
                }

                throw;
            }
        }
    }

    private sealed class RecordBuffer : IBufferWriter<byte>
    {
        private readonly byte[] bytes = new byte[(16 * 1024) + 1];
        private int capacity;
        private int written;

        public void Advance(int count)
        {
            if (count < 0 || count > this.capacity - this.written)
            {
                throw ReportLimit();
            }

            this.written += count;
        }

        public Memory<byte> GetMemory(int sizeHint = 0)
        {
            this.CheckSize(sizeHint);
            return this.bytes.AsMemory(this.written, this.capacity - this.written);
        }

        public Span<byte> GetSpan(int sizeHint = 0)
        {
            this.CheckSize(sizeHint);
            return this.bytes.AsSpan(this.written, this.capacity - this.written);
        }

        internal void Reset(int capacity)
        {
            this.capacity = capacity;
            this.written = 0;
        }

        internal ReadOnlyMemory<byte> CompleteLine()
        {
            this.bytes[this.written] = (byte)'\n';
            return this.bytes.AsMemory(0, this.written + 1);
        }

        private static JetLimitationException ReportLimit()
            => new(JetErrorCode.ValueTooLarge, "The inventory report exceeds --max-report-bytes or its 16 KiB record limit.");

        private void CheckSize(int sizeHint)
        {
            if (sizeHint < 0 || Math.Max(1, sizeHint) > this.capacity - this.written)
            {
                throw ReportLimit();
            }
        }
    }
}
