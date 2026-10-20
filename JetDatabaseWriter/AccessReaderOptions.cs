namespace JetDatabaseWriter;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Relationships;

/// <summary>
/// Configuration options for opening a JET database with <see cref="AccessReader"/>.
/// </summary>
public sealed class AccessReaderOptions : AccessOptions
{
    /// <summary>
    /// Initializes a new instance of the <see cref="AccessReaderOptions"/> class.
    /// </summary>
    public AccessReaderOptions()
        : base(useByteRangeLocks: false)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="AccessReaderOptions"/> class using a plain-text password.
    /// </summary>
    /// <param name="plainTextPassword">The plain-text password. Null means no password.</param>
    public AccessReaderOptions(string? plainTextPassword)
        : base(plainTextPassword, useByteRangeLocks: false)
    {
    }

    /// <summary>
    /// Gets the maximum stored byte length read for one MEMO or OLE value.
    /// Default: 16,777,215 bytes. The caller may set a positive budget up to the
    /// native 30-bit descriptor maximum of 1,073,741,823 bytes. Larger budgets
    /// permit correspondingly larger allocations for individual values.
    /// </summary>
    public int MaxLongValueBytes { get; init; } = 0xFFFFFF;

    /// <summary>
    /// Gets the maximum uncompressed attachment content length, including its
    /// extension header. Default: 64 MiB. Set a positive value appropriate for
    /// the largest attachment the application permits before opening the file.
    /// This limit applies before decompression, including when parsing is lenient.
    /// </summary>
    public int MaxAttachmentContentBytes { get; init; } = 64 * 1024 * 1024;

    /// <summary>
    /// Gets the maximum aggregate uncompressed attachment content length for one
    /// reader operation, including extension headers. Default: 256 MiB.
    /// Set a positive value appropriate for the largest permitted read before
    /// opening the file. A table scan shares this budget across its attachment
    /// columns, nested table reads and linked sources. Independent operations
    /// start independent budgets, including concurrent operations on one reader.
    /// Budget refusals propagate in strict and lenient parsing modes.
    /// </summary>
    public long MaxAttachmentContentBytesPerOperation { get; init; } = 256 * 1024 * 1024;

    /// <summary>
    /// Gets the maximum aggregate attachment decoding work for one reader operation,
    /// measured in bytes. Default: 2 GiB. Each wrapper reserves its stored byte length
    /// plus four times its declared content length before decoding, bounding compressed
    /// input and content processing together. This includes conservative allowances
    /// for inflation, checksum, extension decoding and payload copying. Increase this
    /// positive limit explicitly for trusted large or costly compressed values.
    /// Nested reads and linked sources share this budget. Refusals propagate in both
    /// strict and lenient modes; cancellation is checked in bounded processing chunks.
    /// </summary>
    public long MaxAttachmentDecodeWorkBytesPerOperation { get; init; } = 2L * 1024 * 1024 * 1024;

    /// <summary>
    /// Gets the maximum number of metadata entries inspected during complex-column
    /// discovery, including aggregate fallback scans for one table read. Default:
    /// 65,536. Set a positive value appropriate for the largest catalog permitted.
    /// Budget refusals propagate in strict and lenient parsing modes.
    /// </summary>
    public int MaxComplexDiscoveryEntries { get; init; } = 65536;

    /// <summary>
    /// Gets the maximum cumulative password-hash iterations for this reader and
    /// every linked source it opens during its lifetime. Default: 10,000,000.
    /// Must be nonnegative. Repeated or failed password attempts consume this
    /// budget; opening a new root reader starts an independent budget, even when
    /// the same options are reused. Increase explicitly for trusted workloads.
    /// </summary>
    public long MaxTotalEncryptionSpinCount { get; init; } = 10_000_000;

    /// <summary>Gets the maximum number of pages to keep in cache. Positive values enable caching; 0 or negative disables it. Default: 256 (1 MB for 4K pages).</summary>
    public int PageCacheSize { get; init; } = 256;

    /// <summary>Gets a value indicating whether verbose diagnostic information is logged. Default: false.</summary>
    public bool DiagnosticsEnabled { get; init; }

    /// <summary>Gets the page-I/O optimization mode. Default: <see cref="PageReadOptimizationMode.Auto" />.</summary>
    public PageReadOptimizationMode PageReadOptimizationMode { get; init; } = PageReadOptimizationMode.Auto;

    /// <summary>Gets a value indicating whether the database header format is validated on open; this does not validate every page or value. Default: true.</summary>
    public bool ValidateOnOpen { get; init; } = true;

    /// <summary>
    /// Gets a value indicating whether malformed assigned values are refused during decoding.
    /// Default: <see langword="true"/>. Audited fixed, variable-area and calculated scalar
    /// faults throw <see cref="Exceptions.JetCorruptDataException"/> with
    /// <see cref="Exceptions.JetErrorCode.MalformedValue"/> in strict mode. Lenient scalar
    /// reads emit a trace warning and return <see cref="DBNull.Value"/> in typed rows,
    /// empty text in string rows, or an unassigned/default POCO property.
    /// Unreadable MEMO/OLE descriptors or chains throw <see cref="Exceptions.JetCorruptDataException"/>
    /// in strict mode; malformed calculated MEMO/OLE envelopes throw
    /// <see cref="Exceptions.JetCorruptDataException"/> with
    /// <see cref="Exceptions.JetErrorCode.MalformedValue"/>. Lenient MEMO/OLE faults
    /// emit a trace diagnostic and return a missing value:
    /// <see cref="DBNull.Value"/> in typed rows, null in string rows, or a default POCO property.
    /// Type-specific decoding coverage remains incomplete; this option does not make every
    /// malformed database readable. Underlying I/O failures and resource-budget refusals
    /// propagate in both modes. POCO assignment-conversion failures remain separate errors.
    /// </summary>
    public bool StrictParsing { get; init; } = true;

    /// <summary>Gets the file access mode. Default: Read.</summary>
    public FileAccess FileAccess { get; init; } = FileAccess.Read;

    /// <summary>
    /// Gets the requested file sharing mode. Default: Read. Write and delete sharing are excluded.
    /// Path-based readers restrict sharing to read access. A caller-supplied FileStream
    /// supplied to OpenAsync additionally requires a compatible read-only lease; resolver
    /// capabilities instead require caller-managed isolation. Windows enforces sharing;
    /// other platforms require cooperating access. Keep the source unchanged while open.
    /// </summary>
    public FileShare FileShare { get; init; } = FileShare.Read;

    /// <summary>
    /// Gets the resolver that supplies an authorized source for an Access or text link.
    /// Without a resolver, linked data execution is refused. The callback receives detached,
    /// untrusted catalog metadata, an absolute path hint, and the operation cancellation token.
    /// Return a fresh readable, seekable stream positioned at zero, or throw to deny access.
    /// Ownership transfers to the reader, including on cancellation or failed parsing.
    /// </summary>
    /// <remarks>
    /// Authorize the actual resource before returning it, using trusted registrations or
    /// host-specific handle verification. Opening a catalog-supplied path after checking its
    /// spelling does not establish authorization: symlinks, hard links and concurrent renames
    /// can refer to another resource. The library reads only the returned stream; it never
    /// opens its filename, lock file or recovery journal. Supply an isolated, committed snapshot
    /// and retain exclusive use of its position until disposal. The resolver is inherited by
    /// nested Access links. Path hints and cycle checks do not establish physical file identity;
    /// the depth limit bounds recursion through aliases. ODBC links remain metadata-only.
    /// </remarks>
    public Func<LinkedTableInfo, string, CancellationToken, ValueTask<Stream>>? LinkedSourceResolver { get; init; }

    /// <summary>Gets the maximum number of successive Access linked-source opens. Default: 32; zero disables Access read-through.</summary>
    public int LinkedSourceMaxDepth { get; init; } = 32;

    /// <summary>
    /// Gets an optional password provider for an authorized Access linked source.
    /// The callback receives a detached link and its resolved absolute path after the resolver supplies an authorized stream.
    /// The host database password is never forwarded automatically. Returned memory must remain unchanged until the linked read completes.
    /// </summary>
    public Func<LinkedTableInfo, string, ReadOnlyMemory<char>>? LinkedSourcePasswordResolver { get; init; }

    /// <summary>
    /// Gets the maximum number of characters accepted in a single linked text/CSV record.
    /// Default: <c>1048576</c> characters.
    /// </summary>
    public int LinkedTextMaxRecordLength { get; init; } = 1_048_576;

    /// <summary>
    /// Gets the maximum number of decoded characters accepted in a single linked text/CSV field.
    /// Default: <c>1048576</c> characters.
    /// </summary>
    public int LinkedTextMaxFieldLength { get; init; } = 1_048_576;

    /// <summary>
    /// Gets the maximum number of columns accepted in a linked text/CSV record.
    /// Default: <c>255</c>, matching the Access table column limit.
    /// </summary>
    public int LinkedTextMaxColumnCount { get; init; } = 255;

    /// <summary>
    /// Gets an optional maximum source-file size, in bytes, for linked text/CSV read-through.
    /// Checks the opened file length and bytes consumed, including growth while reading.
    /// The reader may probe one excess byte to distinguish the end of the file from an exceeded limit.
    /// Leave <see langword="null"/> to allow files of any size while still enforcing record, field, and column limits.
    /// </summary>
    public long? LinkedTextMaxSourceFileBytes { get; init; }

    /// <summary>
    /// Gets an optional maximum number of linked text/CSV rows that fully materializing APIs may load.
    /// Streaming row APIs and row-count scans are not capped by this option.
    /// </summary>
    public uint? LinkedTextMaxMaterializedRows { get; init; }

    /// <summary>Gets the immutable ancestry inherited by an internal linked reader.</summary>
    internal LinkedSourceTraversal? LinkedSourceTraversal { get; init; }

    /// <summary>Gets or sets the budget inherited by linked readers.</summary>
    internal EncryptionWorkBudget? EncryptionWorkBudget { get; set; }

    /// <summary>Gets or sets the operation budget context inherited by linked readers.</summary>
    internal AttachmentReadContext? AttachmentReadContext { get; set; }

    /// <summary>Creates private options with fresh root work budgets, or preserves inherited budgets.</summary>
    /// <returns>The private options used for this reader.</returns>
    internal AccessReaderOptions WithEncryptionWorkBudget()
    {
        if (this.EncryptionWorkBudget is not null && this.AttachmentReadContext is not null)
        {
            return this;
        }

        var options = (AccessReaderOptions)this.MemberwiseClone();
        options.EncryptionWorkBudget ??= new EncryptionWorkBudget(this.MaxTotalEncryptionSpinCount);
        options.AttachmentReadContext ??= new(this.MaxAttachmentContentBytesPerOperation, this.MaxAttachmentDecodeWorkBytesPerOperation);
        return options;
    }

    /// <summary>
    /// Returns whether a reader opened with these options reads its pages
    /// through positional <c>RandomAccess</c> reads, which bypass the I/O gate
    /// and the shared stream position: only when the reader opened the file
    /// by path itself, so it owns a <see cref="FileStream"/> and its handle,
    /// <see cref="PageReadOptimizationMode"/> is not
    /// <see cref="PageReadOptimizationMode.Disabled"/>, and the build has
    /// <c>RandomAccess</c> (.NET 6 or later). A caller's stream, even a
    /// <see cref="FileStream"/>, keeps seek-and-read under the gate, because
    /// the caller may also use its position.
    /// <see cref="AccessReader.OpenAsync(string, AccessReaderOptions?, System.Threading.CancellationToken)"/>
    /// applies this rule, and the test harness opens files by the same rule.
    /// </summary>
    /// <param name="openedFromPath">Whether the reader opened the file by path.</param>
    /// <returns><see langword="true"/> when page reads are positional.</returns>
    internal bool UsesPositionalPageReads(bool openedFromPath)
    {
#if NET6_0_OR_GREATER
        return openedFromPath && this.PageReadOptimizationMode != PageReadOptimizationMode.Disabled;
#else
        // The netstandard2.1 build has no RandomAccess: every page is read through the stream.
        _ = openedFromPath;
        _ = this.PageReadOptimizationMode;
        return false;
#endif
    }
}
