namespace JetDatabaseWriter.Relationships;

using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.ValueDecoding;

/// <summary>
/// Reads linked tables for the reader: discovers the <c>MSysObjects</c> type 4 / 6
/// links (cached until <see cref="ClearCache"/>), reads delimited-text links in
/// place, and reads an Access-file link by opening a separate
/// <see cref="AccessReader"/> on its source database and calling that reader's
/// public API. It never holds the reader that owns it.
/// </summary>
/// <param name="catalog">Reads the <c>MSysObjects</c> rows that define the links.</param>
/// <param name="policy">How link source paths are resolved and opened.</param>
internal sealed class LinkedTableReader(CatalogReader catalog, LinkedSourcePolicy policy)
{
    /// <summary>One fully scanned snapshot shared by public listings and execution policy.</summary>
    private volatile LinkedTableManager.LinkedCatalog? links;

    /// <summary>Returns non-system linked tables, scanning <c>MSysObjects</c> on first use.</summary>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<List<LinkedTableInfo>> GetLinkedTablesAsync(CancellationToken cancellationToken)
        => (await this.GetLinkedCatalogAsync(cancellationToken).ConfigureAwait(false)).VisibleLinks;

    /// <summary>Discards the catalog snapshot used by listings and execution.</summary>
    internal void ClearCache() => this.links = null;

    /// <summary>Refuses unavailable linked execution even when a local cached TDEF exists.</summary>
    /// <param name="tableName">The requested table.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <exception cref="JetNotSupportedException">ODBC execution is unavailable.</exception>
    /// <exception cref="JetAccessDeniedException">No authorized source resolver was supplied.</exception>
    internal async ValueTask ThrowIfExecutionUnavailableAsync(string tableName, CancellationToken cancellationToken)
    {
        LinkedTableInfo? link = await this.FindLinkedTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (link?.Kind == LinkedTableKind.Odbc)
        {
            throw new JetNotSupportedException(JetErrorCode.OdbcRowsNotAvailable, $"Linked ODBC table '{tableName}' exposes cached schema only; ODBC execution is unavailable.", new JetErrorInfo { TableName = tableName });
        }

        if (link is not null && policy.OpenOptions.LinkedSourceResolver is null)
        {
            throw new JetAccessDeniedException(JetErrorCode.LinkedSourceNotPermitted, $"Linked table '{tableName}' requires AccessReaderOptions.LinkedSourceResolver to supply an authorized source stream.", new JetErrorInfo { TableName = tableName });
        }
    }

    /// <summary>Checks whether a name represents external data, independently of cached TDEFs.</summary>
    /// <param name="tableName">The requested table.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>Whether the table is linked.</returns>
    internal async ValueTask<bool> IsLinkedTableAsync(string tableName, CancellationToken cancellationToken)
        => await this.FindLinkedTableAsync(tableName, cancellationToken).ConfigureAwait(false) is not null;

    /// <summary>Returns the kind of a linked table without opening its source.</summary>
    /// <param name="tableName">The requested table.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The link kind, or null for a local or absent table.</returns>
    internal async ValueTask<LinkedTableKind?> GetTableKindAsync(string tableName, CancellationToken cancellationToken)
        => (await this.FindLinkedTableAsync(tableName, cancellationToken).ConfigureAwait(false))?.Kind;

    /// <summary>Counts the rows of a linked table, or returns <see langword="null"/> when <paramref name="tableName"/> is not a link.</summary>
    /// <param name="tableName">The linked table name.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<long?> TryGetRowCountAsync(string tableName, CancellationToken cancellationToken)
    {
        LinkedTableInfo? link = await this.FindLinkedTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (link == null)
        {
            return null;
        }

        if (link.Kind == LinkedTableKind.Text)
        {
            return await LinkedTableManager.CountLinkedTextRowsAsync(policy, link, cancellationToken).ConfigureAwait(false);
        }

        await using AccessReader source = await LinkedTableManager.OpenLinkedSourceAsync(policy, link, cancellationToken).ConfigureAwait(false);
        return await source.GetRealRowCountAsync(link.SourceObjectName, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Streams a linked table's rows as typed object arrays; empty when <paramref name="tableName"/> is not a link.</summary>
    /// <param name="tableName">The linked table name.</param>
    /// <param name="progress">Optional row-count progress sink.</param>
    /// <param name="cancellationToken">A token used to cancel enumeration.</param>
    internal IAsyncEnumerable<object[]> EnumerateRowsAsync(
        string tableName,
        IProgress<long>? progress,
        CancellationToken cancellationToken) =>
        this.EnumerateLinkedTableRowsAsync(
            tableName,
            link => LinkedTableManager.RowsLinkedTextAsStringsAsync(policy, link, progress, cancellationToken),
            (source, link) => source.Rows(link.SourceObjectName, progress, cancellationToken),
            cancellationToken);

    /// <summary>Streams a linked table's rows mapped to <typeparamref name="T"/>; empty when <paramref name="tableName"/> is not a link.</summary>
    /// <typeparam name="T">The mapped row type.</typeparam>
    /// <param name="tableName">The linked table name.</param>
    /// <param name="progress">Optional row-count progress sink.</param>
    /// <param name="cancellationToken">A token used to cancel enumeration.</param>
    internal IAsyncEnumerable<T> EnumerateRowsAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicParameterlessConstructor)] T>(
        string tableName,
        IProgress<long>? progress,
        CancellationToken cancellationToken)
        where T : class, new()
        => this.EnumerateLinkedTableRowsAsync(
            tableName,
            link => LinkedTableManager.RowsLinkedTextMappedAsync(
                policy,
                link,
                progress,
                static metadata => RowMapper<T>.Build(metadata),
                cancellationToken),
            (source, link) => source.Rows<T>(link.SourceObjectName, progress, cancellationToken),
            cancellationToken);

    /// <summary>Streams a linked table's rows as strings; empty when <paramref name="tableName"/> is not a link.</summary>
    /// <param name="tableName">The linked table name.</param>
    /// <param name="progress">Optional row-count progress sink.</param>
    /// <param name="cancellationToken">A token used to cancel enumeration.</param>
    internal IAsyncEnumerable<string[]> EnumerateRowsAsStringsAsync(
        string tableName,
        IProgress<long>? progress,
        CancellationToken cancellationToken) =>
        this.EnumerateLinkedTableRowsAsync(
            tableName,
            link => LinkedTableManager.RowsLinkedTextAsStringsAsync(policy, link, progress, cancellationToken),
            (source, link) => source.RowsAsStrings(link.SourceObjectName, progress, cancellationToken),
            cancellationToken);

    /// <summary>Reads a linked table into a typed <see cref="DataTable"/>, or returns <see langword="null"/> when <paramref name="tableName"/> is not a link.</summary>
    /// <param name="tableName">The linked table name.</param>
    /// <param name="maxRows">Maximum number of rows to read, or <see langword="null"/> for unlimited.</param>
    /// <param name="progress">Optional row-count progress sink.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal ValueTask<DataTable?> TryReadDataTableAsync(
        string tableName,
        uint? maxRows,
        IProgress<long>? progress,
        CancellationToken cancellationToken) =>
        this.TryReadLinkedTableAsync(
            tableName,
            link => LinkedTableManager.ReadLinkedTextDataTableAsync(policy, link, maxRows, progress, cancellationToken),
            (source, link) => source.ReadTableAsync(link.SourceObjectName, maxRows, progress, cancellationToken),
            cancellationToken);

    /// <summary>Reads a linked table into a string-typed <see cref="DataTable"/>, or returns <see langword="null"/> when <paramref name="tableName"/> is not a link.</summary>
    /// <param name="tableName">The linked table name.</param>
    /// <param name="maxRows">Maximum number of rows to read, or <see langword="null"/> for unlimited.</param>
    /// <param name="progress">Optional row-count progress sink.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal ValueTask<DataTable?> TryReadTableAsStringsAsync(
        string tableName,
        uint? maxRows,
        IProgress<long>? progress,
        CancellationToken cancellationToken) =>
        this.TryReadLinkedTableAsync(
            tableName,
            link => LinkedTableManager.ReadLinkedTextDataTableAsync(policy, link, maxRows, progress, cancellationToken),
            (source, link) => source.ReadTableAsStringsAsync(link.SourceObjectName, maxRows, progress, cancellationToken),
            cancellationToken);

    /// <summary>Reads a linked table's rows mapped to <typeparamref name="T"/>, or returns <see langword="null"/> when <paramref name="tableName"/> is not a link.</summary>
    /// <typeparam name="T">The mapped row type.</typeparam>
    /// <param name="tableName">The linked table name.</param>
    /// <param name="maxRows">Maximum number of rows to read, or <see langword="null"/> for unlimited.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal ValueTask<IReadOnlyList<T>?> TryReadTableAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicParameterlessConstructor)] T>(
        string tableName,
        uint? maxRows,
        CancellationToken cancellationToken)
        where T : class, new()
        => this.TryReadLinkedTableAsync(
            tableName,
            link => LinkedTableManager.ReadLinkedTextMappedRowsAsync(
                policy,
                link,
                maxRows,
                static metadata => RowMapper<T>.Build(metadata),
                cancellationToken),
            (source, link) => source.ReadTableAsync<T>(link.SourceObjectName, maxRows, cancellationToken),
            cancellationToken);

    /// <summary>Returns a linked table's column metadata, or <see langword="null"/> when <paramref name="tableName"/> is not a link.</summary>
    /// <param name="tableName">The linked table name.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal ValueTask<IReadOnlyList<ColumnMetadata>?> TryGetColumnMetadataAsync(string tableName, CancellationToken cancellationToken) =>
        this.TryReadLinkedTableAsync(
            tableName,
            link => LinkedTableManager.GetLinkedTextColumnMetadataAsync(policy, link, cancellationToken),
            (source, link) => source.GetColumnMetadataAsync(link.SourceObjectName, cancellationToken),
            cancellationToken);

    /// <summary>Runs a metadata or complex-item read against an approved Access source.</summary>
    /// <typeparam name="TResult">The detached read result.</typeparam>
    /// <param name="tableName">The host table name.</param>
    /// <param name="read">The source operation.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The source result, or null when this is not an Access link.</returns>
    internal async ValueTask<TResult?> TryReadAccessAsync<TResult>(string tableName, Func<AccessReader, string, ValueTask<TResult>> read, CancellationToken cancellationToken)
        where TResult : class
    {
        cancellationToken.ThrowIfCancellationRequested();
        LinkedTableInfo? link = await this.FindLinkedTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (link?.Kind != LinkedTableKind.Access)
        {
            return null;
        }

        await using AccessReader source = await LinkedTableManager.OpenLinkedSourceAsync(policy, link, cancellationToken).ConfigureAwait(false);
        return await read(source, link.SourceObjectName).ConfigureAwait(false);
    }

    /// <summary>Streams index results from the approved Access source, keeping it open through enumeration.</summary>
    /// <typeparam name="TRow">The projected row type.</typeparam>
    /// <param name="tableName">The host table name.</param>
    /// <param name="read">The source operation.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <exception cref="JetNotSupportedException">Text links have no index storage.</exception>
    internal async IAsyncEnumerable<TRow> ReadAccessIndexAsync<TRow>(string tableName, Func<AccessReader, string, IAsyncEnumerable<TRow>> read, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        LinkedTableInfo? link = await this.FindLinkedTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (link is null)
        {
            yield break;
        }

        await this.ThrowIfExecutionUnavailableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (link.Kind == LinkedTableKind.Text)
        {
            throw new JetNotSupportedException(JetErrorCode.LinkedTableHasNoIndexes, $"Linked text table '{tableName}' has no indexes.", new JetErrorInfo { TableName = tableName });
        }

        await using AccessReader source = await LinkedTableManager.OpenLinkedSourceAsync(policy, link, cancellationToken).ConfigureAwait(false);
        await foreach (TRow row in read(source, link.SourceObjectName).WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            yield return row;
        }
    }

    private async ValueTask<LinkedTableManager.LinkedCatalog> GetLinkedCatalogAsync(CancellationToken cancellationToken)
    {
        LinkedTableManager.LinkedCatalog? cached = this.links;
        if (cached is not null)
        {
            return cached;
        }

        LinkedTableManager.LinkedCatalog scanned = await LinkedTableManager.GetLinkedTablesAsync(catalog, cancellationToken).ConfigureAwait(false);
        this.links = scanned;
        return scanned;
    }

    /// <summary>
    /// Locates the linked-table entry matching <paramref name="tableName"/>
    /// (case-insensitive) or returns <see langword="null"/> when the name does
    /// not refer to a linked table.
    /// </summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private async ValueTask<LinkedTableInfo?> FindLinkedTableAsync(string tableName, CancellationToken cancellationToken)
    {
        List<LinkedTableInfo> linkedTables = (await this.GetLinkedCatalogAsync(cancellationToken).ConfigureAwait(false)).ExecutionLinks;
        LinkedTableInfo? link = linkedTables.Find(l => string.Equals(l.Name, tableName, StringComparison.OrdinalIgnoreCase));
        return link is null ? null : link with { };
    }

    private async IAsyncEnumerable<TRow> EnumerateLinkedTableRowsAsync<TRow>(
        string tableName,
        Func<LinkedTableInfo, IAsyncEnumerable<TRow>> readText,
        Func<AccessReader, LinkedTableInfo, IAsyncEnumerable<TRow>> readAccess,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        LinkedTableInfo? link = await this.FindLinkedTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (link == null)
        {
            yield break;
        }

        if (link.Kind == LinkedTableKind.Text)
        {
            await foreach (TRow? row in readText(link).ConfigureAwait(false))
            {
                yield return row;
            }

            yield break;
        }

        await using AccessReader source = await LinkedTableManager.OpenLinkedSourceAsync(policy, link, cancellationToken).ConfigureAwait(false);
        await foreach (TRow? row in readAccess(source, link).ConfigureAwait(false))
        {
            yield return row;
        }
    }

    private async ValueTask<TResult?> TryReadLinkedTableAsync<TResult>(
        string tableName,
        Func<LinkedTableInfo, ValueTask<TResult>> readText,
        Func<AccessReader, LinkedTableInfo, ValueTask<TResult>> readAccess,
        CancellationToken cancellationToken)
        where TResult : class
    {
        LinkedTableInfo? link = await this.FindLinkedTableAsync(tableName, cancellationToken).ConfigureAwait(false);
        if (link == null)
        {
            return null;
        }

        if (link.Kind == LinkedTableKind.Text)
        {
            return await readText(link).ConfigureAwait(false);
        }

        await using AccessReader source = await LinkedTableManager.OpenLinkedSourceAsync(policy, link, cancellationToken).ConfigureAwait(false);
        return await readAccess(source, link).ConfigureAwait(false);
    }
}
