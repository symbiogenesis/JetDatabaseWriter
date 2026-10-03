namespace JetDatabaseWriter.Relationships;

using System;
using System.Collections.Generic;
using System.Data;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Enums;
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
    /// <summary>
    /// The cached link list. A single reference: a volatile write of a fully
    /// built list is atomic, so readers never see a torn value.
    /// </summary>
    private volatile List<LinkedTableInfo>? links;

    /// <summary>Returns every linked table defined in the catalog, scanning <c>MSysObjects</c> on first use.</summary>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal async ValueTask<List<LinkedTableInfo>> GetLinkedTablesAsync(CancellationToken cancellationToken)
    {
        List<LinkedTableInfo>? cached = this.links;
        if (cached != null)
        {
            return cached;
        }

        List<LinkedTableInfo> scanned = await LinkedTableManager.GetLinkedTablesAsync(catalog, cancellationToken).ConfigureAwait(false);
        this.links = scanned;
        return scanned;
    }

    /// <summary>Discards the cached link list.</summary>
    internal void ClearCache() => this.links = null;

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
    internal IAsyncEnumerable<T> EnumerateRowsAsync<T>(
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
    /// <param name="forWriteBack">Whether to read the writer's write-back snapshot (<see cref="AccessReader.ReadDataTableForSchemaRewriteAsync"/>) instead of the public typed read.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    internal ValueTask<DataTable?> TryReadDataTableAsync(
        string tableName,
        uint? maxRows,
        IProgress<long>? progress,
        bool forWriteBack,
        CancellationToken cancellationToken) =>
        this.TryReadLinkedTableAsync(
            tableName,
            link => LinkedTableManager.ReadLinkedTextDataTableAsync(policy, link, maxRows, progress, cancellationToken),
            (source, link) => forWriteBack
                ? source.ReadDataTableForSchemaRewriteAsync(link.SourceObjectName, cancellationToken)
                : source.ReadTableAsync(link.SourceObjectName, maxRows, progress, cancellationToken),
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
    internal ValueTask<IReadOnlyList<T>?> TryReadTableAsync<T>(
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

    /// <summary>
    /// Locates the linked-table entry matching <paramref name="tableName"/>
    /// (case-insensitive) or returns <see langword="null"/> when the name does
    /// not refer to a linked table.
    /// </summary>
    /// <param name="tableName">The table name.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    private async ValueTask<LinkedTableInfo?> FindLinkedTableAsync(string tableName, CancellationToken cancellationToken)
    {
        List<LinkedTableInfo> linkedTables = await this.GetLinkedTablesAsync(cancellationToken).ConfigureAwait(false);
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
