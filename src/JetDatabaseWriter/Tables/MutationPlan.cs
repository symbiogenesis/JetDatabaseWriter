namespace JetDatabaseWriter.Tables;

using System;
using System.Collections.Generic;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Pages.Models;

/// <summary>Prepared row changes grouped by physical table in first-use order.</summary>
internal sealed class MutationPlan
{
    private readonly List<TableChanges> tables = [];
    private readonly Dictionary<long, TableChanges> byPage = [];

    /// <summary>Native index maintenance required by a table mutation.</summary>
    internal enum IndexPolicy
    {
        /// <summary>Try incremental maintenance, retaining the full rebuild fallback.</summary>
        Standard = 0,

        /// <summary>Rebuild when the source rows or storage layout cannot supply incremental hints.</summary>
        Rebuild = 1,

        /// <summary>Require the native system-table incremental policy.</summary>
        System = 2,

        /// <summary>Splice new catalog identities into their native indexes.</summary>
        CatalogInsert = 3,

        /// <summary>Maintain catalog keys incrementally, allowing a rebuild only on Jet3.</summary>
        CatalogChange = 4,

        /// <summary>Retain stored catalog keys while relocating a row.</summary>
        CatalogRelocation = 5,
    }

    /// <summary>Gets the affected tables in application order.</summary>
    internal IReadOnlyList<TableChanges> Tables => this.tables;

    /// <summary>Gets the shared change set for one physical table.</summary>
    /// <param name="page">The table-definition page.</param>
    /// <param name="definition">The validated table definition.</param>
    /// <param name="name">The table name used in diagnostics.</param>
    /// <param name="policy">The required native index policy.</param>
    /// <param name="operation">The catalog object or operation used in refusal messages.</param>
    /// <returns>The table's accumulated changes.</returns>
    /// <exception cref="InvalidOperationException">The same physical table requires incompatible native policies.</exception>
    /// <remarks>Contributing planners must resolve the same native layout; the first validated definition is retained.</remarks>
    internal TableChanges ForTable(long page, TableDef definition, string name, IndexPolicy policy = IndexPolicy.Standard, string? operation = null)
    {
        if (!this.byPage.TryGetValue(page, out TableChanges? table))
        {
            table = new TableChanges(page, definition, name, policy, operation);
            this.byPage.Add(page, table);
            this.tables.Add(table);
        }
        else if (table.Policy != policy)
        {
            throw new InvalidOperationException($"Table '{name}' cannot combine different native index policies in one mutation plan.");
        }

        return table;
    }

    /// <summary>A table's composed row changes and native bookkeeping requirements.</summary>
    /// <param name="Page">The table-definition page.</param>
    /// <param name="Definition">The validated descriptor.</param>
    /// <param name="Name">The table name.</param>
    /// <param name="Policy">The native index-maintenance policy.</param>
    /// <param name="Operation">The catalog operation or object used in diagnostics.</param>
    internal sealed record TableChanges(long Page, TableDef Definition, string Name, IndexPolicy Policy, string? Operation)
    {
        /// <summary>Gets the changes in their planned row order.</summary>
        internal List<RowChange> Rows { get; } = [];

        /// <summary>Gets or sets a value indicating whether new values raise the scalar AutoNumber counter.</summary>
        internal bool RaiseAutoNumbers { get; set; }

        /// <summary>Gets or sets a value indicating whether new values raise the complex-reference counter.</summary>
        internal bool RaiseComplexReferences { get; set; }

        /// <summary>Gets the existing system-table pages to reset before reinserting live rows.</summary>
        internal List<long> ResetPages { get; } = [];

        /// <summary>Gets or sets a complete row count for a native system-table rewrite.</summary>
        internal uint? FinalRowCount { get; set; }

        /// <summary>Stages a new row.</summary>
        /// <param name="values">The prepared values.</param>
        /// <param name="updateRowCount">Whether this row raises the table count.</param>
        internal void Insert(object[] values, bool updateRowCount = true)
            => this.Rows.Add(new RowChange(null, null, values, null, DeletedRowDataMode.Default, updateRowCount ? 1 : 0));

        /// <summary>Stages a replacement while retaining unassigned fixed Text and long-value storage.</summary>
        /// <param name="location">The original row slot.</param>
        /// <param name="original">The original decoded or index values.</param>
        /// <param name="replacement">The final prepared values.</param>
        /// <param name="fixedText">Unassigned fixed Text slots copied without encoding.</param>
        /// <param name="dataMode">The original payload's erase policy.</param>
        /// <param name="longValues">Unassigned long-value slots whose storage remains owned by this table.</param>
        internal void Update(RowLocation location, object[] original, object[] replacement, IReadOnlyDictionary<int, byte[]>? fixedText = null, DeletedRowDataMode dataMode = DeletedRowDataMode.Default, IReadOnlyDictionary<int, byte[]>? longValues = null)
            => this.Rows.Add(new RowChange(location, original, replacement, fixedText, dataMode, 0) { LongValues = longValues });

        /// <summary>Stages a deletion; absent original values retain the rebuild fallback.</summary>
        /// <param name="location">The original row slot.</param>
        /// <param name="original">The original decoded or index values, when available.</param>
        /// <param name="dataMode">The payload erase policy.</param>
        internal void Delete(RowLocation location, object[]? original = null, DeletedRowDataMode dataMode = DeletedRowDataMode.Default)
            => this.Rows.Add(new RowChange(location, original, null, null, dataMode, -1));
    }

    /// <summary>One prepared insert, replacement or deletion.</summary>
    /// <param name="Location">The original row slot, absent for an insert.</param>
    /// <param name="Original">Original values used for index removal, when known.</param>
    /// <param name="Replacement">The final values, absent for a deletion.</param>
    /// <param name="FixedText">Unassigned fixed Text storage retained by a replacement.</param>
    /// <param name="DataMode">The original payload's erase policy.</param>
    /// <param name="CountDelta">The row-count adjustment.</param>
    internal readonly record struct RowChange(RowLocation? Location, object[]? Original, object[]? Replacement, IReadOnlyDictionary<int, byte[]>? FixedText, DeletedRowDataMode DataMode, int CountDelta)
    {
        /// <summary>Gets unchanged long-value slots retained byte-for-byte without reallocating their payloads.</summary>
        internal IReadOnlyDictionary<int, byte[]>? LongValues { get; init; }
    }
}
