namespace JetDatabaseWriter.ComplexColumns;

using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.ComplexColumns.Models;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.LongValues;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Models;

internal sealed partial class ComplexColumnReader
{
    private ComplexItemIdentityBudget GetIdentityBudget()
        => complexItemReads?.Budget ?? new(maxComplexItemIdentityEntriesPerOperation);

    private async ValueTask<ItemInventory> ReadItemInventoryAsync(FlatTable flat, string columnName, ComplexItemIdentityBudget budget, CancellationToken cancellationToken)
    {
        int identity = this.FindItemIdentityIndex(flat, columnName);
        int parent = FindForeignKeyIndex(flat.Definition, columnName);
        if (identity < 0 || parent < 0 || identity == parent)
        {
            throw new JetCorruptDataException(JetErrorCode.CorruptComplexColumn, "The complex flat table lacks separate item and parent identities.", new JetErrorInfo { TableName = flat.Name, ColumnName = columnName, PageNumber = flat.TDefPage });
        }

        var inventory = new ItemInventory(identity, parent);
        bool[] selected = new bool[flat.Definition.Columns.Count];
        selected[identity] = true;
        selected[parent] = true;
        LongValueVisitedSet seen = default;
        await foreach ((RowLocation location, object?[] values) in rows.EnumerateLocatedScalarRowsAsync(flat.TDefPage, flat.Definition, selected, cancellationToken).ConfigureAwait(false))
        {
            budget.Observe();
            _ = this.AcceptItemIdentity(flat, values, identity, ref seen, inventory.AmbiguousIds, out int id);
            if (values[parent] is not int reference || reference <= 0)
            {
                var failure = new JetCorruptDataException(JetErrorCode.CorruptComplexColumn, "A complex item has an invalid parent reference.", new JetErrorInfo { TableName = flat.Name, ColumnName = flat.Definition.Columns[parent].Name, PageNumber = location.PageNumber });
                if (rows.StrictParsing)
                {
                    throw failure;
                }

                DiagnosticTrace.Warning("A complex item with an unreadable parent reference was omitted from the scalar identity inventory.");
                continue;
            }

            var item = new ComplexItemLocation(id, reference, location.PageNumber, location.RowIndex);
            inventory.All.Add(item);
            if (!inventory.ParentReferences.TryAdd((item.PageNumber, item.RowIndex), reference))
            {
                throw new JetCorruptDataException(JetErrorCode.CorruptComplexColumn, "The scalar inventory repeats a physical complex item.", new JetErrorInfo { TableName = flat.Name, PageNumber = item.PageNumber });
            }

            if (!inventory.Groups.TryGetValue(reference, out List<ComplexItemLocation>? group))
            {
                group = [];
                inventory.Groups.Add(reference, group);
            }

            group.Add(item);
        }

        return inventory;
    }

    private async IAsyncEnumerable<object?[]> ReadReferenceRowsAsync(
        FlatTable flat,
        string columnName,
        int? parentReference,
        IndexMetadata? index,
        ItemInventory? inventory,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        inventory ??= await this.ReadItemInventoryAsync(flat, columnName, this.GetIdentityBudget(), cancellationToken).ConfigureAwait(false);
        List<ComplexItemLocation> selected = inventory.All;
        if (parentReference is int reference)
        {
            selected = inventory.Groups.TryGetValue(reference, out List<ComplexItemLocation>? group) ? group : [];
            if (index is not null)
            {
                await this.ValidateReferenceIndexAsync(flat, inventory, index, reference, selected.Count, cancellationToken).ConfigureAwait(false);
            }
        }

        var admitted = new List<ComplexItemLocation>(selected.Count);
        foreach (ComplexItemLocation item in selected)
        {
            if (item.Id > 0 && !inventory.AmbiguousIds.Contains(item.Id))
            {
                admitted.Add(item);
            }
        }

        if (admitted.Count == 0)
        {
            yield break;
        }

        await foreach (object?[] row in rows.EnumerateTypedRowsAtLocationsAsync(flat.TDefPage, flat.Definition, admitted, inventory.IdentityColumn, inventory.ParentColumn, cancellationToken).ConfigureAwait(false))
        {
            yield return row;
        }
    }

    private async ValueTask ValidateReferenceIndexAsync(FlatTable flat, ItemInventory inventory, IndexMetadata index, int reference, int expectedCount, CancellationToken cancellationToken)
    {
        using var cursor = new IndexCursor(format.IndexPage, rows.ReadPageAsync, format.PageSize);
        var seen = new HashSet<(long Page, int Row)>();
        await foreach ((long page, int row) in cursor.EnumerateRowLocationsForCriteriaAsync(format, flat.Name, index, flat.Definition, IndexQueryCriteria.KeyPrefix([reference]), cancellationToken).ConfigureAwait(false))
        {
            if (!inventory.ParentReferences.TryGetValue((page, row), out int stored) || stored != reference || !seen.Add((page, row)))
            {
                throw new JetCorruptDataException(JetErrorCode.CorruptIndex, "The complex parent index disagrees with the scalar row inventory.", new JetErrorInfo { PageNumber = page, IndexName = index.Name });
            }
        }

        if (seen.Count != expectedCount)
        {
            throw new JetCorruptDataException(JetErrorCode.CorruptIndex, "The complex parent index omits admitted scalar rows.", new JetErrorInfo { PageNumber = flat.TDefPage, IndexName = index.Name });
        }
    }

    private sealed class ItemInventory(int identityColumn, int parentColumn)
    {
        internal int IdentityColumn { get; } = identityColumn;

        internal int ParentColumn { get; } = parentColumn;

        internal List<ComplexItemLocation> All { get; } = [];

        internal Dictionary<int, List<ComplexItemLocation>> Groups { get; } = [];

        internal Dictionary<(long Page, int Row), int> ParentReferences { get; } = [];

        internal HashSet<int> AmbiguousIds { get; } = [];
    }
}
