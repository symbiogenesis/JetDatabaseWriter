namespace JetDatabaseWriter.ComplexColumns.Models;

using System.Collections.Generic;
using JetDatabaseWriter.ComplexColumns;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Relationships;

/// <summary>
/// The hidden flat-table rows that one group of the rows a delete statement
/// removes takes with it, found before the statement writes any row:
/// <see cref="ComplexColumnManager.PlanComplexChildDeletesAsync"/> adds them
/// and <see cref="ComplexColumnManager.ApplyComplexChildDeletesAsync"/>
/// deletes them just before the statement deletes the group's own rows. A
/// group is the rows of one cascade (<see cref="CascadeDelete.FlatRows"/>) or
/// the statement's matching rows. The groups of one statement, each made from
/// another with <see cref="NewGroup"/>, share one record of the flat rows
/// planned, so a flat row that rows of more than one group reach, as when a
/// cascade and the statement itself both delete a parent row, is held once,
/// by the group that planned it first.
/// </summary>
internal sealed class ComplexChildDeletes
{
    private readonly List<(long FlatTDefPage, List<RowLocation> Rows)> tables = [];
    private readonly Dictionary<long, int> tablePositions = [];
    private readonly HashSet<(long PageNumber, int RowIndex)> planned;

    /// <summary>
    /// Initializes a new instance of the <see cref="ComplexChildDeletes"/>
    /// class: the first group of a statement, holding no flat row.
    /// </summary>
    public ComplexChildDeletes()
        : this([])
    {
    }

    private ComplexChildDeletes(HashSet<(long PageNumber, int RowIndex)> planned) => this.planned = planned;

    /// <summary>
    /// Gets each flat table's TDEF page and the rows to delete from it, in
    /// the order the tables were first reached.
    /// </summary>
    public IReadOnlyList<(long FlatTDefPage, List<RowLocation> Rows)> Tables => this.tables;

    /// <summary>
    /// Returns a new, empty group of the same statement, which leaves out
    /// every flat row this group or another group of the statement holds.
    /// </summary>
    /// <returns>The new group.</returns>
    public ComplexChildDeletes NewGroup() => new(this.planned);

    /// <summary>
    /// Adds a row of the flat table at <paramref name="flatTDefPage"/>, unless
    /// this group or another group of the statement already holds it.
    /// </summary>
    /// <param name="flatTDefPage">The flat table's TDEF page.</param>
    /// <param name="row">The flat row.</param>
    public void Add(long flatTDefPage, RowLocation row)
    {
        if (!this.planned.Add((row.PageNumber, row.RowIndex)))
        {
            return;
        }

        if (!this.tablePositions.TryGetValue(flatTDefPage, out int position))
        {
            position = this.tables.Count;
            this.tablePositions[flatTDefPage] = position;
            this.tables.Add((flatTDefPage, []));
        }

        this.tables[position].Rows.Add(row);
    }
}
