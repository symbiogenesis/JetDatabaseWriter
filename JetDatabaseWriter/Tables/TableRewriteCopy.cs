namespace JetDatabaseWriter.Tables;

using System.Collections.Generic;
using JetDatabaseWriter.Catalog.Models;

/// <summary>The copied replacement ready to take ownership of the original table's catalog identity.</summary>
/// <param name="Name">The temporary catalog name.</param>
/// <param name="Entry">The temporary storage identity.</param>
/// <param name="Definition">The replacement schema including emitted foreign-key indexes.</param>
/// <param name="FinalTDefPage">The final table-definition identity.</param>
/// <param name="FkIndexNumbers">The original-to-replacement FK index mapping.</param>
internal sealed record TableRewriteCopy(
    string Name,
    CatalogEntry Entry,
    TableDef Definition,
    long FinalTDefPage,
    IReadOnlyDictionary<int, int> FkIndexNumbers);
