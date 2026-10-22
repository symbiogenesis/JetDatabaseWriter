namespace JetDatabaseWriter.Maintenance;

using JetDatabaseWriter.Schema.Models;

/// <summary>One proposed local-table owner and its immutable original logical definition.</summary>
/// <param name="OwnerTDefPage">The original physical definition root, including root 2 for the catalog.</param>
/// <param name="Definition">The independently observed original definition bytes.</param>
internal sealed record CompactDatabasePlanTable(long OwnerTDefPage, TDefImage Definition);
