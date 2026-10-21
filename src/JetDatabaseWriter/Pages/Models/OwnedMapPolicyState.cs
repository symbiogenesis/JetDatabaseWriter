namespace JetDatabaseWriter.Pages.Models;

using System.Collections.Generic;

/// <summary>
/// An <see cref="IOwnedMapPolicy"/>'s decisions at one point in time, by TDEF
/// page. A transaction takes one when it begins and hands it back to
/// <see cref="IOwnedMapPolicy.Restore"/> if it rolls back.
/// </summary>
/// <param name="WritableTdefs">The TDEF pages whose owned-page usage maps the writer may extend.</param>
/// <param name="RefusedTdefs">The TDEF pages whose owned-page usage maps the writer found it may not extend.</param>
internal sealed record OwnedMapPolicyState(
    IReadOnlyCollection<long> WritableTdefs,
    IReadOnlyCollection<long> RefusedTdefs);
