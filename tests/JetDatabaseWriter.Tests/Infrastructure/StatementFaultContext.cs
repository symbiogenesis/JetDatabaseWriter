namespace JetDatabaseWriter.Tests.Infrastructure;

using System;
using JetDatabaseWriter.Enums;

/// <summary>Identifies a fault without formatting successful sweep iterations.</summary>
/// <param name="Format">The database format.</param>
/// <param name="Statement">The statement under test.</param>
/// <param name="Shard">The independent fault shard.</param>
/// <param name="Mode">The injected failure mode.</param>
/// <param name="Point">The physical I/O position that fails.</param>
internal readonly record struct StatementFaultContext(DatabaseFormat Format, string Statement, int Shard, string Mode, int Point)
{
    public override string ToString()
        => FormattableString.Invariant($"format={this.Format}, statement={this.Statement}, shard={this.Shard}, mode={this.Mode}, point={this.Point}");
}
