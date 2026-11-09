namespace JetDatabaseWriter.Tests.Infrastructure;

using System;
using JetDatabaseWriter.Enums;

/// <summary>Identifies a fault without formatting successful sweep iterations.</summary>
internal readonly record struct StatementFaultContext(DatabaseFormat Format, string Statement, int Shard, string Mode, int Point)
{
    public override string ToString()
        => FormattableString.Invariant($"format={this.Format}, statement={this.Statement}, shard={this.Shard}, mode={this.Mode}, point={this.Point}");
}
