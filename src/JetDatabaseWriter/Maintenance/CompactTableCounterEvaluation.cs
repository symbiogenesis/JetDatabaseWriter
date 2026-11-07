namespace JetDatabaseWriter.Maintenance;

using JetDatabaseWriter.Models;

/// <summary>
/// Evaluated and repaired <c>TDef</c> header counters for a local table.
/// </summary>
/// <param name="OwnerTDefPage">The root TDEF page number of the table.</param>
/// <param name="TableName">The resolved table name.</param>
/// <param name="DeclaredRowCount">The original declared row count in the TDEF header.</param>
/// <param name="RepairedRowCount">The verified or repaired row count.</param>
/// <param name="DeclaredAutoNumber">The original declared AutoNumber seed in the TDEF header.</param>
/// <param name="RepairedAutoNumber">The verified or repaired AutoNumber seed.</param>
/// <param name="DeclaredComplexAutoNumber">The original declared ComplexAutoNumber seed in the TDEF header.</param>
/// <param name="RepairedComplexAutoNumber">The verified or repaired ComplexAutoNumber seed.</param>
/// <param name="CountersRepaired">Whether any TDEF counter requires patching on the destination.</param>
/// <param name="Diagnostic">The non-blocking diagnostic emitted when a corrupt counter was repaired.</param>
internal sealed record CompactTableCounterEvaluation(
    long OwnerTDefPage,
    string TableName,
    uint DeclaredRowCount,
    uint RepairedRowCount,
    uint DeclaredAutoNumber,
    uint RepairedAutoNumber,
    uint DeclaredComplexAutoNumber,
    uint RepairedComplexAutoNumber,
    bool CountersRepaired,
    AccessCompactDiagnostic? Diagnostic);
