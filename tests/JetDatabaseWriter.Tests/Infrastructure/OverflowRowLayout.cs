namespace JetDatabaseWriter.Tests.Infrastructure;

using System.Diagnostics.CodeAnalysis;

/// <summary>How <see cref="SyntheticOverflowRows.MoveRowAsync"/> lays out the moved row.</summary>
[SuppressMessage("Design", "CA1515:Consider making public types internal", Justification = "Theory parameters of public xUnit test methods must be public.")]
public enum OverflowRowLayout
{
    /// <summary>The row data moves to another data page of the same table.</summary>
    CrossPage = 0,

    /// <summary>The row data moves to a new slot on its own page.</summary>
    SamePage = 1,

    /// <summary>The header points at a second pointer slot, which points at the row data.</summary>
    TwoHop = 2,

    /// <summary>Corrupt: the header points past the end of the file.</summary>
    PointerPastEndOfFile = 3,

    /// <summary>Corrupt: the header points at a row of another table (<c>MSysObjects</c>).</summary>
    PointerToOtherTable = 4,

    /// <summary>Corrupt: the header's slot holds only two bytes, too short for a pointer.</summary>
    ShortPointer = 5,

    /// <summary>Corrupt: the header points at a pointer slot that points back at the header.</summary>
    Cycle = 6,
}
