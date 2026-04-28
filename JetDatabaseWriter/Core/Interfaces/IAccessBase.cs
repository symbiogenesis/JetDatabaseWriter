namespace JetDatabaseWriter.Core.Interfaces;

using System;
using JetDatabaseWriter.Enums;

/// <summary>
/// Base interface for Access database readers and writers.
/// </summary>
public interface IAccessBase : IAsyncDisposable
{
    /// <summary>Gets a value indicating whether the database uses Jet4/ACE format (Access 2000+). When <c>false</c>, the database is Jet3 (Access 97).</summary>
    bool IsJet4 { get; }

    /// <summary>Gets the JET engine format variant (Jet3, Jet4, or ACE/ACCDB).</summary>
    DatabaseFormat DatabaseFormat { get; }

    /// <summary>Gets the page size in bytes (2048 for Jet3, 4096 for Jet4/ACE).</summary>
    int PageSize { get; }

    /// <summary>Gets the ANSI code page used for text encoding in the database (e.g. 1252 for Windows-1252).</summary>
    int CodePage { get; }
}
