namespace JetDatabaseWriter.Schema;

using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Pages;

/// <summary>The complete format-specific table definition layout.</summary>
/// <param name="Header">The table header layout.</param>
/// <param name="Column">The column descriptor layout.</param>
/// <param name="Index">The index descriptor and relationship entry layout.</param>
internal readonly record struct TDefFormat(TDefHeaderLayout Header, ColumnDescriptorLayout Column, IndexLayout Index);
