namespace JetDatabaseReader;

using System.Collections.Generic;

internal sealed class TableDef
{
    public List<ColumnInfo> Columns { get; set; } = [];

    public long RowCount { get; set; } // num_rows from TDEF page offset 16

    public bool HasDeletedColumns { get; set; } // true if ColNum sequence has gaps
}
