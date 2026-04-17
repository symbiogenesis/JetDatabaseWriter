namespace JetDatabaseReader
{
    using System;

    /// <summary>
    /// Schema entry for a single column in a <see cref="TableResult"/>.
    /// </summary>
    public sealed class TableColumn
    {
        /// <summary>Gets or sets the column name.</summary>
        public string Name { get; set; }

        /// <summary>Gets or sets the CLR type that best represents this column (e.g., <see cref="string"/>, <see cref="int"/>, <see cref="DateTime"/>).</summary>
        public Type Type { get; set; }

        /// <summary>Gets or sets the structured size — use <see cref="ColumnSize.ToString"/> for a human-readable description.</summary>
        public ColumnSize Size { get; set; }
    }
}
