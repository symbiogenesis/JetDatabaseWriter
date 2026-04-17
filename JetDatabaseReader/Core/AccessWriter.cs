namespace JetDatabaseReader
{
    using System;
    using System.Collections.Generic;
    using System.IO;

    /// <summary>
    /// Pure-managed writer for Microsoft Access JET databases (.mdb / .accdb).
    /// Supports creating tables, inserting, updating, and deleting rows.
    /// </summary>
    public sealed class AccessWriter : IAccessWriter
    {
        private readonly FileStream _fs;
        private bool _disposed;

        private AccessWriter(FileStream fs)
        {
            _fs = fs;
        }

        /// <summary>
        /// Opens a JET database file for writing and returns a new AccessWriter instance.
        /// </summary>
        /// <param name="path">Path to the .mdb or .accdb file.</param>
        /// <returns>An AccessWriter instance for the specified database.</returns>
        public static AccessWriter Open(string path)
        {
            Guard.NotNullOrEmpty(path, nameof(path));

            if (!File.Exists(path))
            {
                throw new FileNotFoundException($"Database file not found: {path}", path);
            }

            var fs = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.Read);
            return new AccessWriter(fs);
        }

        /// <inheritdoc/>
        public void CreateTable(string tableName, IReadOnlyList<ColumnDefinition> columns)
        {
            Guard.NotNullOrEmpty(tableName, nameof(tableName));
            Guard.NotNull(columns, nameof(columns));
            Guard.NotDisposed(_disposed, nameof(AccessWriter));

            throw new NotImplementedException();
        }

        /// <inheritdoc/>
        public void DropTable(string tableName)
        {
            Guard.NotNullOrEmpty(tableName, nameof(tableName));
            Guard.NotDisposed(_disposed, nameof(AccessWriter));

            throw new NotImplementedException();
        }

        /// <inheritdoc/>
        public void InsertRow(string tableName, object[] values)
        {
            Guard.NotNullOrEmpty(tableName, nameof(tableName));
            Guard.NotNull(values, nameof(values));
            Guard.NotDisposed(_disposed, nameof(AccessWriter));

            throw new NotImplementedException();
        }

        /// <inheritdoc/>
        public int InsertRows(string tableName, IEnumerable<object[]> rows)
        {
            Guard.NotNullOrEmpty(tableName, nameof(tableName));
            Guard.NotNull(rows, nameof(rows));
            Guard.NotDisposed(_disposed, nameof(AccessWriter));

            throw new NotImplementedException();
        }

        /// <inheritdoc/>
        public int UpdateRows(string tableName, string predicateColumn, object predicateValue, IDictionary<string, object> updatedValues)
        {
            Guard.NotNullOrEmpty(tableName, nameof(tableName));
            Guard.NotNullOrEmpty(predicateColumn, nameof(predicateColumn));
            Guard.NotNull(updatedValues, nameof(updatedValues));
            Guard.NotDisposed(_disposed, nameof(AccessWriter));

            throw new NotImplementedException();
        }

        /// <inheritdoc/>
        public int DeleteRows(string tableName, string predicateColumn, object predicateValue)
        {
            Guard.NotNullOrEmpty(tableName, nameof(tableName));
            Guard.NotNullOrEmpty(predicateColumn, nameof(predicateColumn));
            Guard.NotDisposed(_disposed, nameof(AccessWriter));

            throw new NotImplementedException();
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            try
            {
                _fs?.Dispose();
            }
            finally
            {
                _disposed = true;
            }
        }
    }
}
