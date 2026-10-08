namespace JetDatabaseWriter.AotSmoke;

#if !SEED
using System;
using System.Collections.Generic;
using System.IO;
#endif
using System.Threading.Tasks;
#if SEED
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
#else
using GeneratedModels;
#endif

#pragma warning disable IDE0210 // Keep seed and consumer code in the same explicit entry point.
internal static class Program
{
    private static async Task Main(string[] args)
    {
        string path = args[0];
#if SEED
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(path, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false });
        await writer.CreateTableAsync("People", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("Last Name", typeof(string), maxLength: 50), new ColumnDefinition("Score", typeof(double))]);
        await writer.InsertRowAsync("People", [1, "Original", 2.5]);
        await writer.CreateTableAsync("Notes", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("PersonId", typeof(int)), new ColumnDefinition("Body", typeof(string), maxLength: 50)]);
        await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_Notes_People", "People", "Id", "Notes", "PersonId"));
#else
        await using (AccessWriter writer = await AccessWriter.OpenAsync(path, new AccessWriterOptions { UseLockFile = false }))
        {
            await writer.InsertRowAsync("People", new People { Id = 2, LastName = "Generated", Score = 7.5 });
        }

        await using AccessReader reader = await AccessReader.OpenAsync(path, new AccessReaderOptions { UseLockFile = false });
        IReadOnlyList<People> rows = await reader.ReadTableAsync<People>("People");
        if (rows.Count != 2 || rows[1].Id != 2 || rows[1].LastName != "Generated" || rows[1].Score != 7.5)
        {
            throw new InvalidDataException("Generated entity typed insert/read did not round trip.");
        }

        int filtered = 0;
        await foreach (People row in reader.Rows<People>("People", item => item.Id == 2))
        {
            filtered += row.LastName == "Generated" ? 1 : 0;
        }

        if (filtered != 1)
        {
            throw new InvalidDataException("Generated entity predicate read did not round trip.");
        }

        int count = 0;
        await foreach (People row in reader.Rows<People>("People"))
        {
            count += row.Id > 0 ? 1 : 0;
        }

        if (count != 2)
        {
            throw new InvalidDataException("Generated entity streaming read did not round trip.");
        }

#pragma warning disable CA1303 // Smoke-consumer success marker is not localized.
        Console.WriteLine("Generated entities passed typed insert, buffered read and streaming read.");
#endif
    }
}
