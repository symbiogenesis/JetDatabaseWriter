namespace JetDatabaseWriter.Tests.Scaffold;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using JetDatabaseWriter;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Emit;
using Xunit;

/// <summary>
/// Compiles scaffolded entity sources together, as the files of one project,
/// against the running framework and the library, so tests can check that the
/// generated code compiles and load the generated types.
/// </summary>
internal static class ScaffoldCompilation
{
    /// <summary>Compiles <paramref name="sources"/> and returns every error and warning, and the assembly when there was no error.</summary>
    /// <param name="sources">The generated source files.</param>
    /// <returns>The diagnostics and the loaded assembly.</returns>
    public static (IReadOnlyList<Diagnostic> Errors, IReadOnlyList<Diagnostic> Warnings, Assembly? Assembly) Compile(IEnumerable<string> sources)
    {
        string trustedAssemblies = (string)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")!;
        IEnumerable<MetadataReference> references = trustedAssemblies
            .Split(Path.PathSeparator, StringSplitOptions.RemoveEmptyEntries)
            .Append(typeof(AccessReader).Assembly.Location)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .Select(path => MetadataReference.CreateFromFile(path));

        var compilation = CSharpCompilation.Create(
            "ScaffoldedEntities_" + Guid.NewGuid().ToString("N"),
            sources.Select(source => CSharpSyntaxTree.ParseText(source)),
            references,
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary, nullableContextOptions: NullableContextOptions.Enable));

        using var image = new MemoryStream();
        EmitResult result = compilation.Emit(image);
        List<Diagnostic> errors = [.. result.Diagnostics.Where(d => d.Severity == DiagnosticSeverity.Error)];
        List<Diagnostic> warnings = [.. result.Diagnostics.Where(d => d.Severity == DiagnosticSeverity.Warning)];
        return (errors, warnings, result.Success ? Assembly.Load(image.ToArray()) : null);
    }

    /// <summary>Compiles <paramref name="sources"/>, fails the test on any error or warning, and returns the assembly.</summary>
    /// <param name="sources">The generated source files.</param>
    /// <returns>The loaded assembly.</returns>
    public static Assembly CompileCleanly(params IEnumerable<string> sources)
    {
        List<string> all = [.. sources];
        (IReadOnlyList<Diagnostic> errors, IReadOnlyList<Diagnostic> warnings, Assembly? assembly) = Compile(all);
        Assert.True(
            errors.Count == 0 && warnings.Count == 0,
            string.Join(Environment.NewLine, errors.Concat(warnings)) + Environment.NewLine + string.Join(Environment.NewLine + "----" + Environment.NewLine, all));
        return assembly!;
    }
}
