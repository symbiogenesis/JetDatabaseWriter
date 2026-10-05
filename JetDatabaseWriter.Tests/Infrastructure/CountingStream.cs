namespace JetDatabaseWriter.Tests.Infrastructure;

using System.IO;
using JetDatabaseWriter.TestSupport;

/// <summary>Test-local name for the shared physical page trace stream.</summary>
/// <param name="inner">The caller-owned backing stream.</param>
internal sealed class CountingStream(Stream inner) : PageTraceStream(inner);