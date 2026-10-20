namespace JetDatabaseWriter.Maintenance;

using System;
using JetDatabaseWriter.Schema.Models;

/// <summary>One original pinned identity and its verified pointer-only logical patch.</summary>
/// <param name="RootPage">The unchanged physical root identity.</param>
/// <param name="Original">The immutable complete original logical image, including padding.</param>
/// <param name="PatchedBytes">The same-length result of verified reference patching, including unchanged padding.</param>
/// <remarks>All inputs remain immutable throughout installation and independent reopened comparison.</remarks>
internal sealed record CompactTDefChainInstallation(long RootPage, TDefImage Original, ReadOnlyMemory<byte> PatchedBytes);
