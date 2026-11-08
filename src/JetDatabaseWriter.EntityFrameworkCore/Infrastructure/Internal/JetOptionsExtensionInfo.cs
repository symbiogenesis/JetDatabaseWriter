namespace JetDatabaseWriter.EntityFrameworkCore.Infrastructure.Internal;

using System;
using System.Collections.Generic;
using System.Globalization;
using Microsoft.EntityFrameworkCore.Infrastructure;

/// <summary>
/// Metadata descriptor for <see cref="JetOptionsExtension"/>.
/// </summary>
/// <param name="extension">The owning <see cref="JetOptionsExtension"/>.</param>
internal sealed class JetOptionsExtensionInfo(IDbContextOptionsExtension extension) : DbContextOptionsExtensionInfo(extension)
{
    private string? logFragment;

    /// <inheritdoc/>
    public override bool IsDatabaseProvider => true;

    /// <inheritdoc/>
    public override string LogFragment
    {
        get
        {
            if (this.logFragment is null)
            {
                JetOptionsExtension ext = this.TypedExtension;
                string source;
                if (ext.DatabasePath is not null)
                {
                    source = $"Path={ext.DatabasePath}";
                }
                else if (ext.DatabaseStream is not null)
                {
                    source = "Source=Stream";
                }
                else
                {
                    source = "Source=StreamFactory";
                }

                this.logFragment = string.Create(CultureInfo.InvariantCulture, $"{source} Format={ext.Format} ");
            }

            return this.logFragment;
        }
    }

    private JetOptionsExtension TypedExtension => (JetOptionsExtension)this.Extension;

    /// <inheritdoc/>
    public override int GetServiceProviderHashCode() => 0;

    /// <inheritdoc/>
    public override bool ShouldUseSameServiceProvider(DbContextOptionsExtensionInfo other)
        => other is JetOptionsExtensionInfo;

    /// <inheritdoc/>
    public override void PopulateDebugInfo(IDictionary<string, string> debugInfo)
    {
        ArgumentNullException.ThrowIfNull(debugInfo);

        debugInfo["JetDatabaseWriter:Format"] = this.TypedExtension.Format.ToString();
    }
}
