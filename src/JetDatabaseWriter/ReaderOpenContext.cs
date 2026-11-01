namespace JetDatabaseWriter;

using System;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Relationships;
using JetDatabaseWriter.ValueDecoding;

/// <summary>One reader's credentials and source provenance, with shared budgets for its linked readers.</summary>
internal sealed class ReaderOpenContext
{
    private ReaderOpenContext(AccessReaderOptions options, ReadOnlyMemory<char> password, string? sourceHint, LinkedSourceTraversal? traversal, EncryptionWorkBudget encryptionWorkBudget, AttachmentReadContext attachmentReadContext, RowReadAccounting rowReads, ComplexItemIdentityReadContext complexItemReads)
    {
        this.Options = options;
        this.Password = password;
        this.SourceHint = sourceHint;
        this.Traversal = traversal;
        this.EncryptionWorkBudget = encryptionWorkBudget;
        this.AttachmentReadContext = attachmentReadContext;
        this.RowReads = rowReads;
        this.ComplexItemReads = complexItemReads;
    }

    /// <summary>Gets unchanged init-only configuration; source credentials come from <see cref="Password"/>.</summary>
    internal AccessReaderOptions Options { get; }

    /// <summary>Gets credentials explicitly supplied for this source.</summary>
    internal ReadOnlyMemory<char> Password { get; }

    /// <summary>Gets a linked source's logical path hint; null identifies an ordinary root open.</summary>
    internal string? SourceHint { get; }

    /// <summary>Gets immutable linked-source ancestry.</summary>
    internal LinkedSourceTraversal? Traversal { get; }

    /// <summary>Gets password work shared by this reader and all linked opens.</summary>
    internal EncryptionWorkBudget EncryptionWorkBudget { get; }

    /// <summary>Gets attachment operation scopes shared by nested and linked reads.</summary>
    internal AttachmentReadContext AttachmentReadContext { get; }

    /// <summary>Gets omission accounting shared by the root reader and authorized linked sources.</summary>
    internal RowReadAccounting RowReads { get; }

    /// <summary>Gets complex-item identity allowances shared by nested and linked reader operations.</summary>
    internal ComplexItemIdentityReadContext ComplexItemReads { get; }

    /// <summary>Creates a fresh root context; an explicit password-work budget supports bounded internal password attempts.</summary>
    /// <param name="options">The caller's configuration.</param>
    /// <param name="workBudget">An explicitly shared internal password-work budget, or null for a new root budget.</param>
    /// <returns>The independent root context.</returns>
    internal static ReaderOpenContext CreateRoot(AccessReaderOptions options, EncryptionWorkBudget? workBudget = null)
    {
        Guard.NotNull(options, nameof(options));
        return new(options, options.Password, null, null, workBudget ?? new EncryptionWorkBudget(options.MaxTotalEncryptionSpinCount), new AttachmentReadContext(options.MaxAttachmentContentBytesPerOperation, options.MaxAttachmentDecodeWorkBytesPerOperation), new RowReadAccounting(), new ComplexItemIdentityReadContext(options.MaxComplexItemIdentityEntriesPerOperation));
    }

    /// <summary>Derives a linked open without inheriting the host password or copying configuration.</summary>
    /// <param name="sourceHint">The logical path used only to anchor further linked hints.</param>
    /// <param name="traversal">The extended immutable ancestry.</param>
    /// <param name="password">Credentials explicitly resolved for this source; empty when none were supplied.</param>
    /// <returns>The linked context with the same aggregate budgets.</returns>
    internal ReaderOpenContext ForLinkedSource(string sourceHint, LinkedSourceTraversal traversal, ReadOnlyMemory<char> password = default)
        => new(this.Options, password, sourceHint, traversal, this.EncryptionWorkBudget, this.AttachmentReadContext, this.RowReads, this.ComplexItemReads);
}
