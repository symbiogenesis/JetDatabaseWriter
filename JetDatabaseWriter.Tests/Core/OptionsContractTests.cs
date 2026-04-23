namespace JetDatabaseWriter.Tests;

using System.Security;
using Xunit;

public sealed class OptionsContractTests
{
    [Fact]
    public void AccessReaderOptions_Password_DefaultsToNull()
    {
        var options = new AccessReaderOptions();

        Assert.Null(options.Password);
    }

    [Fact]
    public void AccessWriterOptions_Password_DefaultsToNull()
    {
        var options = new AccessWriterOptions();

        Assert.Null(options.Password);
    }

    [Fact]
    public void AccessReaderOptions_PlainTextConstructor_ConvertsToSecureString()
    {
        var options = new AccessReaderOptions("secret");

        Assert.NotNull(options.Password);
        Assert.True(options.Password!.IsReadOnly());
        Assert.True(SecureStringUtilities.EqualsPlainText(options.Password, "secret"));
    }

    [Fact]
    public void AccessWriterOptions_PlainTextConstructor_ConvertsToSecureString()
    {
        var options = new AccessWriterOptions("secret");

        Assert.NotNull(options.Password);
        Assert.True(options.Password!.IsReadOnly());
        Assert.True(SecureStringUtilities.EqualsPlainText(options.Password, "secret"));
    }

    [Fact]
    public void AccessReaderOptions_Password_InitCopiesAsReadOnly()
    {
        var source = new SecureString();
        source.AppendChar('s');

        var options = new AccessReaderOptions { Password = source };

        Assert.NotNull(options.Password);
        Assert.NotSame(source, options.Password);
        Assert.True(options.Password!.IsReadOnly());
        Assert.True(SecureStringUtilities.EqualsPlainText(options.Password, "s"));
    }

    [Fact]
    public void AccessWriterOptions_Password_InitCopiesAsReadOnly()
    {
        var source = new SecureString();
        source.AppendChar('x');

        var options = new AccessWriterOptions { Password = source };

        Assert.NotNull(options.Password);
        Assert.NotSame(source, options.Password);
        Assert.True(options.Password!.IsReadOnly());
        Assert.True(SecureStringUtilities.EqualsPlainText(options.Password, "x"));
    }

    [Fact]
    public void AccessReaderOptions_PlainTextConstructor_AllowsObjectInitializer()
    {
        var options = new AccessReaderOptions("secret") { PageCacheSize = 123 };

        Assert.Equal(123, options.PageCacheSize);
        Assert.True(SecureStringUtilities.EqualsPlainText(options.Password, "secret"));
    }

    [Fact]
    public void AccessWriterOptions_PlainTextConstructor_AllowsObjectInitializer()
    {
        var options = new AccessWriterOptions("secret") { UseLockFile = false, RespectExistingLockFile = false };

        Assert.False(options.UseLockFile);
        Assert.False(options.RespectExistingLockFile);
        Assert.True(SecureStringUtilities.EqualsPlainText(options.Password, "secret"));
    }
}
