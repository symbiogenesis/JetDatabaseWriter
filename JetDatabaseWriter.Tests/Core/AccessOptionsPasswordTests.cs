namespace JetDatabaseWriter.Tests.Core;

using System;
using JetDatabaseWriter.Core;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

public sealed class AccessOptionsPasswordTests
{
    [Fact]
    public void AccessReaderOptions_Password_DefaultsToEmpty()
    {
        var options = new AccessReaderOptions();

        Assert.True(options.Password.IsEmpty);
    }

    [Fact]
    public void AccessWriterOptions_Password_DefaultsToEmpty()
    {
        var options = new AccessWriterOptions();

        Assert.True(options.Password.IsEmpty);
    }

    [Fact]
    public void AccessReaderOptions_PlainTextConstructor_StoresPassword()
    {
        var options = new AccessReaderOptions(TestDatabases.AesEncryptedPassword);

        Assert.Equal(TestDatabases.AesEncryptedPassword, options.Password.ToString());
    }

    [Fact]
    public void AccessWriterOptions_PlainTextConstructor_StoresPassword()
    {
        var options = new AccessWriterOptions(TestDatabases.AesEncryptedPassword);

        Assert.Equal(TestDatabases.AesEncryptedPassword, options.Password.ToString());
    }

    [Fact]
    public void AccessReaderOptions_PlainTextConstructor_NullProducesEmpty()
    {
        var options = new AccessReaderOptions(null);

        Assert.True(options.Password.IsEmpty);
    }

    [Fact]
    public void AccessReaderOptions_Password_InitFromMemory()
    {
        var options = new AccessReaderOptions { Password = "s".AsMemory() };

        Assert.Equal("s", options.Password.ToString());
    }

    [Fact]
    public void AccessWriterOptions_Password_InitFromMemory()
    {
        var options = new AccessWriterOptions { Password = "x".AsMemory() };

        Assert.Equal("x", options.Password.ToString());
    }

    [Fact]
    public void AccessReaderOptions_PlainTextConstructor_AllowsObjectInitializer()
    {
        var options = new AccessReaderOptions(TestDatabases.AesEncryptedPassword) { PageCacheSize = 123 };

        Assert.Equal(123, options.PageCacheSize);
        Assert.Equal(TestDatabases.AesEncryptedPassword, options.Password.ToString());
    }

    [Fact]
    public void AccessWriterOptions_PlainTextConstructor_AllowsObjectInitializer()
    {
        var options = new AccessWriterOptions(TestDatabases.AesEncryptedPassword) { UseLockFile = false, RespectExistingLockFile = false };

        Assert.False(options.UseLockFile);
        Assert.False(options.RespectExistingLockFile);
        Assert.Equal(TestDatabases.AesEncryptedPassword, options.Password.ToString());
    }
}
