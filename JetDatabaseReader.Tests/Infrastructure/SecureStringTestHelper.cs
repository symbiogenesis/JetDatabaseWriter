namespace JetDatabaseReader.Tests;

using System.Security;

internal static class SecureStringTestHelper
{
    public static SecureString? FromString(string? value)
    {
        if (value == null)
        {
            return null;
        }

        var secure = new SecureString();
        foreach (char c in value)
        {
            secure.AppendChar(c);
        }

        secure.MakeReadOnly();
        return secure;
    }
}
