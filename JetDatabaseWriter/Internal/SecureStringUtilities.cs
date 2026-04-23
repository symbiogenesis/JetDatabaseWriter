namespace JetDatabaseWriter;

using System;
using System.Runtime.InteropServices;
using System.Security;

internal static class SecureStringUtilities
{
    public static SecureString? FromPlainText(string? plainText)
    {
        if (plainText == null)
        {
            return null;
        }

        var secure = new SecureString();
        foreach (char c in plainText)
        {
            secure.AppendChar(c);
        }

        secure.MakeReadOnly();
        return secure;
    }

    public static bool IsNullOrEmpty(SecureString? password)
    {
        return password == null || password.Length == 0;
    }

    public static bool EqualsPlainText(SecureString? password, string plainText)
    {
        if (password == null)
        {
            return false;
        }

        if (plainText == null)
        {
            return false;
        }

        IntPtr ptr = IntPtr.Zero;
        try
        {
            ptr = Marshal.SecureStringToGlobalAllocUnicode(password);
            string decrypted = Marshal.PtrToStringUni(ptr, password.Length) ?? string.Empty;
            return string.Equals(decrypted, plainText, StringComparison.Ordinal);
        }
        finally
        {
            if (ptr != IntPtr.Zero)
            {
                Marshal.ZeroFreeGlobalAllocUnicode(ptr);
            }
        }
    }

    public static SecureString? CopyAsReadOnly(SecureString? password)
    {
        if (password == null)
        {
            return null;
        }

        SecureString copy = password.Copy();
        if (!copy.IsReadOnly())
        {
            copy.MakeReadOnly();
        }

        return copy;
    }
}
