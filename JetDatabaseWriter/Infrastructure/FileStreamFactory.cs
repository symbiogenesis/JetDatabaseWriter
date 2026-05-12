namespace JetDatabaseWriter.Infrastructure;

using System.IO;

internal static class FileStreamFactory
{
    private const int DefaultBufferSize = 4096;

    public static FileStream Open(
        string path,
        FileMode mode,
        FileAccess access,
        FileShare share,
        FileOptions options = FileOptions.None,
        long preallocationSize = 0)
    {
#if NET6_0_OR_GREATER
        return new FileStream(
            path,
            new FileStreamOptions
            {
                Mode = mode,
                Access = access,
                Share = share,
                Options = options,
                BufferSize = DefaultBufferSize,
                PreallocationSize = preallocationSize,
            });
#else
        _ = preallocationSize;
        return new FileStream(path, mode, access, share, DefaultBufferSize, options);
#endif
    }
}
