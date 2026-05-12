namespace JetDatabaseWriter.FormatProbe;

using System.Threading.Tasks;

internal static class Program
{
    public static Task<int> Main(string[] args) => FormatProbeApplication.RunAsync(args);
}
