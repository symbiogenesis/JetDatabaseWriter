namespace JetDatabaseWriter.FormatProbe;

using System.Text;
using System.Threading.Tasks;

internal static class Program
{
    public static Task<int> Main(string[] args)
    {
        Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
        return FormatProbeApplication.RunAsync(args);
    }
}
