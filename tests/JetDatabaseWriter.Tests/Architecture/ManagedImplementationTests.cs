namespace JetDatabaseWriter.Tests.Architecture;

using System.IO;
using System.Reflection;
using System.Reflection.Metadata;
using System.Reflection.PortableExecutable;
using Xunit;

/// <summary>Protects the library's fully managed implementation contract.</summary>
public sealed class ManagedImplementationTests
{
    [Fact]
    public void Library_HasNoNativeImportsOrUnmanagedDelegation()
    {
        using FileStream stream = File.OpenRead(typeof(AccessReader).Assembly.Location);
        using var image = new PEReader(stream);
        MetadataReader metadata = image.GetMetadataReader();

        foreach (MethodDefinitionHandle handle in metadata.MethodDefinitions)
        {
            MethodDefinition method = metadata.GetMethodDefinition(handle);
            if ((method.Attributes & MethodAttributes.PinvokeImpl) != 0)
            {
                Assert.Fail($"Library method '{metadata.GetString(method.Name)}' imports native code.");
            }
        }

        foreach (TypeDefinitionHandle handle in metadata.TypeDefinitions)
        {
            TypeDefinition type = metadata.GetTypeDefinition(handle);
            if ((type.Attributes & TypeAttributes.Import) != 0)
            {
                Assert.Fail($"Library type '{metadata.GetString(type.Name)}' imports a COM implementation.");
            }
        }

        foreach (TypeReferenceHandle handle in metadata.TypeReferences)
        {
            TypeReference type = metadata.GetTypeReference(handle);
            if ((metadata.GetString(type.Namespace) == "System.Runtime.InteropServices" &&
                 metadata.GetString(type.Name) == "NativeLibrary") ||
                (metadata.GetString(type.Namespace) == "System.Diagnostics" &&
                 metadata.GetString(type.Name) == "Process"))
            {
                Assert.Fail($"The library delegates through {metadata.GetString(type.Namespace)}.{metadata.GetString(type.Name)}.");
            }
        }
    }
}
