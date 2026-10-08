#if NETSTANDARD2_1
namespace System.Diagnostics.CodeAnalysis;

using System;

// Linker annotations are understood by their full names, including on the netstandard asset.
#pragma warning disable SA1402, SA1649 // Keep framework annotation polyfills together.
#pragma warning disable SA1600 // Framework annotation polyfills are internal implementation details.
#pragma warning disable CA1019 // Matches the framework annotation contract.
#pragma warning disable CA1712 // Matches the framework enum names.
[Flags]
internal enum DynamicallyAccessedMemberTypes
{
    None = 0,
    PublicParameterlessConstructor = 1,
    PublicMethods = 8,
    NonPublicMethods = 16,
    PublicProperties = 512,
}

[AttributeUsage(AttributeTargets.GenericParameter | AttributeTargets.Parameter | AttributeTargets.Field | AttributeTargets.Property | AttributeTargets.ReturnValue)]
internal sealed class DynamicallyAccessedMembersAttribute(DynamicallyAccessedMemberTypes memberTypes) : Attribute
{
    public DynamicallyAccessedMemberTypes MemberTypes { get; } = memberTypes;
}

[AttributeUsage(AttributeTargets.Method | AttributeTargets.Constructor | AttributeTargets.Class, Inherited = false)]
internal sealed class RequiresUnreferencedCodeAttribute(string message) : Attribute
{
    public string Message { get; } = message;
}

[AttributeUsage(AttributeTargets.Method | AttributeTargets.Constructor | AttributeTargets.Class, Inherited = false)]
internal sealed class RequiresDynamicCodeAttribute(string message) : Attribute
{
    public string Message { get; } = message;
}
#endif
