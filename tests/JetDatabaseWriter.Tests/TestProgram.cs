namespace JetDatabaseWriter.Tests;

using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Tests.Encryption;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.Tests.Writer;
using Xunit.MicrosoftTestingPlatform;
using Xunit.Runner.InProc.SystemConsole;
using Xunit.v3;

/// <summary>Runs ordinary xUnit tests or one isolated process-termination scenario.</summary>
internal static class TestProgram
{
    private static async Task<int> Main(string[] args)
    {
        if (args.Length > 0 && args[0] == "--jdw-crash-child")
        {
            return await RunCrashChildAsync(args);
        }

        // Preserve the xUnit entry point's MTP and console-runner selection.
        if (args.Any(arg => arg is "--server" or "--internal-msbuild-node"))
        {
            return await TestPlatformTestFramework.RunAsync(args, SelfRegisteredExtensions.AddSelfRegisteredExtensions);
        }

        return await ConsoleRunner.Run(args);
    }

    private static Task RunNormalFailureAsync()
    {
        new CrashRecoveryProcessTests().AssertChild_NormalFailureAfterReadyAndPhase_CannotPassCrashEvidence();
        return Task.CompletedTask;
    }

    private static async Task<int> RunCrashChildAsync(string[] args)
    {
#pragma warning disable CA1031 // Translate child startup/assertion failures into an ordinary exit, never forced-termination evidence.
        try
        {
            if (args.Length != 2)
            {
                throw new ArgumentException("A crash child requires exactly one method name.");
            }

            (string PathVariable, Func<Task> Run) child = args[1] switch
            {
                "JetDatabaseWriter.Tests.Writer.CreationCrashRecoveryTests.Publish_ProcessKilled_PreservesCompleteDestination" =>
                    ("JDW_CREATION_CRASH_PATH", static () => new CreationCrashRecoveryTests().Publish_ProcessKilled_PreservesCompleteDestination()),
                "JetDatabaseWriter.Tests.Writer.TransactionCrashRecoveryTests.InterruptedWrites_RecoverExactOriginalBytes" =>
                    ("JDW_CRASH_TEST_PATH", static () => new TransactionCrashRecoveryTests().InterruptedWrites_RecoverExactOriginalBytes()),
                "JetDatabaseWriter.Tests.Writer.ShrinkCrashRecoveryTests.InterruptedShrink_RecoversExactOriginalOrCommittedImage" =>
                    ("JDW_SHRINK_CRASH_PATH", static () => new ShrinkCrashRecoveryTests().InterruptedShrink_RecoversExactOriginalOrCommittedImage()),
                "JetDatabaseWriter.Tests.Writer.RecoveryReplayCrashTests.ProcessInterruptedReplay_RestoresOriginalOrKeepsCommittedRawImage" =>
                    ("JDW_REPLAY_CRASH_PATH", static () => new RecoveryReplayCrashTests(new TestOutputHelper()).ProcessInterruptedReplay_RestoresOriginalOrKeepsCommittedRawImage()),
                "JetDatabaseWriter.Tests.Encryption.ReplacementCrashRecoveryTests.Commit_ProcessKilled_RetainsCompleteImages" =>
                    ("JDW_REPLACEMENT_CRASH_PATH", static () => new ReplacementCrashRecoveryTests().Commit_ProcessKilled_RetainsCompleteImages()),
                "JetDatabaseWriter.Tests.Encryption.ReplacementCrashRecoveryTests.Recovery_ProcessKilledTwice_CompletesRecordedDecision" =>
                    ("JDW_REPLACEMENT_CRASH_PATH", static () => new ReplacementCrashRecoveryTests().Recovery_ProcessKilledTwice_CompletesRecordedDecision()),
                "JetDatabaseWriter.Tests.Infrastructure.CrashRecoveryProcessTests.AssertChild_NormalFailureAfterReadyAndPhase_CannotPassCrashEvidence" =>
                    ("JDW_CRASH_HELPER_NORMAL_FAILURE", RunNormalFailureAsync),
                _ => throw new ArgumentException("Unknown crash child method: " + args[1]),
            };

            if (Environment.GetEnvironmentVariable(child.PathVariable) is not { Length: > 0 } path || !Path.IsPathFullyQualified(path))
            {
                throw new ArgumentException("Crash child path is missing or is not absolute: " + child.PathVariable);
            }

            using var verification = new SchemaCacheVerificationFixture();
            await child.Run();
            return 0;
        }
        catch (Exception exception)
        {
            await Console.Error.WriteLineAsync(exception.ToString());
            return 1;
        }
#pragma warning restore CA1031
    }
}
