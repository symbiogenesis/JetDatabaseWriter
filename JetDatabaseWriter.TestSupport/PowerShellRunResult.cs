namespace JetDatabaseWriter.TestSupport;

/// <summary>Result of one <see cref="PowerShellProcessRunner.Run"/> call.</summary>
/// <param name="ExitCode">The process exit code, or -1 when the run timed out and the process tree was killed.</param>
/// <param name="StandardOutput">Standard output captured before the run returned, one line per line written.</param>
/// <param name="StandardError">Standard error captured before the run returned, one line per line written.</param>
/// <param name="TimedOut"><see langword="true"/> when the process did not exit within the timeout and was killed.</param>
/// <param name="OutputComplete">
/// <see langword="false"/> when a stream was still open after the drain timeout, for example because a
/// child process the script started still holds it; the captured text may then be incomplete.
/// </param>
internal sealed record PowerShellRunResult(
    int ExitCode,
    string StandardOutput,
    string StandardError,
    bool TimedOut,
    bool OutputComplete);
