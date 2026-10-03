namespace JetDatabaseWriter.TestSupport;

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Text;
using System.Threading;

/// <summary>
/// Runs a Windows PowerShell host (or any console program) with both output streams captured,
/// and returns within a bounded time even when the host hangs or a child process it started
/// keeps the output pipes open.
/// </summary>
/// <remarks>
/// Reading a redirected stream to its end before waiting for the process, as this code used to,
/// makes the timeout useless: <c>ReadToEnd</c> returns only when every process holding the pipe
/// has exited, so a hung host, or a child it started, blocked the caller for as long as it ran.
/// Here both streams are read through events while the process runs, the timeout applies to the
/// process itself, and a timed-out host is killed together with every process it started. A
/// process in that tree that this user may not terminate is left running, and the run still
/// returns as a timeout instead of throwing.
/// </remarks>
internal static class PowerShellProcessRunner
{
    private static readonly TimeSpan DefaultDrainTimeout = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan KillWaitTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Runs <paramref name="powerShellPath"/> with <paramref name="arguments"/> and waits for it.</summary>
    /// <param name="powerShellPath">Path of the program to run.</param>
    /// <param name="arguments">Arguments, each passed as one argument.</param>
    /// <param name="timeout">How long the process may run before it and every process it started are killed.</param>
    /// <param name="drainTimeout">
    /// How long to wait, after the process exits or is killed, for both output streams to close.
    /// A child process that inherited the pipes can keep them open; the run then returns the
    /// output captured so far with <see cref="PowerShellRunResult.OutputComplete"/> false. Defaults to 5 seconds.
    /// </param>
    /// <returns>The exit code (-1 after a timeout) and the captured output.</returns>
    /// <exception cref="Win32Exception">The program could not be started.</exception>
    /// <exception cref="InvalidOperationException">The process did not start.</exception>
    public static PowerShellRunResult Run(
        string powerShellPath,
        IReadOnlyList<string> arguments,
        TimeSpan timeout,
        TimeSpan? drainTimeout = null)
    {
        var startInfo = new ProcessStartInfo(powerShellPath)
        {
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            CreateNoWindow = true,
        };
        foreach (string argument in arguments)
        {
            startInfo.ArgumentList.Add(argument);
        }

        // The captures outlive the process: disposing it cancels the pending reads, which then
        // report end of stream to these handlers, so they hold nothing that needs disposing.
        var output = new StreamCapture();
        var error = new StreamCapture();
        using var process = new Process { StartInfo = startInfo };
        process.OutputDataReceived += output.OnDataReceived;
        process.ErrorDataReceived += error.OnDataReceived;
        if (!process.Start())
        {
            throw new InvalidOperationException($"Failed to start '{powerShellPath}'.");
        }

        process.BeginOutputReadLine();
        process.BeginErrorReadLine();

        bool timedOut = !process.WaitForExit(timeout);
        if (timedOut)
        {
            KillProcessTree(process);
        }

        TimeSpan drain = drainTimeout ?? DefaultDrainTimeout;
        var drainClock = Stopwatch.StartNew();
        bool outputComplete = output.WaitForEnd(drain);
        TimeSpan remaining = drain - drainClock.Elapsed;
        outputComplete &= error.WaitForEnd(remaining > TimeSpan.Zero ? remaining : TimeSpan.Zero);

        int exitCode = timedOut ? -1 : process.ExitCode;
        return new PowerShellRunResult(exitCode, output.GetText(), error.GetText(), timedOut, outputComplete);
    }

    private static void KillProcessTree(Process process)
    {
        try
        {
            process.Kill(entireProcessTree: true);
        }
        catch (AggregateException)
        {
            // Kill(entireProcessTree: true) kills every process in the tree it can and then
            // reports each one it could not, such as a child running elevated, as a
            // Win32Exception inside one AggregateException; it never throws Win32Exception
            // itself, and a process that has already exited is not an error. The processes it
            // could not kill keep running, and the run still returns as a timeout.
        }

        _ = process.WaitForExit(KillWaitTimeout);
    }

    /// <summary>Collects one redirected stream's lines and records when the stream closes.</summary>
    private sealed class StreamCapture
    {
        private readonly StringBuilder text = new();
        private bool ended;

        public void OnDataReceived(object sender, DataReceivedEventArgs e)
        {
            lock (this.text)
            {
                if (e.Data is null)
                {
                    this.ended = true;
                    Monitor.PulseAll(this.text);
                }
                else
                {
                    this.text.AppendLine(e.Data);
                }
            }
        }

        public bool WaitForEnd(TimeSpan timeout)
        {
            var clock = Stopwatch.StartNew();
            lock (this.text)
            {
                while (!this.ended)
                {
                    TimeSpan remaining = timeout - clock.Elapsed;
                    if (remaining <= TimeSpan.Zero || !Monitor.Wait(this.text, remaining))
                    {
                        return this.ended;
                    }
                }

                return true;
            }
        }

        public string GetText()
        {
            lock (this.text)
            {
                return this.text.ToString();
            }
        }
    }
}
