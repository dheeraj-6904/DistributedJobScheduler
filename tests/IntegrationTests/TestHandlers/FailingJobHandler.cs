using DistributedJobScheduler.Core.Domain;
using DistributedJobScheduler.Core.Execution;

namespace DistributedJobScheduler.IntegrationTests.TestHandlers;

/// <summary>
/// Throws for the first <c>failuresBeforeSuccess</c> invocations, then succeeds.
/// Pass <see cref="int.MaxValue"/> to simulate a job that never succeeds.
/// </summary>
public sealed class FailingJobHandler : IJobHandler
{
    public const string FailureMessage = "Simulated handler failure";

    private readonly int _failuresBeforeSuccess;
    private int _invocations;

    public FailingJobHandler(int failuresBeforeSuccess)
    {
        _failuresBeforeSuccess = failuresBeforeSuccess;
    }

    public int Invocations => Volatile.Read(ref _invocations);

    public Task HandleAsync(Job job, CancellationToken cancellationToken)
    {
        var attempt = Interlocked.Increment(ref _invocations);
        if (attempt <= _failuresBeforeSuccess)
        {
            throw new InvalidOperationException($"{FailureMessage} (invocation {attempt})");
        }

        return Task.CompletedTask;
    }
}
