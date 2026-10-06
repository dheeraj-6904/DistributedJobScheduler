using DistributedJobScheduler.Core.Domain;
using DistributedJobScheduler.Core.Execution;

namespace DistributedJobScheduler.IntegrationTests.TestHandlers;

/// <summary>Completes instantly so tests don't depend on the simulated production delay.</summary>
public sealed class SucceedingJobHandler : IJobHandler
{
    public Task HandleAsync(Job job, CancellationToken cancellationToken) => Task.CompletedTask;
}
