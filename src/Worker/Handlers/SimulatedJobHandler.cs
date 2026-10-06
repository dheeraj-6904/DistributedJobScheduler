using DistributedJobScheduler.Core.Domain;
using DistributedJobScheduler.Core.Execution;

namespace DistributedJobScheduler.Worker.Handlers;

/// <summary>
/// Simulates the actual business logic execution.
/// </summary>
public class SimulatedJobHandler : IJobHandler
{
    public virtual async Task HandleAsync(Job job, CancellationToken cancellationToken)
    {
        // Simulate a job that takes anywhere from 500ms to 1000ms to complete
        var randomDelay = new Random().Next(500, 1000);
        await Task.Delay(randomDelay, cancellationToken);
    }
}
