using DistributedJobScheduler.Core.Domain;

namespace DistributedJobScheduler.Core.Execution;

public interface IJobHandler
{
    Task HandleAsync(Job job, CancellationToken cancellationToken);
}
