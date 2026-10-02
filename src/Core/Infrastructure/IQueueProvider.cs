using DistributedJobScheduler.Core.Domain;

namespace DistributedJobScheduler.Core.Infrastructure;

/// <summary>
/// Handles all fast queueing operations (Redis).
/// </summary>
public interface IQueueProvider
{
    /// <summary>
    /// Pushes a job to the Redis Sorted Set matching its Topic.
    /// Uses the JobScoreCalculator to determine its ZADD score.
    /// </summary>
    Task PushAsync(Job job, CancellationToken cancellationToken);

    /// <summary>
    /// Blocking pop (BZMPOP) from multiple topic queues.
    /// Waits until a job is available in any of the specified topics,
    /// pops the one with the lowest score, and returns its JobId.
    /// </summary>
    Task<Guid?> PopAsync(IEnumerable<JobTopic> topics, TimeSpan timeout, CancellationToken cancellationToken);
}
