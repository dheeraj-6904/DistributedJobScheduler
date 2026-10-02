using DistributedJobScheduler.Core.Domain;

namespace DistributedJobScheduler.Core.Scheduling;

/// <summary>
/// Computes the Redis Sorted Set score for a job used to determine dispatch order.
/// Lower score = higher urgency = popped first by ZPOPMIN / BZMPOP.
/// </summary>
/// <remarks>
/// Score formula:
///     Score = ScheduledAt (Unix seconds) + (Priority.Value * AgingCoefficientSeconds)
///
/// Priority aging: because Priority 0 adds 0 seconds and Priority 10 adds
/// (10 * AgingCoefficientSeconds), lower priority jobs have a higher base score.
/// However, as time passes, a waiting Priority 10 job submitted earlier will
/// eventually have a lower absolute score than a fresh Priority 0 job submitted later —
/// preventing indefinite starvation without any background sweeper.
///
/// Example (AgingCoefficient = 600 seconds = 10 minutes per priority point):
///   Job A: Priority 0, ScheduledAt = T+0     → Score = T
///   Job B: Priority 5, ScheduledAt = T+0     → Score = T + 3000
///   Job C: Priority 0, ScheduledAt = T+3001  → Score = T + 3001
///
///   Pop order: A → B → C  (B beats C despite lower priority because it was scheduled earlier)
/// </remarks>
public static class JobScoreCalculator
{
    /// <summary>
    /// The number of seconds each priority unit adds to the score.
    /// Default: 600 seconds (10 minutes) per priority point.
    /// A Priority 10 job acts as if it were submitted 100 minutes "later" than a Priority 0 job.
    /// </summary>
    public const long DefaultAgingCoefficientSeconds = 600;

    /// <summary>
    /// Calculates the Redis score for a job.
    /// </summary>
    /// <param name="priority">The job's priority (0 = highest, 10 = lowest).</param>
    /// <param name="scheduledAt">The time at which the job is eligible to run.</param>
    /// <param name="agingCoefficientSeconds">
    /// Seconds of effective "delay" each priority unit adds. Defaults to 600.
    /// </param>
    /// <returns>
    /// A double suitable for use as a Redis ZADD score.
    /// Lower score = higher urgency.
    /// </returns>
    public static double Calculate(
        JobPriority priority,
        DateTimeOffset scheduledAt,
        long agingCoefficientSeconds = DefaultAgingCoefficientSeconds)
    {
        long scheduledAtUnix = scheduledAt.ToUnixTimeSeconds();
        long agingOffset     = priority.Value * agingCoefficientSeconds;
        return scheduledAtUnix + agingOffset;
    }

    /// <summary>
    /// Convenience overload that takes a Job directly.
    /// </summary>
    public static double Calculate(
        Job job,
        long agingCoefficientSeconds = DefaultAgingCoefficientSeconds)
        => Calculate(job.Priority, job.ScheduledAt, agingCoefficientSeconds);
}
