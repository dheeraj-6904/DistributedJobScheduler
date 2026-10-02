namespace DistributedJobScheduler.Core.Domain;

/// <summary>
/// Represents the lifecycle state of a job as it moves through the scheduler.
/// </summary>
public enum JobStatus
{
    /// <summary>
    /// Job has been submitted but not yet picked up by the scheduler.
    /// It is waiting for its ScheduledAt time to arrive.
    /// </summary>
    Pending,

    /// <summary>
    /// Scheduler has picked up the job and pushed it to the Redis queue.
    /// It is waiting for a worker to claim it.
    /// </summary>
    Queued,

    /// <summary>
    /// A worker has claimed the job and is actively executing it.
    /// The worker holds a lease (LeaseUntil) that must be renewed via heartbeat.
    /// </summary>
    Running,

    /// <summary>
    /// Job execution succeeded. Terminal state.
    /// </summary>
    Completed,

    /// <summary>
    /// Job execution failed and has been scheduled for a retry.
    /// It will move back to Queued once NextRetryAt is reached.
    /// </summary>
    Retrying,

    /// <summary>
    /// Job has exhausted all retry attempts. Terminal state.
    /// </summary>
    Failed,

    /// <summary>
    /// Job was explicitly cancelled by a client before completion. Terminal state.
    /// </summary>
    Cancelled
}
