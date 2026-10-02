using DistributedJobScheduler.Core.Domain;

namespace DistributedJobScheduler.Core.Infrastructure;

/// <summary>
/// Handles all PostgreSQL database operations for jobs.
/// </summary>
public interface IJobRepository
{
    /// <summary>Retrieves a job by its unique ID.</summary>
    Task<Job?> GetJobAsync(Guid id, CancellationToken cancellationToken);

    /// <summary>Inserts a new job. Enforces idempotency key uniqueness.</summary>
    Task<bool> CreateJobAsync(Job job, CancellationToken cancellationToken);

    /// <summary>
    /// The critical Scheduler loop query:
    /// 1. Finds PENDING jobs where ScheduledAt <= Now.
    /// 2. Uses SELECT ... FOR UPDATE SKIP LOCKED to prevent concurrent schedulers from claiming the same jobs.
    /// 3. Updates their status to QUEUED and returns them.
    /// </summary>
    Task<IReadOnlyList<Job>> DequeueReadyJobsAsync(int batchSize, CancellationToken cancellationToken);

    /// <summary>
    /// The Lease Sweeper query:
    /// Finds RUNNING jobs where LeaseUntil < Now, increments Attempts, and resets them to QUEUED or FAILED.
    /// Returns the jobs that were reset so they can be re-pushed to Redis if necessary.
    /// </summary>
    Task<IReadOnlyList<Job>> SweepExpiredLeasesAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Persists state changes (Status, Attempts, WorkerId, etc.) back to the database.
    /// </summary>
    Task UpdateJobAsync(Job job, CancellationToken cancellationToken);
    
    /// <summary>
    /// Extends leases for a batch of running jobs. Used by the worker heartbeat.
    /// </summary>
    Task RenewLeasesAsync(IEnumerable<Guid> jobIds, DateTimeOffset newLeaseUntil, CancellationToken cancellationToken);
}
