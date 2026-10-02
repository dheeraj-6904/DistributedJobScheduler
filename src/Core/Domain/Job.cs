namespace DistributedJobScheduler.Core.Domain;

/// <summary>
/// Represents a unit of work to be executed by a worker.
/// This is the authoritative job record — PostgreSQL is the source of truth.
/// </summary>
public sealed class Job
{
    /// <summary>Unique identifier for this job.</summary>
    public Guid Id { get; init; }

    /// <summary>
    /// The type of work to perform.
    /// Used by workers to look up the correct handler.
    /// </summary>
    public string Type { get; init; } = string.Empty;

    /// <summary>
    /// The compute resource category. Determines which Redis topic queue
    /// this job is routed to and which worker pools can claim it.
    /// </summary>
    public JobTopic Topic { get; init; }

    /// <summary>
    /// JSON-serialized input data for the job handler.
    /// Stored as JSONB in PostgreSQL.
    /// </summary>
    public string Payload { get; init; } = string.Empty;

    /// <summary>
    /// Scheduling priority. 0 = highest, 10 = lowest.
    /// Combined with ScheduledAt to produce a Redis score with aging.
    /// </summary>
    public JobPriority Priority { get; init; }

    /// <summary>Current lifecycle state of the job.</summary>
    public JobStatus Status { get; private set; }

    /// <summary>Total number of execution attempts made so far.</summary>
    public int Attempts { get; private set; }

    /// <summary>Maximum number of attempts before the job is permanently failed.</summary>
    public int MaxAttempts { get; init; }

    /// <summary>
    /// The earliest time at which this job may be executed.
    /// Always stored in UTC.
    /// </summary>
    public DateTimeOffset ScheduledAt { get; init; }

    /// <summary>
    /// When to next attempt execution after a failure.
    /// Null if the job has not failed or has permanently failed.
    /// </summary>
    public DateTimeOffset? NextRetryAt { get; private set; }

    /// <summary>
    /// The ID of the worker currently holding the execution lease.
    /// Null when the job is not in Running state.
    /// </summary>
    public string? WorkerId { get; private set; }

    /// <summary>
    /// The time at which the current worker's lease expires.
    /// If Now() > LeaseUntil and Status is Running, the job is considered dead
    /// and will be re-queued by the lease sweeper.
    /// </summary>
    public DateTimeOffset? LeaseUntil { get; private set; }

    /// <summary>When this job was first submitted.</summary>
    public DateTimeOffset CreatedAt { get; init; }

    /// <summary>When the job first transitioned to Running. Null until then.</summary>
    public DateTimeOffset? StartedAt { get; private set; }

    /// <summary>When the job reached a terminal state (Completed/Failed/Cancelled).</summary>
    public DateTimeOffset? CompletedAt { get; private set; }

    /// <summary>
    /// Optional client-supplied key to prevent duplicate job submissions.
    /// Enforced via a unique index in PostgreSQL.
    /// </summary>
    public string? IdempotencyKey { get; init; }

    /// <summary>JSON-serialized output from a successful job execution.</summary>
    public string? Result { get; private set; }

    /// <summary>Error message or stack trace from the last failed attempt.</summary>
    public string? Error { get; private set; }

    // -------------------------------------------------------------------------
    // State transitions — all mutation goes through explicit methods
    // so invalid transitions are impossible from the outside.
    // -------------------------------------------------------------------------

    /// <summary>
    /// Transitions the job to Running state when a worker claims it.
    /// </summary>
    public void MarkAsRunning(string workerId, DateTimeOffset leaseUntil, DateTimeOffset now)
    {
        Status   = JobStatus.Running;
        WorkerId = workerId;
        LeaseUntil = leaseUntil;
        StartedAt ??= now; // only set on first attempt
        Attempts++;
    }

    /// <summary>
    /// Transitions the job to Completed. Terminal state.
    /// </summary>
    public void MarkAsCompleted(string? result, DateTimeOffset now)
    {
        Status      = JobStatus.Completed;
        Result      = result;
        CompletedAt = now;
        WorkerId    = null;
        LeaseUntil  = null;
    }

    /// <summary>
    /// Transitions the job to Retrying with a calculated next retry time.
    /// </summary>
    public void MarkAsRetrying(string error, DateTimeOffset nextRetryAt)
    {
        Status       = JobStatus.Retrying;
        Error        = error;
        NextRetryAt  = nextRetryAt;
        WorkerId     = null;
        LeaseUntil   = null;
    }

    /// <summary>
    /// Transitions the job to permanently Failed. Terminal state.
    /// Called when Attempts >= MaxAttempts.
    /// </summary>
    public void MarkAsFailed(string error, DateTimeOffset now)
    {
        Status      = JobStatus.Failed;
        Error       = error;
        CompletedAt = now;
        WorkerId    = null;
        LeaseUntil  = null;
    }

    /// <summary>
    /// Transitions the job to Cancelled. Terminal state.
    /// Only valid from Pending or Queued states.
    /// </summary>
    public void MarkAsCancelled(DateTimeOffset now)
    {
        Status      = JobStatus.Cancelled;
        CompletedAt = now;
        WorkerId    = null;
        LeaseUntil  = null;
    }

    /// <summary>
    /// Extends the worker's lease. Called by the heartbeat service.
    /// </summary>
    public void RenewLease(DateTimeOffset newLeaseUntil) => LeaseUntil = newLeaseUntil;

    // -------------------------------------------------------------------------
    // Convenience queries
    // -------------------------------------------------------------------------

    public bool IsTerminal =>
        Status is JobStatus.Completed or JobStatus.Failed or JobStatus.Cancelled;

    public bool HasRemainingAttempts => Attempts < MaxAttempts;

    public bool IsLeaseExpired(DateTimeOffset now) =>
        Status == JobStatus.Running && LeaseUntil.HasValue && LeaseUntil.Value < now;
}
