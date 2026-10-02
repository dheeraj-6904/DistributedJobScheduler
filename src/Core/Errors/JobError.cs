namespace DistributedJobScheduler.Core.Errors;

/// <summary>
/// Discriminated union of all known, expected error cases in the job scheduler.
/// Use these as return values instead of throwing exceptions for predictable failures.
/// Exceptions are reserved for truly unexpected/unrecoverable situations.
/// </summary>
public abstract record JobError
{
    /// <summary>No job exists with the given ID.</summary>
    public sealed record NotFound(Guid JobId) : JobError
    {
        public override string ToString() =>
            $"Job '{JobId}' was not found.";
    }

    /// <summary>
    /// The job cannot be cancelled because it is already in a terminal
    /// or non-cancellable state (e.g., Running, Completed, Failed).
    /// </summary>
    public sealed record NotCancellable(Guid JobId, Domain.JobStatus CurrentStatus) : JobError
    {
        public override string ToString() =>
            $"Job '{JobId}' cannot be cancelled because it is in '{CurrentStatus}' state.";
    }

    /// <summary>
    /// A job with this idempotency key has already been submitted.
    /// The existing job's ID is returned so the caller can track it.
    /// </summary>
    public sealed record DuplicateIdempotencyKey(string Key, Guid ExistingJobId) : JobError
    {
        public override string ToString() =>
            $"A job with idempotency key '{Key}' already exists (Id: '{ExistingJobId}').";
    }

    /// <summary>
    /// The supplied priority value is outside the valid 0–10 range.
    /// </summary>
    public sealed record InvalidPriority(int Value) : JobError
    {
        public override string ToString() =>
            $"Priority '{Value}' is invalid. Must be between 0 (highest) and 10 (lowest).";
    }

    /// <summary>
    /// The job's ScheduledAt is in the past beyond the allowed threshold,
    /// or otherwise fails scheduling validation.
    /// </summary>
    public sealed record InvalidSchedule(string Reason) : JobError
    {
        public override string ToString() => $"Invalid schedule: {Reason}";
    }
}
