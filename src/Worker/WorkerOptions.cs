using Microsoft.Extensions.Options;

namespace DistributedJobScheduler.Worker;

/// <summary>
/// Configuration options controlling Worker execution behavior.
/// </summary>
public sealed class WorkerOptions
{
    public const string SectionName = "Worker";
    public const int DefaultConcurrency = 4;

    /// <summary>
    /// Maximum number of jobs this worker instance may execute concurrently. Must be at least 1.
    /// </summary>
    public int Concurrency { get; set; } = DefaultConcurrency;

    /// <summary>
    /// Maximum time to wait for in-flight jobs to finish during graceful shutdown.
    /// </summary>
    public TimeSpan ShutdownTimeout { get; set; } = TimeSpan.FromSeconds(30);

    public void Validate()
    {
        if (Concurrency < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(Concurrency),
                Concurrency,
                "Worker:Concurrency must be at least 1.");
        }

        if (ShutdownTimeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(ShutdownTimeout),
                ShutdownTimeout,
                "Worker:ShutdownTimeout must be greater than zero.");
        }
    }
}

public sealed class WorkerOptionsValidator : IValidateOptions<WorkerOptions>
{
    public ValidateOptionsResult Validate(string? name, WorkerOptions options)
    {
        if (options.Concurrency < 1)
        {
            return ValidateOptionsResult.Fail(
                $"Worker:Concurrency must be at least 1, but was {options.Concurrency}.");
        }

        if (options.ShutdownTimeout <= TimeSpan.Zero)
        {
            return ValidateOptionsResult.Fail(
                $"Worker:ShutdownTimeout must be greater than zero, but was {options.ShutdownTimeout}.");
        }

        return ValidateOptionsResult.Success;
    }
}
