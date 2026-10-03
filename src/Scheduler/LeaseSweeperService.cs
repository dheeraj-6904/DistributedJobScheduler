using DistributedJobScheduler.Core.Infrastructure;

namespace DistributedJobScheduler.Scheduler;

/// <summary>
/// Background job that periodically checks for dead workers (expired leases) and re-queues their jobs.
/// </summary>
public class LeaseSweeperService : BackgroundService
{
    private readonly IJobRepository _repository;
    private readonly IQueueProvider _queue;
    private readonly ILogger<LeaseSweeperService> _logger;

    public LeaseSweeperService(IJobRepository repository, IQueueProvider queue, ILogger<LeaseSweeperService> logger)
    {
        _repository = repository;
        _queue = queue;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Lease Sweeper Service started. Monitoring for dead workers...");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var resetJobs = await _repository.SweepExpiredLeasesAsync(stoppingToken);

                foreach (var job in resetJobs)
                {
                    // If the job didn't hit MaxAttempts, the repo marked it as Queued. Push it back to Redis.
                    if (job.Status == Core.Domain.JobStatus.Queued)
                    {
                        await _queue.PushAsync(job, stoppingToken);
                        _logger.LogWarning("Worker died while processing Job {JobId}. Re-queued for retry {Attempt}/{MaxAttempts}.", job.Id, job.Attempts, job.MaxAttempts);
                    }
                    else if (job.Status == Core.Domain.JobStatus.Failed)
                    {
                        _logger.LogError("Job {JobId} failed permanently after {MaxAttempts} attempts due to worker timeouts.", job.Id, job.MaxAttempts);
                    }
                }

                // Sweeper doesn't need to run constantly. Every 10 seconds is plenty.
                await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogError(ex, "Error occurred while sweeping dead leases.");
                await Task.Delay(TimeSpan.FromSeconds(15), stoppingToken); // Backoff on error
            }
        }
    }
}
