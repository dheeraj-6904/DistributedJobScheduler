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

                var queuedJobs = resetJobs.Where(j => j.Status == Core.Domain.JobStatus.Queued).ToList();
                for (int i = 0; i < queuedJobs.Count; i++)
                {
                    var job = queuedJobs[i];
                    bool pushed = false;

                    for (int attempt = 1; attempt <= 3; attempt++)
                    {
                        try
                        {
                            await _queue.PushAsync(job, stoppingToken);
                            JobMetrics.JobsQueued.Add(1);
                            _logger.LogWarning("Worker died while processing Job {JobId}. Re-queued for retry {Attempt}/{MaxAttempts}.", job.Id, job.Attempts, job.MaxAttempts);
                            pushed = true;
                            break;
                        }
                        catch (Exception ex) when (ex is not OperationCanceledException)
                        {
                            _logger.LogWarning(ex, "Failed to re-queue swept Job {JobId} to Redis (attempt {Attempt}/3).", job.Id, attempt);
                            if (attempt < 3)
                            {
                                await Task.Delay(100 * attempt, stoppingToken);
                            }
                        }
                    }

                    if (!pushed)
                    {
                        var unpushedJobs = queuedJobs.Skip(i).ToList();
                        var unpushedIds = unpushedJobs.Select(j => j.Id).ToList();
                        _logger.LogError("Failed to re-queue swept Job {JobId} to Redis after retries. Rolling back {Count} jobs to Pending.", job.Id, unpushedIds.Count);

                        await _repository.RollbackToPendingAsync(unpushedIds, stoppingToken);
                        foreach (var unpushed in unpushedJobs)
                        {
                            unpushed.MarkAsPending();
                        }

                        break;
                    }
                }

                foreach (var job in resetJobs.Where(j => j.Status == Core.Domain.JobStatus.Failed))
                {
                    _logger.LogError("Job {JobId} failed permanently after {MaxAttempts} attempts due to worker timeouts.", job.Id, job.MaxAttempts);
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
