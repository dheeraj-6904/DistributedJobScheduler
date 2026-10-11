using DistributedJobScheduler.Core.Infrastructure;

namespace DistributedJobScheduler.Scheduler;

/// <summary>
/// Background service that reconciles jobs marked Queued in PostgreSQL that are missing from Redis
/// (e.g., due to process crashes or network failures during dispatch).
/// Identifies stranded jobs and resets them to Pending so DispatcherService re-dispatches them cleanly.
/// </summary>
public class QueueReconciliationService : BackgroundService
{
    private readonly IJobRepository _repository;
    private readonly IQueueProvider _queue;
    private readonly ILogger<QueueReconciliationService> _logger;
    private readonly TimeSpan _staleThreshold;
    private readonly TimeSpan _pollInterval;

    public QueueReconciliationService(
        IJobRepository repository,
        IQueueProvider queue,
        ILogger<QueueReconciliationService> logger,
        TimeSpan? staleThreshold = null,
        TimeSpan? pollInterval = null)
    {
        _repository = repository;
        _queue = queue;
        _logger = logger;
        _staleThreshold = staleThreshold ?? TimeSpan.FromMinutes(1);
        _pollInterval = pollInterval ?? TimeSpan.FromSeconds(30);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "Queue Reconciliation Service started. Scanning for jobs stranded in Queued state older than {ThresholdSeconds}s every {IntervalSeconds}s.",
            _staleThreshold.TotalSeconds,
            _pollInterval.TotalSeconds);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var cutoff = DateTimeOffset.UtcNow.Subtract(_staleThreshold);
                var staleJobs = await _repository.GetStaleQueuedJobsAsync(cutoff, batchSize: 50, stoppingToken);

                if (staleJobs.Count > 0)
                {
                    _logger.LogInformation("Found {Count} stale Queued jobs to reconcile.", staleJobs.Count);

                    var missingFromRedisIds = new List<Guid>();
                    var presentInRedisIds = new List<Guid>();

                    foreach (var job in staleJobs)
                    {
                        bool existsInRedis = await _queue.ContainsAsync(job.Topic, job.Id, stoppingToken);

                        if (!existsInRedis)
                        {
                            missingFromRedisIds.Add(job.Id);
                            _logger.LogWarning("Reconciliation: Job {JobId} (Topic: {Topic}) was marked Queued in PostgreSQL but was missing from Redis. Marking as Pending for DispatcherService.", job.Id, job.Topic);
                        }
                        else
                        {
                            presentInRedisIds.Add(job.Id);
                        }
                    }

                    if (missingFromRedisIds.Count > 0)
                    {
                        // Reset stranded jobs back to Pending so DispatcherService picks them up cleanly
                        await _repository.RollbackToPendingAsync(missingFromRedisIds, stoppingToken);
                        _logger.LogInformation("Reset {Count} stranded jobs back to Pending.", missingFromRedisIds.Count);
                    }

                    if (presentInRedisIds.Count > 0)
                    {
                        // Refresh QueuedAt timestamp to prevent redundant checks on subsequent cycles
                        await _repository.UpdateQueuedAtAsync(presentInRedisIds, DateTimeOffset.UtcNow, stoppingToken);
                    }
                }

                await Task.Delay(_pollInterval, stoppingToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogError(ex, "Error occurred during queue reconciliation sweep.");
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
            }
        }
    }
}
