using DistributedJobScheduler.Core.Infrastructure;

namespace DistributedJobScheduler.Scheduler;

/// <summary>
/// The core scheduler loop. Polls PostgreSQL for ready jobs and pushes them to Redis.
/// </summary>
public class DispatcherService : BackgroundService
{
    private readonly IJobRepository _repository;
    private readonly IQueueProvider _queue;
    private readonly ILogger<DispatcherService> _logger;

    public DispatcherService(IJobRepository repository, IQueueProvider queue, ILogger<DispatcherService> logger)
    {
        _repository = repository;
        _queue = queue;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Dispatcher Service started. Polling for pending jobs...");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                // Pull up to 50 ready jobs at a time. 
                // The DB query uses SKIP LOCKED so multiple schedulers can run in parallel without conflicts.
                var jobs = await _repository.DequeueReadyJobsAsync(batchSize: 50, stoppingToken);
                bool hadFailures = false;
                
                for (int i = 0; i < jobs.Count; i++)
                {
                    var job = jobs[i];
                    bool pushed = false;

                    // Immediate retry: try up to 3 attempts with brief backoff
                    for (int attempt = 1; attempt <= 3; attempt++)
                    {
                        try
                        {
                            await _queue.PushAsync(job, stoppingToken);
                            JobMetrics.JobsQueued.Add(1);
                            _logger.LogInformation("Pushed Job {JobId} (Topic: {Topic}, Priority: {Priority}) to Redis.", job.Id, job.Topic, job.Priority);
                            pushed = true;
                            break;
                        }
                        catch (Exception ex) when (ex is not OperationCanceledException)
                        {
                            _logger.LogWarning(ex, "Failed to push Job {JobId} to Redis (attempt {Attempt}/3).", job.Id, attempt);
                            if (attempt < 3)
                            {
                                await Task.Delay(100 * attempt, stoppingToken);
                            }
                        }
                    }

                    if (!pushed)
                    {
                        hadFailures = true;
                        var unpushedJobs = jobs.Skip(i).ToList();
                        var unpushedIds = unpushedJobs.Select(j => j.Id).ToList();

                        _logger.LogError("Failed to push Job {JobId} to Redis after retries. Rolling back {Count} unpushed jobs to Pending.", job.Id, unpushedIds.Count);

                        await _repository.RollbackToPendingAsync(unpushedIds, stoppingToken);
                        foreach (var unpushed in unpushedJobs)
                        {
                            unpushed.MarkAsPending();
                        }

                        break;
                    }
                }

                // If no jobs were found or if Redis failed, sleep to avoid hammering resources.
                // If all jobs were pushed successfully, loop immediately to drain the queue fast.
                if (jobs.Count == 0 || hadFailures)
                {
                    await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogError(ex, "Error occurred while dispatching jobs.");
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken); // Backoff on error
            }
        }
    }
}
