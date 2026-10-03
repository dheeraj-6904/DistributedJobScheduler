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
                
                foreach (var job in jobs)
                {
                    await _queue.PushAsync(job, stoppingToken);
                    JobMetrics.JobsQueued.Add(1);
                    _logger.LogInformation("Pushed Job {JobId} (Topic: {Topic}, Priority: {Priority}) to Redis.", job.Id, job.Topic, job.Priority);
                }

                // If no jobs were found, sleep for 2 seconds to avoid hammering the database.
                // If jobs were found, we loop immediately to drain the queue fast.
                if (jobs.Count == 0)
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
