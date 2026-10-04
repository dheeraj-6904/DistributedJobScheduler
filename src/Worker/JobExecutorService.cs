using DistributedJobScheduler.Core.Domain;
using DistributedJobScheduler.Core.Infrastructure;

namespace DistributedJobScheduler.Worker;

/// <summary>
/// The core Worker loop. Pops from Redis, marks as Running in Postgres, executes, and marks as Completed.
/// </summary>
public sealed class JobExecutorService : BackgroundService
{
    private readonly IQueueProvider _queue;
    private readonly IJobRepository _repository;
    private readonly ActiveJobTracker _tracker;
    private readonly ILogger<JobExecutorService> _logger;
    private readonly string _workerId;

    public JobExecutorService(IQueueProvider queue, IJobRepository repository, ActiveJobTracker tracker, ILogger<JobExecutorService> logger)
    {
        _queue = queue;
        _repository = repository;
        _tracker = tracker;
        _logger = logger;
        
        // Generate a unique ID for this instance so we can track which machine took the job
        _workerId = Guid.NewGuid().ToString("N"); 
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Job Executor started on Worker ID: {WorkerId}", _workerId);

        // Define which queues this specific worker listens to (could easily be driven by config)
        var topicsToListenTo = new[] { JobTopic.Default, JobTopic.CpuIntensive };

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                // 1. Wait for a job from Redis (Blocking Pop). Will wait up to 5 seconds.
                var poppedJobId = await _queue.PopAsync(topicsToListenTo, TimeSpan.FromSeconds(5), stoppingToken);

                if (poppedJobId == null)
                {
                    continue; // Timeout reached, just loop again and wait another 5 seconds
                }

                // 2. We got an ID from Redis! Fetch the full payload from PostgreSQL
                var job = await _repository.GetJobAsync(poppedJobId.Value, stoppingToken);
                
                if (job == null || job.Status != JobStatus.Queued)
                {
                    _logger.LogWarning("Job {JobId} not found or was not in Queued state (State: {State}).", poppedJobId, job?.Status);
                    continue;
                }

                // 3. Mark it as Running in Postgres and track it so the HeartbeatService renews it
                var now = DateTimeOffset.UtcNow;
                var initialLease = now.AddSeconds(30);
                
                job.MarkAsRunning(_workerId, initialLease, now);
                await _repository.UpdateJobAsync(job, stoppingToken);
                
                _tracker.Add(job.Id);

                // Record the time spent in queue/scheduler
                var queueTimeMs = (DateTimeOffset.UtcNow - job.ScheduledAt).TotalMilliseconds;
                if (queueTimeMs > 0)
                {
                    JobMetrics.JobQueueTime.Record(queueTimeMs);
                }

                // 4. EXECUTE THE WORK
                _logger.LogInformation("Started executing Job {JobId} (Type: {Type})", job.Id, job.Type);
                var stopwatch = System.Diagnostics.Stopwatch.StartNew();
                
                try
                {
                    await ProcessJobSimulatedAsync(job, stoppingToken);
                    
                    stopwatch.Stop();
                    JobMetrics.JobExecutionTime.Record(stopwatch.ElapsedMilliseconds);

                    // 5. Execution finished successfully. Mark as Completed.
                    var completedAt = DateTimeOffset.UtcNow;
                    job.MarkAsCompleted("{\"status\":\"success\"}", completedAt);
                    await _repository.UpdateJobAsync(job, stoppingToken);
                    
                    var lifecycleTimeMs = (completedAt - job.CreatedAt).TotalMilliseconds;
                    if (lifecycleTimeMs > 0)
                    {
                        JobMetrics.JobTotalLifecycleTime.Record(lifecycleTimeMs);
                    }

                    JobMetrics.JobsProcessed.Add(1);
                    _logger.LogInformation("Successfully completed Job {JobId}", job.Id);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    stopwatch.Stop();
                    _logger.LogError(ex, "Error occurred while executing Job {JobId}", job.Id);
                    
                    var nowTime = DateTimeOffset.UtcNow;
                    if (job.HasRemainingAttempts)
                    {
                        // Exponential backoff based on Attempts
                        var backoffDelay = TimeSpan.FromSeconds(Math.Pow(2, job.Attempts));
                        job.MarkAsRetrying(ex.ToString(), nowTime.Add(backoffDelay));
                        _logger.LogInformation("Job {JobId} failed and will retry at {NextRetryAt}", job.Id, job.NextRetryAt);
                    }
                    else
                    {
                        job.MarkAsFailed(ex.ToString(), nowTime);
                        _logger.LogWarning("Job {JobId} has permanently failed after {Attempts} attempts", job.Id, job.Attempts);
                    }
                    
                    await _repository.UpdateJobAsync(job, stoppingToken);
                }
                finally
                {
                    // Clean up active job tracking and cancel heartbeats immediately upon completion or failure
                    _tracker.Remove(job.Id);
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogError(ex, "Critical error occurred in worker loop.");
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
            }
        }
    }

    /// <summary>
    /// Simulates the actual business logic execution.
    /// In reality, you would use a Factory to resolve an IJobHandler based on job.Type.
    /// </summary>
    private async Task ProcessJobSimulatedAsync(Job job, CancellationToken token)
    {
        // Simulate a job that takes anywhere from 500ms to 1000ms to complete
        // (This aligns with realistic times for sending emails, rendering PDFs, etc.)
        var randomDelay = new Random().Next(500, 1000);
        await Task.Delay(randomDelay, token);
    }
}
