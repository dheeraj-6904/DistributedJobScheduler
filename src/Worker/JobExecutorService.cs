using System.Diagnostics;
using DistributedJobScheduler.Core.Domain;
using DistributedJobScheduler.Core.Execution;
using DistributedJobScheduler.Core.Infrastructure;
using Microsoft.Extensions.Options;

namespace DistributedJobScheduler.Worker;

/// <summary>
/// The core Worker loop. Pops from Redis using bounded concurrency (SemaphoreSlim),
/// marks as Running in Postgres, executes, and marks as Completed/Retrying/Failed.
/// </summary>
public sealed class JobExecutorService : BackgroundService
{
    private readonly IQueueProvider _queue;
    private readonly IJobRepository _repository;
    private readonly ActiveJobTracker _tracker;
    private readonly ILogger<JobExecutorService> _logger;
    private readonly IJobHandlerFactory _handlerFactory;
    private readonly WorkerOptions _options;
    private readonly SemaphoreSlim _semaphore;
    private readonly string _workerId;

    public JobExecutorService(
        IQueueProvider queue,
        IJobRepository repository,
        ActiveJobTracker tracker,
        IJobHandlerFactory handlerFactory,
        ILogger<JobExecutorService> logger,
        IOptions<WorkerOptions>? options = null)
    {
        _queue = queue;
        _repository = repository;
        _tracker = tracker;
        _handlerFactory = handlerFactory;
        _logger = logger;
        _options = options?.Value ?? new WorkerOptions();
        _options.Validate();

        _semaphore = new SemaphoreSlim(_options.Concurrency, _options.Concurrency);

        // Generate a unique ID for this instance so we can track which machine took the job
        _workerId = Guid.NewGuid().ToString("N");
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "Job Executor started on Worker ID: {WorkerId} with Concurrency: {Concurrency}",
            _workerId,
            _options.Concurrency);

        // Define which queues this specific worker listens to (could easily be driven by config)
        var topicsToListenTo = new[] { JobTopic.Default, JobTopic.CpuIntensive };
        var inFlightTasks = new List<Task>();

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                inFlightTasks.RemoveAll(t => t.IsCompleted);

                try
                {
                    // 1. Wait for a free concurrency slot BEFORE asking the queue for work (backpressure by design)
                    await _semaphore.WaitAsync(stoppingToken);

                    Guid? poppedJobId;
                    try
                    {
                        // 2. Wait for a job from Redis (Blocking Pop). Will wait up to 5 seconds.
                        poppedJobId = await _queue.PopAsync(topicsToListenTo, TimeSpan.FromSeconds(5), stoppingToken);
                    }
                    catch
                    {
                        _semaphore.Release();
                        throw;
                    }

                    if (poppedJobId == null)
                    {
                        _semaphore.Release();
                        continue; // Timeout reached, release slot and loop again
                    }

                    // 3. Dispatch job execution to the ThreadPool; slot is released in ProcessAndReleaseAsync finally block
                    inFlightTasks.Add(ProcessAndReleaseAsync(poppedJobId.Value, stoppingToken));
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    _logger.LogError(ex, "Critical error occurred in worker loop on Worker {WorkerId}.", _workerId);
                    await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Expected when stoppingToken is triggered during WaitAsync, PopAsync, or error backoff delay.
        }
        finally
        {
            await WaitForInFlightJobsAsync(inFlightTasks);
        }
    }

    private async Task ProcessAndReleaseAsync(Guid jobId, CancellationToken stoppingToken)
    {
        try
        {
            await Task.Yield();
            await ProcessJobAsync(jobId, stoppingToken);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            _logger.LogInformation("Job {JobId} execution was cancelled due to shutdown on Worker {WorkerId}.", jobId, _workerId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Unhandled error while processing Job {JobId} on Worker {WorkerId}.", jobId, _workerId);
        }
        finally
        {
            _semaphore.Release();
        }
    }

    /// <summary>
    /// Queue-agnostic job processing pipeline: fetches job from Postgres, marks Running,
    /// tracks active lease, executes handler, records metrics, and marks Completed/Retrying/Failed.
    /// </summary>
    public async Task ProcessJobAsync(Guid jobId, CancellationToken cancellationToken)
    {
        var job = await _repository.GetJobAsync(jobId, cancellationToken);

        if (job == null || job.Status != JobStatus.Queued)
        {
            _logger.LogWarning("Job {JobId} not found or was not in Queued state (State: {State}).", jobId, job?.Status);
            return;
        }

        // Mark as Running in Postgres and track it so HeartbeatService renews its lease
        var now = DateTimeOffset.UtcNow;
        var initialLease = now.AddSeconds(30);

        job.MarkAsRunning(_workerId, initialLease, now);
        await _repository.UpdateJobAsync(job, cancellationToken);

        _tracker.Add(job.Id);
        JobMetrics.IncrementInFlight();

        try
        {
            // Record the time spent in queue/scheduler
            var queueTimeMs = (DateTimeOffset.UtcNow - job.ScheduledAt).TotalMilliseconds;
            if (queueTimeMs > 0)
            {
                JobMetrics.JobQueueTime.Record(queueTimeMs);
            }

            _logger.LogInformation("Started executing Job {JobId} (Type: {Type}) on Worker {WorkerId}", job.Id, job.Type, _workerId);
            var stopwatch = Stopwatch.StartNew();

            try
            {
                var handler = _handlerFactory.GetHandler(job.Type);
                await handler.HandleAsync(job, cancellationToken);

                stopwatch.Stop();
                JobMetrics.JobExecutionTime.Record(stopwatch.ElapsedMilliseconds);

                // Execution finished successfully. Mark as Completed.
                var completedAt = DateTimeOffset.UtcNow;
                job.MarkAsCompleted("{\"status\":\"success\"}", completedAt);
                var updateToken = cancellationToken.IsCancellationRequested ? CancellationToken.None : cancellationToken;
                await _repository.UpdateJobAsync(job, updateToken);

                var lifecycleTimeMs = (completedAt - job.CreatedAt).TotalMilliseconds;
                if (lifecycleTimeMs > 0)
                {
                    JobMetrics.JobTotalLifecycleTime.Record(lifecycleTimeMs);
                }

                JobMetrics.JobsProcessed.Add(1);
                _logger.LogInformation("Successfully completed Job {JobId} on Worker {WorkerId}", job.Id, _workerId);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                stopwatch.Stop();
                _logger.LogError(ex, "Error occurred while executing Job {JobId} on Worker {WorkerId}", job.Id, _workerId);

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

                var updateToken = cancellationToken.IsCancellationRequested ? CancellationToken.None : cancellationToken;
                await _repository.UpdateJobAsync(job, updateToken);
            }
        }
        finally
        {
            // Clean up active job tracking and cancel heartbeats immediately upon completion or failure
            _tracker.Remove(job.Id);
            JobMetrics.DecrementInFlight();
        }
    }

    private async Task WaitForInFlightJobsAsync(List<Task> inFlightTasks)
    {
        inFlightTasks.RemoveAll(t => t.IsCompleted);
        if (inFlightTasks.Count == 0)
        {
            return;
        }

        _logger.LogInformation(
            "Worker {WorkerId} shutting down. Waiting up to {TimeoutSeconds}s for {InFlightCount} in-flight job(s) to finish.",
            _workerId,
            _options.ShutdownTimeout.TotalSeconds,
            inFlightTasks.Count);

        try
        {
            await Task.WhenAll(inFlightTasks).WaitAsync(_options.ShutdownTimeout, CancellationToken.None);
        }
        catch (TimeoutException)
        {
            _logger.LogWarning(
                "Worker {WorkerId} timed out after {TimeoutSeconds}s waiting for in-flight jobs to complete.",
                _workerId,
                _options.ShutdownTimeout.TotalSeconds);
        }
    }

    public override void Dispose()
    {
        _semaphore.Dispose();
        base.Dispose();
    }
}
