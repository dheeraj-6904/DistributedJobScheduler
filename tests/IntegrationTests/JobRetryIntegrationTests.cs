using DistributedJobScheduler.Core.Domain;
using DistributedJobScheduler.Core.Infrastructure;
using DistributedJobScheduler.IntegrationTests.TestHandlers;
using DistributedJobScheduler.Scheduler;
using DistributedJobScheduler.Worker;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using StackExchange.Redis;
using Xunit;

namespace DistributedJobScheduler.IntegrationTests;

/// <summary>
/// Verifies the full failure path: Worker handler throws -> Retrying with backoff ->
/// Dispatcher re-queues once NextRetryAt passes -> Worker retries -> Completed or Failed.
/// </summary>
[Collection("Integration Tests")]
public class JobRetryIntegrationTests : IAsyncLifetime
{
    private const int MaxAttempts = 3;

    private readonly IntegrationTestFixture _fixture;

    public JobRetryIntegrationTests(IntegrationTestFixture fixture)
    {
        _fixture = fixture;
    }

    public async Task InitializeAsync()
    {
        await _fixture.ClearDatabaseAsync();
        await _fixture.ClearRedisAsync();
    }

    public Task DisposeAsync() => Task.CompletedTask;

    [Fact]
    public async Task Job_FailsOnce_IsRetried_AndCompletes()
    {
        var handler = new FailingJobHandler(failuresBeforeSuccess: 1);
        var repo = CreateRepository();
        var job = await CreatePendingJobAsync(repo);

        var finalJob = await RunPipelineUntilTerminalAsync(repo, handler, job.Id, TimeSpan.FromSeconds(20));

        Assert.Equal(JobStatus.Completed, finalJob.Status);
        Assert.Equal(2, finalJob.Attempts);
        Assert.Equal(2, handler.Invocations);
        Assert.NotNull(finalJob.CompletedAt);
        Assert.Null(finalJob.WorkerId);
        Assert.Null(finalJob.LeaseUntil);
    }

    [Fact]
    public async Task Job_FailsEveryTime_IsMarkedFailed_AfterMaxAttempts()
    {
        var handler = new FailingJobHandler(failuresBeforeSuccess: int.MaxValue);
        var repo = CreateRepository();
        var job = await CreatePendingJobAsync(repo);

        // Backoff is 2s + 4s, plus dispatcher polling, so allow generous headroom.
        var finalJob = await RunPipelineUntilTerminalAsync(repo, handler, job.Id, TimeSpan.FromSeconds(30));

        Assert.Equal(JobStatus.Failed, finalJob.Status);
        Assert.Equal(MaxAttempts, finalJob.Attempts);
        Assert.Equal(MaxAttempts, handler.Invocations);
        Assert.Contains(FailingJobHandler.FailureMessage, finalJob.Error);
        Assert.NotNull(finalJob.CompletedAt);
        Assert.Null(finalJob.WorkerId);
        Assert.Null(finalJob.LeaseUntil);
    }

    [Fact]
    public async Task Job_AfterFirstFailure_IsRetryingWithBackoff()
    {
        var handler = new FailingJobHandler(failuresBeforeSuccess: int.MaxValue);
        var repo = CreateRepository();
        var job = await CreateQueuedJobAsync(repo);
        await new RedisQueueProvider(_fixture.RedisConnection).PushAsync(job, CancellationToken.None);

        // Run only the worker so the job can't be re-dispatched and we observe the intermediate state.
        var tracker = new ActiveJobTracker();
        var beforeRun = DateTimeOffset.UtcNow;
        var retrying = await RunWorkerUntilAsync(repo, handler, tracker, job.Id,
            j => j.Status == JobStatus.Retrying, TimeSpan.FromSeconds(10));

        Assert.Equal(JobStatus.Retrying, retrying.Status);
        Assert.Equal(1, retrying.Attempts);
        Assert.NotNull(retrying.NextRetryAt);
        // Backoff after the 1st attempt is 2^1 = 2 seconds.
        Assert.True(retrying.NextRetryAt >= beforeRun.AddSeconds(2),
            $"Expected NextRetryAt >= {beforeRun.AddSeconds(2):O} but was {retrying.NextRetryAt:O}.");
        Assert.Contains(FailingJobHandler.FailureMessage, retrying.Error);
        Assert.Null(retrying.WorkerId);
        Assert.DoesNotContain(job.Id, tracker.GetActiveJobIds());
    }

    [Fact]
    public async Task DequeueReadyJobs_ReturnsRetryingJobOnlyAfterBackoffElapses()
    {
        var repo = CreateRepository();
        var dueJob = await CreateRetryingJobAsync(repo, DateTimeOffset.UtcNow.AddMinutes(-1));
        var notDueJob = await CreateRetryingJobAsync(repo, DateTimeOffset.UtcNow.AddMinutes(10));

        var dequeued = await repo.DequeueReadyJobsAsync(batchSize: 10, CancellationToken.None);

        var dequeuedIds = dequeued.Select(j => j.Id).ToList();
        Assert.Contains(dueJob.Id, dequeuedIds);
        Assert.DoesNotContain(notDueJob.Id, dequeuedIds);

        var notDueAfter = await repo.GetJobAsync(notDueJob.Id, CancellationToken.None);
        Assert.Equal(JobStatus.Retrying, notDueAfter!.Status);
    }

    private PostgresJobRepository CreateRepository() =>
        new(NpgsqlDataSource.Create(_fixture.PostgresConnectionString));

    private async Task<Job> RunPipelineUntilTerminalAsync(
        IJobRepository repo, FailingJobHandler handler, Guid jobId, TimeSpan timeout)
    {
        var dispatcher = new DispatcherService(repo, new RedisQueueProvider(_fixture.RedisConnection),
            NullLogger<DispatcherService>.Instance);

        using var cts = new CancellationTokenSource();
        var dispatcherTask = dispatcher.StartAsync(cts.Token);
        try
        {
            return await RunWorkerUntilAsync(repo, handler, new ActiveJobTracker(), jobId,
                j => j.IsTerminal, timeout);
        }
        finally
        {
            await cts.CancelAsync();
            await dispatcher.StopAsync(CancellationToken.None);
            await dispatcherTask;
        }
    }

    private async Task<Job> RunWorkerUntilAsync(
        IJobRepository repo, FailingJobHandler handler, ActiveJobTracker tracker, Guid jobId,
        Func<Job, bool> condition, TimeSpan timeout)
    {
        // Worker gets its own Redis connection: its blocking pop would otherwise stall the Dispatcher's commands.
        await using var workerRedis = await ConnectionMultiplexer.ConnectAsync(_fixture.RedisContainer.GetConnectionString());
        var executor = new JobExecutorService(new RedisQueueProvider(workerRedis), repo, tracker,
            new TestJobHandlerFactory(handler), NullLogger<JobExecutorService>.Instance);

        using var cts = new CancellationTokenSource();
        await executor.StartAsync(cts.Token);
        try
        {
            return await WaitForJobAsync(repo, jobId, condition, timeout);
        }
        finally
        {
            await cts.CancelAsync();
            await executor.StopAsync(CancellationToken.None);
        }
    }

    private static async Task<Job> WaitForJobAsync(
        IJobRepository repo, Guid jobId, Func<Job, bool> condition, TimeSpan timeout)
    {
        var deadline = DateTimeOffset.UtcNow + timeout;
        Job? job = null;

        while (DateTimeOffset.UtcNow < deadline)
        {
            job = await repo.GetJobAsync(jobId, CancellationToken.None);
            if (job != null && condition(job)) return job;
            await Task.Delay(250);
        }

        throw new TimeoutException(
            $"Job {jobId} did not reach the expected state within {timeout}. Last status: {job?.Status}, attempts: {job?.Attempts}.");
    }

    private static Task<Job> CreatePendingJobAsync(IJobRepository repo) =>
        CreateJobAsync(repo, JobStatus.Pending);

    private static Task<Job> CreateQueuedJobAsync(IJobRepository repo) =>
        CreateJobAsync(repo, JobStatus.Queued);

    private static async Task<Job> CreateRetryingJobAsync(IJobRepository repo, DateTimeOffset nextRetryAt)
    {
        var job = await CreateJobAsync(repo, JobStatus.Pending);
        job.MarkAsRunning("test-worker", DateTimeOffset.UtcNow.AddSeconds(30), DateTimeOffset.UtcNow);
        job.MarkAsRetrying("previous failure", nextRetryAt);
        await repo.UpdateJobAsync(job, CancellationToken.None);
        return job;
    }

    private static async Task<Job> CreateJobAsync(IJobRepository repo, JobStatus status)
    {
        var job = new Job
        {
            Id = Guid.NewGuid(),
            Type = "RetryTestJob",
            Topic = JobTopic.Default,
            Payload = "{}",
            Priority = JobPriority.From(5),
            MaxAttempts = MaxAttempts,
            // Scheduled in the past to avoid clock skew with the Postgres container.
            ScheduledAt = DateTimeOffset.UtcNow.AddMinutes(-1),
            CreatedAt = DateTimeOffset.UtcNow
        };
        // Status has a private setter; tests seed the initial state the same way the other integration tests do.
        typeof(Job).GetProperty(nameof(Job.Status))!.SetValue(job, status);

        await repo.CreateJobAsync(job, CancellationToken.None);
        return job;
    }
}
