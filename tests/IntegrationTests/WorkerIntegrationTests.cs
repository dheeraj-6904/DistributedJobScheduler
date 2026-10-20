using System.Diagnostics;
using DistributedJobScheduler.Core.Domain;
using DistributedJobScheduler.Core.Execution;
using DistributedJobScheduler.Core.Infrastructure;
using DistributedJobScheduler.IntegrationTests.TestHandlers;
using DistributedJobScheduler.Worker;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Npgsql;
using StackExchange.Redis;
using Xunit;

namespace DistributedJobScheduler.IntegrationTests;

[Collection("Integration Tests")]
public class WorkerIntegrationTests : IAsyncLifetime
{
    private readonly IntegrationTestFixture _fixture;

    public WorkerIntegrationTests(IntegrationTestFixture fixture)
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
    public async Task JobExecutorService_PopsJobAndCompletesIt()
    {
        await using var dataSource = NpgsqlDataSource.Create(_fixture.PostgresConnectionString);
        var repo = new PostgresJobRepository(dataSource);
        var queue = new RedisQueueProvider(_fixture.RedisConnection);
        var tracker = new ActiveJobTracker();
        using var executor = new JobExecutorService(
            queue,
            repo,
            tracker,
            new TestJobHandlerFactory(new SucceedingJobHandler()),
            NullLogger<JobExecutorService>.Instance);

        var job = await CreateAndPushQueuedJobAsync(repo, queue);

        using var cts = new CancellationTokenSource();
        await executor.StartAsync(cts.Token);

        try
        {
            var completedJob = await WaitForAllJobsAsync(repo, new[] { job.Id }, j => j.Status == JobStatus.Completed, TimeSpan.FromSeconds(10));
            Assert.Equal(JobStatus.Completed, completedJob[0].Status);
        }
        finally
        {
            await cts.CancelAsync();
            await executor.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task JobExecutorService_WithConcurrencyN_CompletesNSlowJobsInParallel()
    {
        const int concurrency = 4;
        var perJobDuration = TimeSpan.FromMilliseconds(800);

        await using var dataSource = NpgsqlDataSource.Create(_fixture.PostgresConnectionString);
        var repo = new PostgresJobRepository(dataSource);
        await using var workerRedis = await ConnectionMultiplexer.ConnectAsync(_fixture.RedisContainer.GetConnectionString());
        var queue = new RedisQueueProvider(workerRedis);
        var tracker = new ActiveJobTracker();

        var jobIds = new List<Guid>(concurrency);
        for (int i = 0; i < concurrency; i++)
        {
            var job = await CreateAndPushQueuedJobAsync(repo, queue);
            jobIds.Add(job.Id);
        }

        var handler = new DelayedTrackingJobHandler(perJobDuration);
        var options = Options.Create(new WorkerOptions { Concurrency = concurrency });
        using var executor = new JobExecutorService(
            queue,
            repo,
            tracker,
            new TestJobHandlerFactory(handler),
            NullLogger<JobExecutorService>.Instance,
            options);

        var stopwatch = Stopwatch.StartNew();
        using var cts = new CancellationTokenSource();
        await executor.StartAsync(cts.Token);

        try
        {
            var completedJobs = await WaitForAllJobsAsync(
                repo,
                jobIds,
                j => j.Status == JobStatus.Completed,
                TimeSpan.FromSeconds(10));
            stopwatch.Stop();

            Assert.All(completedJobs, j => Assert.Equal(JobStatus.Completed, j.Status));
            Assert.Equal(concurrency, handler.PeakConcurrency);
            // Sequential execution would take at least 4 * 800ms = 3200ms.
            // Parallel execution with Concurrency = 4 should finish well below the sequential threshold.
            var sequentialDuration = perJobDuration * concurrency;
            Assert.True(
                stopwatch.Elapsed < sequentialDuration * 0.75,
                $"Expected {concurrency} parallel jobs ({perJobDuration.TotalMilliseconds}ms each) to complete in well under {sequentialDuration.TotalMilliseconds}ms, but took {stopwatch.Elapsed.TotalMilliseconds:F0}ms.");
        }
        finally
        {
            await cts.CancelAsync();
            await executor.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task HeartbeatService_RenewsLeasesForMultipleConcurrentInFlightJobs()
    {
        const int concurrency = 3;

        await using var dataSource = NpgsqlDataSource.Create(_fixture.PostgresConnectionString);
        var repo = new PostgresJobRepository(dataSource);
        await using var workerRedis = await ConnectionMultiplexer.ConnectAsync(_fixture.RedisContainer.GetConnectionString());
        var queue = new RedisQueueProvider(workerRedis);
        var tracker = new ActiveJobTracker();

        var jobIds = new List<Guid>(concurrency);
        for (int i = 0; i < concurrency; i++)
        {
            var job = await CreateAndPushQueuedJobAsync(repo, queue);
            jobIds.Add(job.Id);
        }

        var allStartedTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseHandlersTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var startedCount = 0;

        var blockingHandler = new DelegateJobHandler(async (_, ct) =>
        {
            if (Interlocked.Increment(ref startedCount) == concurrency)
            {
                allStartedTcs.TrySetResult();
            }

            await releaseHandlersTcs.Task.WaitAsync(ct);
        });

        var options = Options.Create(new WorkerOptions { Concurrency = concurrency });
        using var executor = new JobExecutorService(
            queue,
            repo,
            tracker,
            new TestJobHandlerFactory(blockingHandler),
            NullLogger<JobExecutorService>.Instance,
            options);
        using var heartbeat = new HeartbeatService(
            tracker,
            repo,
            NullLogger<HeartbeatService>.Instance,
            TimeSpan.FromMilliseconds(200));

        using var cts = new CancellationTokenSource();
        await executor.StartAsync(cts.Token);

        try
        {
            // Wait until all 3 jobs are concurrently in-flight inside the handler
            await allStartedTcs.Task.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.Equal(concurrency, tracker.Count);

            var initialJobs = await WaitForAllJobsAsync(
                repo,
                jobIds,
                j => j.Status == JobStatus.Running && j.LeaseUntil.HasValue,
                TimeSpan.FromSeconds(5));
            var initialLeases = initialJobs.ToDictionary(j => j.Id, j => j.LeaseUntil!.Value);

            // Wait slightly so the renewed LeaseUntil timestamp is strictly greater than the initial lease
            await Task.Delay(150);
            await heartbeat.StartAsync(cts.Token);

            var renewedJobs = await WaitForAllJobsAsync(
                repo,
                jobIds,
                j => j.LeaseUntil.HasValue && j.LeaseUntil.Value > initialLeases[j.Id],
                TimeSpan.FromSeconds(5));

            foreach (var renewed in renewedJobs)
            {
                Assert.Equal(JobStatus.Running, renewed.Status);
                Assert.True(
                    renewed.LeaseUntil > initialLeases[renewed.Id],
                    $"Expected LeaseUntil for Job {renewed.Id} to be extended beyond {initialLeases[renewed.Id]:O}, but was {renewed.LeaseUntil:O}.");
            }

            // Release all handlers and confirm clean completion
            releaseHandlersTcs.TrySetResult();
            var completedJobs = await WaitForAllJobsAsync(
                repo,
                jobIds,
                j => j.Status == JobStatus.Completed,
                TimeSpan.FromSeconds(10));
            Assert.All(completedJobs, j => Assert.Equal(JobStatus.Completed, j.Status));
            Assert.Equal(0, tracker.Count);
        }
        finally
        {
            releaseHandlersTcs.TrySetResult();
            await cts.CancelAsync();
            await heartbeat.StopAsync(CancellationToken.None);
            await executor.StopAsync(CancellationToken.None);
        }
    }

    private static async Task<Job> CreateAndPushQueuedJobAsync(IJobRepository repo, IQueueProvider queue)
    {
        var job = new Job
        {
            Id = Guid.NewGuid(),
            Type = "TestType",
            Topic = JobTopic.Default,
            Payload = "{}",
            Priority = JobPriority.From(5),
            MaxAttempts = 3,
            ScheduledAt = DateTimeOffset.UtcNow.AddSeconds(-5),
            CreatedAt = DateTimeOffset.UtcNow.AddSeconds(-5)
        };
        typeof(Job).GetProperty(nameof(Job.Status))!.SetValue(job, JobStatus.Queued);
        await repo.CreateJobAsync(job, CancellationToken.None);
        await queue.PushAsync(job, CancellationToken.None);
        return job;
    }

    private static async Task<IReadOnlyList<Job>> WaitForAllJobsAsync(
        IJobRepository repo,
        IReadOnlyCollection<Guid> jobIds,
        Func<Job, bool> predicate,
        TimeSpan timeout)
    {
        var deadline = DateTimeOffset.UtcNow + timeout;
        while (DateTimeOffset.UtcNow < deadline)
        {
            var results = new List<Job>(jobIds.Count);
            var allMatched = true;

            foreach (var id in jobIds)
            {
                var job = await repo.GetJobAsync(id, CancellationToken.None);
                if (job == null || !predicate(job))
                {
                    allMatched = false;
                    break;
                }
                results.Add(job);
            }

            if (allMatched)
            {
                return results;
            }

            await Task.Delay(50);
        }

        throw new TimeoutException($"Not all jobs satisfied the condition within {timeout}.");
    }

    private sealed class DelayedTrackingJobHandler : IJobHandler
    {
        private readonly TimeSpan _delay;
        private int _current;
        private int _peak;

        public DelayedTrackingJobHandler(TimeSpan delay)
        {
            _delay = delay;
        }

        public int PeakConcurrency => Volatile.Read(ref _peak);

        public async Task HandleAsync(Job job, CancellationToken cancellationToken)
        {
            var active = Interlocked.Increment(ref _current);
            int prev;
            do
            {
                prev = Volatile.Read(ref _peak);
                if (active <= prev) break;
            }
            while (Interlocked.CompareExchange(ref _peak, active, prev) != prev);

            try
            {
                await Task.Delay(_delay, cancellationToken);
            }
            finally
            {
                Interlocked.Decrement(ref _current);
            }
        }
    }

    private sealed class DelegateJobHandler : IJobHandler
    {
        private readonly Func<Job, CancellationToken, Task> _callback;

        public DelegateJobHandler(Func<Job, CancellationToken, Task> callback)
        {
            _callback = callback;
        }

        public Task HandleAsync(Job job, CancellationToken cancellationToken) => _callback(job, cancellationToken);
    }
}
