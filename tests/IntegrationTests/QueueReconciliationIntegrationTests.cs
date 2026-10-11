using DistributedJobScheduler.Core.Domain;
using DistributedJobScheduler.Core.Infrastructure;
using Npgsql;
using Xunit;

namespace DistributedJobScheduler.IntegrationTests;

[Collection("Integration Tests")]
public class QueueReconciliationIntegrationTests : IAsyncLifetime
{
    private readonly IntegrationTestFixture _fixture;

    public QueueReconciliationIntegrationTests(IntegrationTestFixture fixture)
    {
        _fixture = fixture;
    }

    public async Task InitializeAsync()
    {
        await _fixture.ClearDatabaseAsync();
        await _fixture.ClearRedisAsync();
    }

    public Task DisposeAsync() => Task.CompletedTask;

    private PostgresJobRepository CreateRepository() =>
        new(NpgsqlDataSource.Create(_fixture.PostgresConnectionString));

    private static Job CreateJob(JobStatus status, DateTimeOffset? queuedAt = null)
    {
        var job = new Job
        {
            Id = Guid.NewGuid(),
            Type = "TestType",
            Topic = JobTopic.Default,
            Payload = "{}",
            Priority = JobPriority.From(5),
            MaxAttempts = 3,
            ScheduledAt = DateTimeOffset.UtcNow.AddMinutes(-10),
            CreatedAt = DateTimeOffset.UtcNow.AddMinutes(-10)
        };

        if (status == JobStatus.Queued)
        {
            job.MarkAsQueued(queuedAt ?? DateTimeOffset.UtcNow);
        }
        else
        {
            // Status has a private setter; existing integration tests use the same approach
            typeof(Job).GetProperty(nameof(Job.Status))?.SetValue(job, status, null);
        }

        return job;
    }

    [Fact]
    public async Task RollbackToPendingAsync_UpdatesStatusAndClearsQueuedAt()
    {
        var repo = CreateRepository();
        var job = CreateJob(JobStatus.Queued, DateTimeOffset.UtcNow);
        await repo.CreateJobAsync(job, CancellationToken.None);

        await repo.RollbackToPendingAsync(new[] { job.Id }, CancellationToken.None);

        var stored = await repo.GetJobAsync(job.Id, CancellationToken.None);
        Assert.NotNull(stored);
        Assert.Equal(JobStatus.Pending, stored!.Status);
        Assert.Null(stored.QueuedAt);
    }

    [Fact]
    public async Task RollbackToPendingAsync_DoesNotTouchJobsThatAreNoLongerQueued()
    {
        var repo = CreateRepository();
        var runningJob = CreateJob(JobStatus.Running);
        await repo.CreateJobAsync(runningJob, CancellationToken.None);

        await repo.RollbackToPendingAsync(new[] { runningJob.Id }, CancellationToken.None);

        // Guards against a race where a worker already claimed the job
        var stored = await repo.GetJobAsync(runningJob.Id, CancellationToken.None);
        Assert.Equal(JobStatus.Running, stored!.Status);
    }

    [Fact]
    public async Task GetStaleQueuedJobsAsync_ReturnsOnlyStaleQueuedJobs()
    {
        var repo = CreateRepository();
        var staleQueued = CreateJob(JobStatus.Queued, DateTimeOffset.UtcNow.AddMinutes(-5));
        var freshQueued = CreateJob(JobStatus.Queued, DateTimeOffset.UtcNow);
        var pending = CreateJob(JobStatus.Pending);

        await repo.CreateJobAsync(staleQueued, CancellationToken.None);
        await repo.CreateJobAsync(freshQueued, CancellationToken.None);
        await repo.CreateJobAsync(pending, CancellationToken.None);

        var result = await repo.GetStaleQueuedJobsAsync(DateTimeOffset.UtcNow.AddMinutes(-2), batchSize: 50, CancellationToken.None);

        var single = Assert.Single(result);
        Assert.Equal(staleQueued.Id, single.Id);
    }

    [Fact]
    public async Task UpdateQueuedAtAsync_RefreshesTimestamp()
    {
        var repo = CreateRepository();
        var job = CreateJob(JobStatus.Queued, DateTimeOffset.UtcNow.AddMinutes(-5));
        await repo.CreateJobAsync(job, CancellationToken.None);

        var refreshedAt = DateTimeOffset.UtcNow;
        await repo.UpdateQueuedAtAsync(new[] { job.Id }, refreshedAt, CancellationToken.None);

        // Once refreshed, the job should no longer be considered stale
        var stale = await repo.GetStaleQueuedJobsAsync(DateTimeOffset.UtcNow.AddMinutes(-2), batchSize: 50, CancellationToken.None);
        Assert.Empty(stale);
    }

    [Fact]
    public async Task ContainsAsync_ReturnsTrueWhenInSet_AndFalseWhenNot()
    {
        var queue = new RedisQueueProvider(_fixture.RedisConnection);
        var pushedJob = CreateJob(JobStatus.Queued);
        var missingJob = CreateJob(JobStatus.Queued);

        await queue.PushAsync(pushedJob, CancellationToken.None);

        Assert.True(await queue.ContainsAsync(pushedJob.Topic, pushedJob.Id, CancellationToken.None));
        Assert.False(await queue.ContainsAsync(missingJob.Topic, missingJob.Id, CancellationToken.None));
    }
}
