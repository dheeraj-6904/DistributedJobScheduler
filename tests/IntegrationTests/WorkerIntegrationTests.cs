using DistributedJobScheduler.Core.Domain;
using DistributedJobScheduler.Core.Infrastructure;
using DistributedJobScheduler.Worker;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
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
        var repo = new PostgresJobRepository(NpgsqlDataSource.Create(_fixture.PostgresConnectionString));
        var queue = new RedisQueueProvider(_fixture.RedisConnection);
        var tracker = new ActiveJobTracker();
        var executor = new JobExecutorService(queue, repo, tracker, NullLogger<JobExecutorService>.Instance);
        
        var jobId = Guid.NewGuid();
        var job = new Job
        {
            Id = jobId,
            Type = "TestType",
            Topic = JobTopic.Default,
            Payload = "{}",
            Priority = JobPriority.From(5),
            MaxAttempts = 3,
            ScheduledAt = DateTimeOffset.UtcNow,
            CreatedAt = DateTimeOffset.UtcNow
        };
        typeof(Job).GetProperty("Status")?.SetValue(job, JobStatus.Queued, null);
        await repo.CreateJobAsync(job, CancellationToken.None);
        
        await queue.PushAsync(job, CancellationToken.None);

        var cts = new CancellationTokenSource();
        var executeTask = executor.StartAsync(cts.Token);
        
        await Task.Delay(2500); 
        cts.Cancel();
        try { await executeTask; } catch (OperationCanceledException) { }

        var updatedJob = await repo.GetJobAsync(jobId, CancellationToken.None);
        Assert.NotNull(updatedJob);
        Assert.Equal(JobStatus.Completed, updatedJob.Status);
    }
}
