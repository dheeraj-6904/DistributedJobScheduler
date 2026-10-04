using DistributedJobScheduler.Core.Domain;
using DistributedJobScheduler.Core.Infrastructure;
using DistributedJobScheduler.Scheduler;
using Microsoft.Extensions.Logging.Abstractions;
using StackExchange.Redis;
using Npgsql;
using Xunit;

namespace DistributedJobScheduler.IntegrationTests;

[Collection("Integration Tests")]
public class SchedulerIntegrationTests : IAsyncLifetime
{
    private readonly IntegrationTestFixture _fixture;

    public SchedulerIntegrationTests(IntegrationTestFixture fixture)
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
    public async Task DispatcherService_PushesPendingJobToRedis()
    {
        var repo = new PostgresJobRepository(NpgsqlDataSource.Create(_fixture.PostgresConnectionString));
        var queue = new RedisQueueProvider(_fixture.RedisConnection);
        var dispatcher = new DispatcherService(repo, queue, NullLogger<DispatcherService>.Instance);
        
        var job = new Job
        {
            Id = Guid.NewGuid(),
            Type = "TestType",
            Topic = JobTopic.Default,
            Payload = "{}",
            Priority = JobPriority.From(5),
            MaxAttempts = 3,
            ScheduledAt = DateTimeOffset.UtcNow,
            CreatedAt = DateTimeOffset.UtcNow
        };
        
        // Force the Job status to Pending via reflection
        typeof(Job).GetProperty("Status")?.SetValue(job, JobStatus.Pending, null);
        
        await repo.CreateJobAsync(job, CancellationToken.None);

        var cts = new CancellationTokenSource();
        var executeTask = dispatcher.StartAsync(cts.Token);
        
        await Task.Delay(2000);
        cts.Cancel();
        try { await executeTask; } catch (OperationCanceledException) { }
        
        var db = _fixture.RedisConnection.GetDatabase();
        var elements = await db.SortedSetRangeByRankAsync("jobs:ready:default");
        
        Assert.Single(elements);
        Assert.Equal(job.Id.ToString(), elements[0].ToString());
    }
}
