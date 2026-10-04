using System.Net.Http.Json;
using DistributedJobScheduler.Core.Domain;
using DistributedJobScheduler.Core.Infrastructure;
using DistributedJobScheduler.Scheduler;
using DistributedJobScheduler.Worker;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Xunit;

namespace DistributedJobScheduler.IntegrationTests;

[Collection("Integration Tests")]
public class EndToEndTests : IAsyncLifetime
{
    private readonly IntegrationTestFixture _fixture;
    private readonly WebApplicationFactory<Program> _factory;
    private readonly HttpClient _client;

    public EndToEndTests(IntegrationTestFixture fixture)
    {
        _fixture = fixture;
        
        _factory = new WebApplicationFactory<Program>().WithWebHostBuilder(builder =>
        {
            builder.ConfigureServices(services =>
            {
                var descriptor = services.SingleOrDefault(d => d.ServiceType == typeof(NpgsqlDataSource));
                if (descriptor != null) services.Remove(descriptor);
                
                services.AddSingleton(NpgsqlDataSource.Create(_fixture.PostgresConnectionString));
            });
        });

        _client = _factory.CreateClient();
    }

    public async Task InitializeAsync()
    {
        await _fixture.ClearDatabaseAsync();
        await _fixture.ClearRedisAsync();
    }

    public Task DisposeAsync() => Task.CompletedTask;

    [Fact]
    public async Task FullFlow_ApiToSchedulerToWorker()
    {
        // 1. Setup shared infrastructure
        var repo = new PostgresJobRepository(NpgsqlDataSource.Create(_fixture.PostgresConnectionString));
        var queue = new RedisQueueProvider(_fixture.RedisConnection);
        
        // 2. Start Scheduler (Dispatcher)
        var dispatcher = new DispatcherService(repo, queue, NullLogger<DispatcherService>.Instance);
        var cts = new CancellationTokenSource();
        var dispatcherTask = dispatcher.StartAsync(cts.Token);
        
        // 3. Start Worker (Executor) on its own Redis connection: its blocking pop would otherwise stall the Scheduler's commands
        await using var workerRedis = await StackExchange.Redis.ConnectionMultiplexer.ConnectAsync(_fixture.RedisContainer.GetConnectionString());
        var workerQueue = new RedisQueueProvider(workerRedis);
        var tracker = new ActiveJobTracker();
        var executor = new JobExecutorService(workerQueue, repo, tracker, NullLogger<JobExecutorService>.Instance);
        var executorTask = executor.StartAsync(cts.Token);

        try
        {
            // 4. Act: Submit a new job via API (Schedule in the past to avoid clock skew issues with Postgres container)
            var request = new CreateJobRequest("E2E_TestJob", "{\"data\":\"test\"}", "Default", 10, 3, DateTimeOffset.UtcNow.AddMinutes(-1), "e2e-idemp-1");
            var response = await _client.PostAsJsonAsync("/jobs", request);
            response.EnsureSuccessStatusCode();
            
            var createdJob = await response.Content.ReadFromJsonAsync<Job>();
            Assert.NotNull(createdJob);
            Assert.Equal(JobStatus.Pending, createdJob.Status);

            // 5. Poll for completion
            // The job goes Pending (API) -> Queued (Scheduler) -> Running (Worker) -> Completed (Worker)
            int? currentJobState = null;
            var maxRetries = 30; // Wait up to ~15 seconds
            
            for (int i = 0; i < maxRetries; i++)
            {
                await Task.Delay(500);
                
                var getResponse = await _client.GetAsync($"/jobs/{createdJob.Id}");
                if (getResponse.IsSuccessStatusCode)
                {
                    var node = await getResponse.Content.ReadFromJsonAsync<System.Text.Json.Nodes.JsonNode>();
                    currentJobState = node?["status"]?.GetValue<int>();
                    if (currentJobState == (int)JobStatus.Completed) break;
                }
            }

            // 6. Assert: Job reached Completed state
            Assert.Equal((int)JobStatus.Completed, currentJobState);
        }
        finally
        {
            // 7. Cleanup running background services
            await cts.CancelAsync();
            try { await Task.WhenAll(dispatcherTask, executorTask); } catch (OperationCanceledException) { }
        }
    }

}
