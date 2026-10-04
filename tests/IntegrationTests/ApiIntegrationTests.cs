using System.Net.Http.Json;
using DistributedJobScheduler.Core.Domain;
using DistributedJobScheduler.Core.Infrastructure;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Xunit;

namespace DistributedJobScheduler.IntegrationTests;

[Collection("Integration Tests")]
public class ApiIntegrationTests : IAsyncLifetime
{
    private readonly IntegrationTestFixture _fixture;
    private readonly WebApplicationFactory<Program> _factory;
    private readonly HttpClient _client;

    public ApiIntegrationTests(IntegrationTestFixture fixture)
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
    }

    public Task DisposeAsync() => Task.CompletedTask;

    [Fact]
    public async Task PostJob_SavesToDatabase()
    {
        var request = new CreateJobRequest("EmailJob", "{\"to\":\"test@test.com\"}", "Default", 5, 3, null, "idemp-key-1");
        
        var response = await _client.PostAsJsonAsync("/jobs", request);
        response.EnsureSuccessStatusCode();

        var jobResponse = await response.Content.ReadFromJsonAsync<Job>();
        Assert.NotNull(jobResponse);
        Assert.Equal("EmailJob", jobResponse.Type);
        
        var getResponse = await _client.GetAsync($"/jobs/{jobResponse.Id}");
        getResponse.EnsureSuccessStatusCode();
        var retrievedJob = await getResponse.Content.ReadFromJsonAsync<Job>();
        
        Assert.NotNull(retrievedJob);
        Assert.Equal(jobResponse.Id, retrievedJob.Id);
        Assert.Equal(JobStatus.Pending, retrievedJob.Status);
    }
}
