using DistributedJobScheduler.Core.Domain;
using DistributedJobScheduler.Core.Infrastructure;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using System.Net;
using System.Net.Http.Json;
using Xunit;

namespace DistributedJobScheduler.UnitTests.ApiTests;

public class ApiEndpointsTests : IClassFixture<WebApplicationFactory<Program>>
{
    private readonly WebApplicationFactory<Program> _factory;

    public ApiEndpointsTests(WebApplicationFactory<Program> factory)
    {
        _factory = factory;
    }

    [Fact]
    public async Task GetJob_ReturnsOk_WhenJobExists()
    {
        var jobId = Guid.NewGuid();
        var job = new Job { Id = jobId, Type = "Test" };
        var repoMock = new Mock<IJobRepository>();
        repoMock.Setup(r => r.GetJobAsync(jobId, It.IsAny<CancellationToken>())).ReturnsAsync(job);

        var client = _factory.WithWebHostBuilder(builder =>
        {
            builder.ConfigureServices(services =>
            {
                services.AddSingleton(repoMock.Object);
            });
        }).CreateClient();

        var response = await client.GetAsync($"/jobs/{jobId}");
        
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var returnedJob = await response.Content.ReadFromJsonAsync<Job>();
        Assert.NotNull(returnedJob);
        Assert.Equal(jobId, returnedJob.Id);
    }

    [Fact]
    public async Task GetJob_ReturnsNotFound_WhenJobDoesNotExist()
    {
        var repoMock = new Mock<IJobRepository>();
        repoMock.Setup(r => r.GetJobAsync(It.IsAny<Guid>(), It.IsAny<CancellationToken>())).ReturnsAsync((Job?)null);

        var client = _factory.WithWebHostBuilder(builder =>
        {
            builder.ConfigureServices(services =>
            {
                services.AddSingleton(repoMock.Object);
            });
        }).CreateClient();

        var response = await client.GetAsync($"/jobs/{Guid.NewGuid()}");
        
        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }
}
