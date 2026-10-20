using DistributedJobScheduler.Core.Infrastructure;
using DistributedJobScheduler.Worker;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace DistributedJobScheduler.UnitTests.WorkerTests;

public class HeartbeatServiceTests
{
    [Fact]
    public async Task ExecuteAsync_RenewsLeasesForActiveJobs()
    {
        var tracker = new ActiveJobTracker();
        var repoMock = new Mock<IJobRepository>();
        var loggerMock = new Mock<ILogger<HeartbeatService>>();

        var jobId1 = Guid.NewGuid();
        var jobId2 = Guid.NewGuid();
        var jobId3 = Guid.NewGuid();
        tracker.Add(jobId1);
        tracker.Add(jobId2);
        tracker.Add(jobId3);

        var service = new HeartbeatService(tracker, repoMock.Object, loggerMock.Object, TimeSpan.FromMilliseconds(50));

        using var cts = new CancellationTokenSource();
        await service.StartAsync(cts.Token);
        await Task.Delay(150);
        await cts.CancelAsync();
        await service.StopAsync(CancellationToken.None);

        repoMock.Verify(
            r => r.RenewLeasesAsync(
                It.Is<IReadOnlyList<Guid>>(l => l.Count == 3 && l.Contains(jobId1) && l.Contains(jobId2) && l.Contains(jobId3)),
                It.IsAny<DateTimeOffset>(),
                It.IsAny<CancellationToken>()),
            Times.AtLeastOnce);
    }
}
