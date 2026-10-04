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

        var jobId = Guid.NewGuid();
        tracker.Add(jobId);

        var service = new HeartbeatService(tracker, repoMock.Object, loggerMock.Object);

        var cts = new CancellationTokenSource();
        var executeTask = service.StartAsync(cts.Token);
        await Task.Delay(200); // let heartbeat run once
        cts.Cancel();
        try { await executeTask; } catch (OperationCanceledException) {}

        repoMock.Verify(r => r.RenewLeasesAsync(It.Is<IReadOnlyList<Guid>>(l => l.Contains(jobId)), It.IsAny<DateTimeOffset>(), It.IsAny<CancellationToken>()), Times.AtLeastOnce);
    }
}
