using DistributedJobScheduler.Core.Domain;
using DistributedJobScheduler.Core.Infrastructure;
using DistributedJobScheduler.Scheduler;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace DistributedJobScheduler.UnitTests.SchedulerTests;

public class DispatcherServiceTests
{
    [Fact]
    public async Task ExecuteAsync_PushesJobsToQueue()
    {
        var repoMock = new Mock<IJobRepository>();
        var queueMock = new Mock<IQueueProvider>();
        var loggerMock = new Mock<ILogger<DispatcherService>>();

        var job = new Job { Id = Guid.NewGuid(), Topic = JobTopic.Default, Priority = JobPriority.From(5) };
        
        repoMock.Setup(r => r.DequeueReadyJobsAsync(It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .Returns(async (int _, CancellationToken ct) => 
            {
                await Task.Delay(10, ct); // Yield to prevent infinite sync loop
                return new List<Job> { job };
            });

        var service = new DispatcherService(repoMock.Object, queueMock.Object, loggerMock.Object);

        var cts = new CancellationTokenSource();
        var executeTask = service.StartAsync(cts.Token);
        await Task.Delay(150); // allow the loop to run
        cts.Cancel();
        try { await executeTask; } catch (OperationCanceledException) {}

        queueMock.Verify(q => q.PushAsync(It.IsAny<Job>(), It.IsAny<CancellationToken>()), Times.AtLeastOnce);
    }
}
