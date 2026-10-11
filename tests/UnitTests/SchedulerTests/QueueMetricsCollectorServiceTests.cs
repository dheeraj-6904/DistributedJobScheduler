using DistributedJobScheduler.Core.Domain;
using DistributedJobScheduler.Core.Infrastructure;
using DistributedJobScheduler.Scheduler;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace DistributedJobScheduler.UnitTests.SchedulerTests;

public class QueueMetricsCollectorServiceTests
{
    [Fact]
    public async Task ExecuteAsync_SamplesQueueLengthAndUpdatesJobMetrics()
    {
        var queueMock = new Mock<IQueueProvider>();
        var loggerMock = new Mock<ILogger<QueueMetricsCollectorService>>();

        queueMock.Setup(q => q.GetQueueLengthAsync(JobTopic.Default, It.IsAny<CancellationToken>()))
            .ReturnsAsync(42L);
        queueMock.Setup(q => q.GetQueueLengthAsync(JobTopic.CpuIntensive, It.IsAny<CancellationToken>()))
            .ReturnsAsync(15L);
        queueMock.Setup(q => q.GetQueueLengthAsync(JobTopic.GpuIntensive, It.IsAny<CancellationToken>()))
            .ReturnsAsync(7L);

        var service = new QueueMetricsCollectorService(
            queueMock.Object,
            loggerMock.Object,
            samplingInterval: TimeSpan.FromMilliseconds(50));

        using var cts = new CancellationTokenSource();
        var executeTask = service.StartAsync(cts.Token);
        await Task.Delay(150);
        cts.Cancel();
        try { await executeTask; } catch (OperationCanceledException) { }

        Assert.Equal(42L, JobMetrics.GetQueueDepth(JobTopic.Default));
        Assert.Equal(15L, JobMetrics.GetQueueDepth(JobTopic.CpuIntensive));
        Assert.Equal(7L, JobMetrics.GetQueueDepth(JobTopic.GpuIntensive));

        queueMock.Verify(q => q.GetQueueLengthAsync(JobTopic.Default, It.IsAny<CancellationToken>()), Times.AtLeastOnce);
        queueMock.Verify(q => q.GetQueueLengthAsync(JobTopic.CpuIntensive, It.IsAny<CancellationToken>()), Times.AtLeastOnce);
        queueMock.Verify(q => q.GetQueueLengthAsync(JobTopic.GpuIntensive, It.IsAny<CancellationToken>()), Times.AtLeastOnce);
    }
}
