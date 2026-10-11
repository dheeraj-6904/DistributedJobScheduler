using DistributedJobScheduler.Core.Domain;
using DistributedJobScheduler.Core.Infrastructure;
using DistributedJobScheduler.Scheduler;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace DistributedJobScheduler.UnitTests.SchedulerTests;

public class LeaseSweeperServiceTests
{
    [Fact]
    public async Task ExecuteAsync_WhenJobsAreResetToQueued_PushesToQueue()
    {
        var repoMock = new Mock<IJobRepository>();
        var queueMock = new Mock<IQueueProvider>();
        var loggerMock = new Mock<ILogger<LeaseSweeperService>>();

        var expiredJob = new Job
        {
            Id = Guid.NewGuid(),
            Topic = JobTopic.Default,
            Priority = JobPriority.From(3),
            MaxAttempts = 3
        };
        expiredJob.MarkAsQueued();

        repoMock.Setup(r => r.SweepExpiredLeasesAsync(It.IsAny<CancellationToken>()))
            .Returns(async (CancellationToken ct) =>
            {
                await Task.Delay(10, ct);
                return new List<Job> { expiredJob };
            });

        var service = new LeaseSweeperService(repoMock.Object, queueMock.Object, loggerMock.Object);

        using var cts = new CancellationTokenSource();
        var executeTask = service.StartAsync(cts.Token);
        await Task.Delay(150);
        cts.Cancel();
        try { await executeTask; } catch (OperationCanceledException) { }

        queueMock.Verify(q => q.PushAsync(It.Is<Job>(j => j.Id == expiredJob.Id), It.IsAny<CancellationToken>()), Times.AtLeastOnce);
    }

    [Fact]
    public async Task ExecuteAsync_WhenJobIsPermanentlyFailed_DoesNotPushToQueue()
    {
        var repoMock = new Mock<IJobRepository>();
        var queueMock = new Mock<IQueueProvider>();
        var loggerMock = new Mock<ILogger<LeaseSweeperService>>();

        var failedJob = new Job
        {
            Id = Guid.NewGuid(),
            Topic = JobTopic.Default,
            Priority = JobPriority.From(3),
            MaxAttempts = 3
        };
        failedJob.MarkAsFailed("Worker timed out", DateTimeOffset.UtcNow);

        repoMock.Setup(r => r.SweepExpiredLeasesAsync(It.IsAny<CancellationToken>()))
            .Returns(async (CancellationToken ct) =>
            {
                await Task.Delay(10, ct);
                return new List<Job> { failedJob };
            });

        var service = new LeaseSweeperService(repoMock.Object, queueMock.Object, loggerMock.Object);

        using var cts = new CancellationTokenSource();
        var executeTask = service.StartAsync(cts.Token);
        await Task.Delay(150);
        cts.Cancel();
        try { await executeTask; } catch (OperationCanceledException) { }

        queueMock.Verify(q => q.PushAsync(It.IsAny<Job>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task ExecuteAsync_WhenSweptJobFailsToPushToRedis_RollsBackToPending()
    {
        var repoMock = new Mock<IJobRepository>();
        var queueMock = new Mock<IQueueProvider>();
        var loggerMock = new Mock<ILogger<LeaseSweeperService>>();

        var expiredJob = new Job
        {
            Id = Guid.NewGuid(),
            Topic = JobTopic.Default,
            Priority = JobPriority.From(3),
            MaxAttempts = 3
        };
        expiredJob.MarkAsQueued();

        repoMock.Setup(r => r.SweepExpiredLeasesAsync(It.IsAny<CancellationToken>()))
            .Returns(async (CancellationToken ct) =>
            {
                await Task.Delay(10, ct);
                return new List<Job> { expiredJob };
            });

        queueMock.Setup(q => q.PushAsync(It.IsAny<Job>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("Redis unavailable"));

        List<Guid>? rolledBackIds = null;
        repoMock.Setup(r => r.RollbackToPendingAsync(It.IsAny<IEnumerable<Guid>>(), It.IsAny<CancellationToken>()))
            .Callback<IEnumerable<Guid>, CancellationToken>((ids, _) => rolledBackIds = ids.ToList())
            .Returns(Task.CompletedTask);

        var service = new LeaseSweeperService(repoMock.Object, queueMock.Object, loggerMock.Object);

        using var cts = new CancellationTokenSource();
        await service.StartAsync(cts.Token);
        await Task.Delay(800); // 3 push attempts with 100ms + 200ms backoff
        cts.Cancel();
        await service.StopAsync(CancellationToken.None);

        queueMock.Verify(q => q.PushAsync(It.Is<Job>(j => j.Id == expiredJob.Id), It.IsAny<CancellationToken>()), Times.Exactly(3));
        Assert.Equal(new[] { expiredJob.Id }, rolledBackIds);
        Assert.Equal(JobStatus.Pending, expiredJob.Status);
    }
}
