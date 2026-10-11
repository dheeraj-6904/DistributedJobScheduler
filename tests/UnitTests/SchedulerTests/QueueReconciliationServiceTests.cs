using DistributedJobScheduler.Core.Domain;
using DistributedJobScheduler.Core.Infrastructure;
using DistributedJobScheduler.Scheduler;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace DistributedJobScheduler.UnitTests.SchedulerTests;

public class QueueReconciliationServiceTests
{
    [Fact]
    public async Task ExecuteAsync_WhenStaleJobMissingFromRedis_RollsBackToPending()
    {
        var repoMock = new Mock<IJobRepository>();
        var queueMock = new Mock<IQueueProvider>();

        var job = CreateStaleQueuedJob();
        SetupSingleStaleBatch(repoMock, job);
        SetupRedisPresence(queueMock, job, existsInRedis: false);

        var rolledBackIds = CaptureRollbacks(repoMock);

        await RunBrieflyAsync(CreateService(repoMock, queueMock));

        Assert.Equal(new[] { job.Id }, rolledBackIds.SingleOrDefault());
        repoMock.Verify(r => r.UpdateQueuedAtAsync(It.IsAny<IEnumerable<Guid>>(), It.IsAny<DateTimeOffset>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task ExecuteAsync_WhenStaleJobAlreadyInRedis_UpdatesQueuedAtWithoutRollback()
    {
        var repoMock = new Mock<IJobRepository>();
        var queueMock = new Mock<IQueueProvider>();

        var job = CreateStaleQueuedJob();
        SetupSingleStaleBatch(repoMock, job);
        SetupRedisPresence(queueMock, job, existsInRedis: true);

        var refreshedIds = CaptureQueuedAtUpdates(repoMock);

        await RunBrieflyAsync(CreateService(repoMock, queueMock));

        Assert.Equal(new[] { job.Id }, refreshedIds.SingleOrDefault());
        repoMock.Verify(r => r.RollbackToPendingAsync(It.IsAny<IEnumerable<Guid>>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task ExecuteAsync_WithMixedBatch_CorrectlyPartitionsRollbacksAndUpdates()
    {
        var repoMock = new Mock<IJobRepository>();
        var queueMock = new Mock<IQueueProvider>();

        var missingJob = CreateStaleQueuedJob();
        var presentJob = CreateStaleQueuedJob();
        SetupSingleStaleBatch(repoMock, missingJob, presentJob);
        SetupRedisPresence(queueMock, missingJob, existsInRedis: false);
        SetupRedisPresence(queueMock, presentJob, existsInRedis: true);

        var rolledBackIds = CaptureRollbacks(repoMock);
        var refreshedIds = CaptureQueuedAtUpdates(repoMock);

        await RunBrieflyAsync(CreateService(repoMock, queueMock));

        Assert.Equal(new[] { missingJob.Id }, rolledBackIds.SingleOrDefault());
        Assert.Equal(new[] { presentJob.Id }, refreshedIds.SingleOrDefault());
    }

    [Fact]
    public async Task ExecuteAsync_NeverPushesToRedisDirectly()
    {
        var repoMock = new Mock<IJobRepository>();
        var queueMock = new Mock<IQueueProvider>();

        var job = CreateStaleQueuedJob();
        SetupSingleStaleBatch(repoMock, job);
        SetupRedisPresence(queueMock, job, existsInRedis: false);

        await RunBrieflyAsync(CreateService(repoMock, queueMock));

        // Re-dispatch is DispatcherService's responsibility, not the reconciler's
        queueMock.Verify(q => q.PushAsync(It.IsAny<Job>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    private static QueueReconciliationService CreateService(Mock<IJobRepository> repoMock, Mock<IQueueProvider> queueMock) =>
        new(repoMock.Object,
            queueMock.Object,
            new Mock<ILogger<QueueReconciliationService>>().Object,
            staleThreshold: TimeSpan.FromMinutes(1),
            pollInterval: TimeSpan.FromMilliseconds(50));

    private static Job CreateStaleQueuedJob()
    {
        var job = new Job { Id = Guid.NewGuid(), Topic = JobTopic.Default, Priority = JobPriority.From(5), MaxAttempts = 3 };
        job.MarkAsQueued(DateTimeOffset.UtcNow.AddMinutes(-5));
        return job;
    }

    // Returns the stale jobs on the first sweep only, so assertions see exactly one reconciliation pass.
    private static void SetupSingleStaleBatch(Mock<IJobRepository> repoMock, params Job[] jobs)
    {
        int calls = 0;
        repoMock.Setup(r => r.GetStaleQueuedJobsAsync(It.IsAny<DateTimeOffset>(), It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .Returns(async (DateTimeOffset _, int _, CancellationToken ct) =>
            {
                await Task.Delay(10, ct);
                return Interlocked.Increment(ref calls) == 1 ? jobs.ToList() : new List<Job>();
            });
    }

    private static void SetupRedisPresence(Mock<IQueueProvider> queueMock, Job job, bool existsInRedis)
    {
        queueMock.Setup(q => q.ContainsAsync(job.Topic, job.Id, It.IsAny<CancellationToken>()))
            .ReturnsAsync(existsInRedis);
    }

    private static List<List<Guid>> CaptureRollbacks(Mock<IJobRepository> repoMock)
    {
        var calls = new List<List<Guid>>();
        repoMock.Setup(r => r.RollbackToPendingAsync(It.IsAny<IEnumerable<Guid>>(), It.IsAny<CancellationToken>()))
            .Callback<IEnumerable<Guid>, CancellationToken>((ids, _) => calls.Add(ids.ToList()))
            .Returns(Task.CompletedTask);
        return calls;
    }

    private static List<List<Guid>> CaptureQueuedAtUpdates(Mock<IJobRepository> repoMock)
    {
        var calls = new List<List<Guid>>();
        repoMock.Setup(r => r.UpdateQueuedAtAsync(It.IsAny<IEnumerable<Guid>>(), It.IsAny<DateTimeOffset>(), It.IsAny<CancellationToken>()))
            .Callback<IEnumerable<Guid>, DateTimeOffset, CancellationToken>((ids, _, _) => calls.Add(ids.ToList()))
            .Returns(Task.CompletedTask);
        return calls;
    }

    private static async Task RunBrieflyAsync(QueueReconciliationService service)
    {
        using var cts = new CancellationTokenSource();
        await service.StartAsync(cts.Token);
        await Task.Delay(300);
        cts.Cancel();
        await service.StopAsync(CancellationToken.None);
    }
}
