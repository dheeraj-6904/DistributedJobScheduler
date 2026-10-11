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

    [Fact]
    public async Task ExecuteAsync_WhenRedisPushFails_RollsBackUnpushedJobsToPending()
    {
        var repoMock = new Mock<IJobRepository>();
        var queueMock = new Mock<IQueueProvider>();
        var loggerMock = new Mock<ILogger<DispatcherService>>();

        var job1 = CreateQueuedJob();
        var job2 = CreateQueuedJob();
        var job3 = CreateQueuedJob();

        SetupSingleBatch(repoMock, job1, job2, job3);

        queueMock.Setup(q => q.PushAsync(It.Is<Job>(j => j.Id == job2.Id), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("Redis unavailable"));

        List<Guid>? rolledBackIds = null;
        repoMock.Setup(r => r.RollbackToPendingAsync(It.IsAny<IEnumerable<Guid>>(), It.IsAny<CancellationToken>()))
            .Callback<IEnumerable<Guid>, CancellationToken>((ids, _) => rolledBackIds = ids.ToList())
            .Returns(Task.CompletedTask);

        var service = new DispatcherService(repoMock.Object, queueMock.Object, loggerMock.Object);

        await RunBrieflyAsync(service, TimeSpan.FromMilliseconds(800));

        // job2 is retried 3 times before giving up; job3 is never attempted
        queueMock.Verify(q => q.PushAsync(It.Is<Job>(j => j.Id == job2.Id), It.IsAny<CancellationToken>()), Times.Exactly(3));
        queueMock.Verify(q => q.PushAsync(It.Is<Job>(j => j.Id == job3.Id), It.IsAny<CancellationToken>()), Times.Never);

        Assert.NotNull(rolledBackIds);
        Assert.Equal(new[] { job2.Id, job3.Id }, rolledBackIds);

        Assert.Equal(JobStatus.Queued, job1.Status);
        Assert.Equal(JobStatus.Pending, job2.Status);
        Assert.Equal(JobStatus.Pending, job3.Status);
    }

    [Fact]
    public async Task ExecuteAsync_WhenRedisPushFailsIntermittently_RetriesAndSucceedsWithoutRollback()
    {
        var repoMock = new Mock<IJobRepository>();
        var queueMock = new Mock<IQueueProvider>();
        var loggerMock = new Mock<ILogger<DispatcherService>>();

        var job = CreateQueuedJob();
        SetupSingleBatch(repoMock, job);

        int pushAttempts = 0;
        queueMock.Setup(q => q.PushAsync(It.Is<Job>(j => j.Id == job.Id), It.IsAny<CancellationToken>()))
            .Returns(() => Interlocked.Increment(ref pushAttempts) == 1
                ? Task.FromException(new InvalidOperationException("Transient Redis error"))
                : Task.CompletedTask);

        var service = new DispatcherService(repoMock.Object, queueMock.Object, loggerMock.Object);

        await RunBrieflyAsync(service, TimeSpan.FromMilliseconds(500));

        Assert.Equal(2, pushAttempts);
        repoMock.Verify(r => r.RollbackToPendingAsync(It.IsAny<IEnumerable<Guid>>(), It.IsAny<CancellationToken>()), Times.Never);
        Assert.Equal(JobStatus.Queued, job.Status);
    }

    private static Job CreateQueuedJob()
    {
        var job = new Job { Id = Guid.NewGuid(), Topic = JobTopic.Default, Priority = JobPriority.From(5), MaxAttempts = 3 };
        job.MarkAsQueued();
        return job;
    }

    // Returns the given batch on the first poll only, so the loop doesn't redispatch the same jobs.
    private static void SetupSingleBatch(Mock<IJobRepository> repoMock, params Job[] jobs)
    {
        int calls = 0;
        repoMock.Setup(r => r.DequeueReadyJobsAsync(It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .Returns(async (int _, CancellationToken ct) =>
            {
                await Task.Delay(10, ct);
                return Interlocked.Increment(ref calls) == 1 ? jobs.ToList() : new List<Job>();
            });
    }

    private static async Task RunBrieflyAsync(DispatcherService service, TimeSpan duration)
    {
        using var cts = new CancellationTokenSource();
        await service.StartAsync(cts.Token);
        await Task.Delay(duration);
        cts.Cancel();
        await service.StopAsync(CancellationToken.None);
    }
}
