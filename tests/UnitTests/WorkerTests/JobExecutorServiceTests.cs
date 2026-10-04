using DistributedJobScheduler.Core.Domain;
using DistributedJobScheduler.Core.Infrastructure;
using DistributedJobScheduler.Worker;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace DistributedJobScheduler.UnitTests.WorkerTests;

public class JobExecutorServiceTests
{
    [Fact]
    public async Task ExecuteAsync_ProcessesJobsSuccessfully()
    {
        var queueMock = new Mock<IQueueProvider>();
        var repoMock = new Mock<IJobRepository>();
        var tracker = new ActiveJobTracker();
        var loggerMock = new Mock<ILogger<JobExecutorService>>();

        var jobId = Guid.NewGuid();
        var job = new Job { Id = jobId, Topic = JobTopic.Default, Priority = JobPriority.From(5) };
        job.MarkAsRunning("worker", DateTimeOffset.UtcNow.AddMinutes(5), DateTimeOffset.UtcNow); // mock running initial for test 
        
        // Let's create a fresh job for Queued state
        var queuedJob = new Job { Id = jobId, Topic = JobTopic.Default, Priority = JobPriority.From(5) };
        typeof(Job).GetProperty("Status")?.SetValue(queuedJob, JobStatus.Queued, null);
        
        // Manually force status by reflection or just mock repo to return it
        queueMock.Setup(q => q.PopAsync(It.IsAny<JobTopic[]>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .Returns(async (JobTopic[] _, TimeSpan __, CancellationToken ct) => 
            {
                await Task.Delay(10, ct);
                return jobId;
            });
            
        repoMock.Setup(r => r.GetJobAsync(jobId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(queuedJob);

        var service = new JobExecutorService(queueMock.Object, repoMock.Object, tracker, loggerMock.Object);

        var cts = new CancellationTokenSource();
        var executeTask = service.StartAsync(cts.Token);
        await Task.Delay(200); 
        cts.Cancel();
        try { await executeTask; } catch (OperationCanceledException) {}

        repoMock.Verify(r => r.UpdateJobAsync(It.IsAny<Job>(), It.IsAny<CancellationToken>()), Times.AtLeastOnce);
    }
}
