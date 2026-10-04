using DistributedJobScheduler.Worker;
using Xunit;

namespace DistributedJobScheduler.UnitTests.WorkerTests;

public class ActiveJobTrackerTests
{
    [Fact]
    public void Add_And_Remove_WorkCorrectly()
    {
        var tracker = new ActiveJobTracker();
        var jobId = Guid.NewGuid();

        tracker.Add(jobId);
        var activeJobs = tracker.GetActiveJobIds();

        Assert.Single(activeJobs);
        Assert.Equal(jobId, activeJobs[0]);

        tracker.Remove(jobId);
        activeJobs = tracker.GetActiveJobIds();

        Assert.Empty(activeJobs);
    }
}
