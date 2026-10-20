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
        Assert.Equal(1, tracker.Count);
        Assert.Equal(jobId, activeJobs[0]);

        tracker.Remove(jobId);
        activeJobs = tracker.GetActiveJobIds();

        Assert.Empty(activeJobs);
        Assert.Equal(0, tracker.Count);
    }

    [Fact]
    public async Task ConcurrentAddAndRemove_MaintainsConsistentActiveJobSet()
    {
        var tracker = new ActiveJobTracker();
        var jobIds = Enumerable.Range(0, 50).Select(_ => Guid.NewGuid()).ToArray();

        await Task.WhenAll(jobIds.Select(id => Task.Run(() => tracker.Add(id))));

        var activeSnapshot = tracker.GetActiveJobIds();
        Assert.Equal(jobIds.Length, activeSnapshot.Count);
        Assert.Equal(jobIds.Length, tracker.Count);

        await Task.WhenAll(jobIds.Take(25).Select(id => Task.Run(() => tracker.Remove(id))));

        var remaining = tracker.GetActiveJobIds();
        Assert.Equal(25, remaining.Count);
        foreach (var expected in jobIds.Skip(25))
        {
            Assert.Contains(expected, remaining);
        }
    }
}
