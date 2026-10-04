using DistributedJobScheduler.Core.Domain;
using Xunit;

namespace DistributedJobScheduler.UnitTests.CoreTests;

public class JobTests
{
    [Fact]
    public void MarkAsRunning_SetsCorrectProperties()
    {
        var job = new Job { Id = Guid.NewGuid(), MaxAttempts = 3, ScheduledAt = DateTimeOffset.UtcNow };
        var now = DateTimeOffset.UtcNow;
        var leaseUntil = now.AddSeconds(30);

        job.MarkAsRunning("worker-1", leaseUntil, now);

        Assert.Equal(JobStatus.Running, job.Status);
        Assert.Equal("worker-1", job.WorkerId);
        Assert.Equal(leaseUntil, job.LeaseUntil);
        Assert.Equal(now, job.StartedAt);
        Assert.Equal(1, job.Attempts);
    }

    [Fact]
    public void MarkAsCompleted_SetsCorrectProperties()
    {
        var job = new Job { Id = Guid.NewGuid(), MaxAttempts = 3 };
        var now = DateTimeOffset.UtcNow;

        job.MarkAsCompleted("success", now);

        Assert.Equal(JobStatus.Completed, job.Status);
        Assert.Equal("success", job.Result);
        Assert.Equal(now, job.CompletedAt);
        Assert.Null(job.WorkerId);
        Assert.Null(job.LeaseUntil);
    }

    [Fact]
    public void IsLeaseExpired_ReturnsTrue_WhenLeaseIsInPast()
    {
        var job = new Job { Id = Guid.NewGuid() };
        var now = DateTimeOffset.UtcNow;
        job.MarkAsRunning("worker", now.AddMinutes(-1), now.AddMinutes(-5));

        Assert.True(job.IsLeaseExpired(now));
    }

    [Fact]
    public void RenewLease_UpdatesLeaseUntil()
    {
        var job = new Job { Id = Guid.NewGuid() };
        var now = DateTimeOffset.UtcNow;
        job.MarkAsRunning("w1", now.AddSeconds(30), now);
        
        var newLease = now.AddSeconds(60);
        job.RenewLease(newLease);
        
        Assert.Equal(newLease, job.LeaseUntil);
    }
}
