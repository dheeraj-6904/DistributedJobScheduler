using DistributedJobScheduler.Core.Domain;
using DistributedJobScheduler.Core.Infrastructure;
using Xunit;

namespace DistributedJobScheduler.UnitTests.CoreTests;

public class JobMetricsTests
{
    [Fact]
    public void RecordAndGetQueueDepth_TracksPerTopicAccurately()
    {
        JobMetrics.RecordQueueDepth(JobTopic.Default, 10);
        JobMetrics.RecordQueueDepth(JobTopic.CpuIntensive, 25);
        JobMetrics.RecordQueueDepth(JobTopic.GpuIntensive, 3);

        Assert.Equal(10, JobMetrics.GetQueueDepth(JobTopic.Default));
        Assert.Equal(25, JobMetrics.GetQueueDepth(JobTopic.CpuIntensive));
        Assert.Equal(3, JobMetrics.GetQueueDepth(JobTopic.GpuIntensive));
    }

    [Fact]
    public void ObservableGauge_IsRegisteredOnMeter()
    {
        Assert.NotNull(JobMetrics.QueueBacklog);
        Assert.Equal("jobs.queue_backlog", JobMetrics.QueueBacklog.Name);
    }
}
