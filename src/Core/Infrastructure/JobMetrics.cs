using System.Diagnostics.Metrics;

namespace DistributedJobScheduler.Core.Infrastructure;

public static class JobMetrics
{
    // The main Meter for our entire distributed system
    public static readonly Meter Meter = new("DistributedJobScheduler");
    
    // Counters
    public static readonly Counter<int> JobsQueued = Meter.CreateCounter<int>("jobs.queued", description: "Number of jobs successfully queued to Redis");
    public static readonly Counter<int> JobsProcessed = Meter.CreateCounter<int>("jobs.processed", description: "Number of jobs successfully executed by workers");
    
    // Histograms
    public static readonly Histogram<double> JobExecutionTime = Meter.CreateHistogram<double>("jobs.execution_time", unit: "ms", description: "Time taken to execute a job");
}
