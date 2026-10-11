using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using DistributedJobScheduler.Core.Domain;

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
    public static readonly Histogram<double> JobQueueTime = Meter.CreateHistogram<double>("jobs.queue_time", unit: "ms", description: "Time spent waiting in the queue before execution starts");
    public static readonly Histogram<double> JobTotalLifecycleTime = Meter.CreateHistogram<double>("jobs.total_lifecycle_time", unit: "ms", description: "Total time from job creation to completion");

    // Queue backlog gauge tracking active Redis queue length per topic
    private static readonly ConcurrentDictionary<string, long> QueueDepths = new(
        Enum.GetValues<JobTopic>().ToDictionary(
            t => t.ToString().ToLowerInvariant(),
            _ => 0L
        )
    );

    public static readonly ObservableGauge<long> QueueBacklog = Meter.CreateObservableGauge<long>(
        "jobs.queue_backlog",
        observeValues: () => QueueDepths.Select(kvp => new Measurement<long>(kvp.Value, new KeyValuePair<string, object?>("topic", kvp.Key))),
        unit: "{jobs}",
        description: "Current number of jobs waiting in Redis queues"
    );

    public static void RecordQueueDepth(JobTopic topic, long count)
    {
        QueueDepths[topic.ToString().ToLowerInvariant()] = count;
    }

    public static long GetQueueDepth(JobTopic topic)
    {
        return QueueDepths.TryGetValue(topic.ToString().ToLowerInvariant(), out var depth) ? depth : 0;
    }
}
