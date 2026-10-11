using DistributedJobScheduler.Core.Domain;
using DistributedJobScheduler.Core.Infrastructure;

namespace DistributedJobScheduler.Scheduler;

/// <summary>
/// Periodically samples Redis queue lengths to update the queue backlog Gauge metrics.
/// </summary>
public class QueueMetricsCollectorService : BackgroundService
{
    private readonly IQueueProvider _queue;
    private readonly ILogger<QueueMetricsCollectorService> _logger;
    private readonly TimeSpan _samplingInterval;

    public QueueMetricsCollectorService(
        IQueueProvider queue,
        ILogger<QueueMetricsCollectorService> logger,
        TimeSpan? samplingInterval = null)
    {
        _queue = queue;
        _logger = logger;
        _samplingInterval = samplingInterval ?? TimeSpan.FromSeconds(2);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Queue Metrics Collector started. Sampling queue depths every {IntervalSeconds}s.", _samplingInterval.TotalSeconds);

        var topics = Enum.GetValues<JobTopic>();

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                foreach (var topic in topics)
                {
                    long length = await _queue.GetQueueLengthAsync(topic, stoppingToken);
                    JobMetrics.RecordQueueDepth(topic, length);
                }

                await Task.Delay(_samplingInterval, stoppingToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogWarning(ex, "Failed to sample queue depths from Redis. Retrying after backoff.");
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
            }
        }
    }
}
