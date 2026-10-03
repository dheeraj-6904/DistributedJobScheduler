using DistributedJobScheduler.Core.Infrastructure;

namespace DistributedJobScheduler.Worker;

/// <summary>
/// Background loop that periodically extends the PostgreSQL lease for all jobs this worker is currently processing.
/// </summary>
public sealed class HeartbeatService : BackgroundService
{
    private readonly ActiveJobTracker _tracker;
    private readonly IJobRepository _repository;
    private readonly ILogger<HeartbeatService> _logger;

    public HeartbeatService(ActiveJobTracker tracker, IJobRepository repository, ILogger<HeartbeatService> logger)
    {
        _tracker = tracker;
        _repository = repository;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Heartbeat Service started.");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var activeJobs = _tracker.GetActiveJobIds();
                if (activeJobs.Count > 0)
                {
                    // Extend the lease by 30 seconds from now
                    var newLease = DateTimeOffset.UtcNow.AddSeconds(30);
                    await _repository.RenewLeasesAsync(activeJobs, newLease, stoppingToken);
                    _logger.LogDebug("Renewed leases for {Count} active jobs.", activeJobs.Count);
                }

                // Fire the heartbeat every 10 seconds
                await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogError(ex, "Failed to renew leases.");
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken); // Backoff on error
            }
        }
    }
}
