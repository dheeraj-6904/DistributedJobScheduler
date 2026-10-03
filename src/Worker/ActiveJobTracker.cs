using System.Collections.Concurrent;

namespace DistributedJobScheduler.Worker;

/// <summary>
/// A simple thread-safe singleton to keep track of jobs currently running on this specific worker instance.
/// </summary>
public sealed class ActiveJobTracker
{
    // A ConcurrentDictionary used as a ConcurrentHashSet
    private readonly ConcurrentDictionary<Guid, byte> _activeJobs = new();

    public void Add(Guid jobId) => _activeJobs.TryAdd(jobId, 0);
    public void Remove(Guid jobId) => _activeJobs.TryRemove(jobId, out _);
    
    public IReadOnlyList<Guid> GetActiveJobIds() => _activeJobs.Keys.ToList();
}
