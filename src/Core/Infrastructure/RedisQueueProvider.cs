using StackExchange.Redis;
using DistributedJobScheduler.Core.Domain;
using DistributedJobScheduler.Core.Scheduling;

namespace DistributedJobScheduler.Core.Infrastructure;

public sealed class RedisQueueProvider : IQueueProvider
{
    private readonly IConnectionMultiplexer _redis;
    private readonly IDatabase _db;

    public RedisQueueProvider(IConnectionMultiplexer redis)
    {
        _redis = redis;
        // Gets the default database (usually DB 0) from the Redis connection
        _db = _redis.GetDatabase();
    }

    public async Task PushAsync(Job job, CancellationToken cancellationToken)
    {
        // 1. Calculate the score using our pure domain logic
        double score = JobScoreCalculator.Calculate(job);
        
        // 2. Determine the exact topic queue key (e.g., "jobs:ready:cpu")
        string queueKey = GetQueueKey(job.Topic);
        
        // 3. Push the JobId onto the Redis Sorted Set (ZADD)
        // If the JobId is already in the set, it updates its score (good for retries)
        await _db.SortedSetAddAsync(queueKey, job.Id.ToString(), score);
    }

    public async Task<Guid?> PopAsync(IEnumerable<JobTopic> topics, TimeSpan timeout, CancellationToken cancellationToken)
    {
        // Convert the requested topics into the exact Redis keys (e.g., ["jobs:ready:cpu", "jobs:ready:default"])
        var keys = topics.Select(t => (RedisKey)GetQueueKey(t)).ToArray();
        
        if (keys.Length == 0) return null;

        // BZMPOP allows us to block and pop the lowest score from MULTIPLE sorted sets at once.
        // It checks the keys in the order they are provided in the array.
        // Format: BZMPOP timeout numkeys key [key ...] MIN COUNT 1
        
        // Note: StackExchange.Redis doesn't have a dedicated strongly-typed BZMPOP method yet,
        // so we use ExecuteAsync to run the raw Redis command.
        
        // Command arguments: BZMPOP {timeout_seconds} {num_keys} {key1} {key2} ... MIN COUNT 1
        var args = new List<object> { timeout.TotalSeconds, keys.Length };
        foreach (var key in keys)
        {
            args.Add(key);
        }
        args.Add("MIN");
        args.Add("COUNT");
        args.Add(1);

        try
        {
            var result = await _db.ExecuteAsync("BZMPOP", args.ToArray());

            // Redis BZMPOP returns an array: [ "key_it_was_popped_from", [ ["member", "score"] ] ]
            if (result.IsNull) return null;

            var outerArray = (RedisResult[]?)result;
            if (outerArray == null || outerArray.Length < 2) return null;

            var elementsArray = (RedisResult[]?)outerArray[1];
            if (elementsArray == null || elementsArray.Length < 1) return null;

            var elementTuple = (RedisResult[]?)elementsArray[0];
            if (elementTuple == null || elementTuple.Length < 1) return null;

            var poppedJobIdStr = (string?)elementTuple[0];
            
            if (poppedJobIdStr != null && Guid.TryParse(poppedJobIdStr, out var jobId))
            {
                return jobId;
            }
            
            return null;
        }
        catch (RedisServerException ex) when (ex.Message.Contains("unknown command"))
        {
            throw new NotSupportedException("Your Redis server is too old. BZMPOP requires Redis 7.0+.", ex);
        }
    }

    private static string GetQueueKey(JobTopic topic)
    {
        // Converts Enum "CpuIntensive" -> "cpuintensive" -> "jobs:ready:cpuintensive"
        return $"jobs:ready:{topic.ToString().ToLowerInvariant()}";
    }
}
