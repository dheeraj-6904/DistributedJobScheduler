using System.Data;
using Dapper;
using Npgsql;
using DistributedJobScheduler.Core.Domain;

namespace DistributedJobScheduler.Core.Infrastructure;

public sealed class PostgresJobRepository : IJobRepository
{
    private readonly NpgsqlDataSource _dataSource;

    public PostgresJobRepository(NpgsqlDataSource dataSource)
    {
        _dataSource = dataSource;
        
        // Tells Dapper to map Enums to/from strings in the database automatically
        SqlMapper.AddTypeHandler(new EnumAsStringHandler<JobStatus>());
        SqlMapper.AddTypeHandler(new EnumAsStringHandler<JobTopic>());
        SqlMapper.AddTypeHandler(new JobPriorityHandler());
    }

    private class JobPriorityHandler : SqlMapper.TypeHandler<JobPriority>
    {
        public override void SetValue(System.Data.IDbDataParameter parameter, JobPriority value)
        {
            parameter.Value = value.Value;
        }

        public override JobPriority Parse(object value)
        {
            return JobPriority.From(Convert.ToInt32(value));
        }
    }

    public async Task<Job?> GetJobAsync(Guid id, CancellationToken cancellationToken)
    {
        const string sql = "SELECT * FROM Jobs WHERE Id = @Id";
        
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        return await connection.QuerySingleOrDefaultAsync<Job>(sql, new { Id = id });
    }

    public async Task<bool> CreateJobAsync(Job job, CancellationToken cancellationToken)
    {
        const string sql = @"
            INSERT INTO Jobs (
                Id, Type, Topic, Payload, Priority, Status, 
                Attempts, MaxAttempts, ScheduledAt, CreatedAt, IdempotencyKey
            ) VALUES (
                @Id, @Type, @Topic, @Payload::jsonb, @Priority, @StatusString, 
                @Attempts, @MaxAttempts, @ScheduledAt, @CreatedAt, @IdempotencyKey
            ) ON CONFLICT (IdempotencyKey) DO NOTHING;";

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        
        // Use an anonymous object to explicitly convert enum properties to strings
        var rowsAffected = await connection.ExecuteAsync(sql, new 
        {
            job.Id,
            job.Type,
            Topic = job.Topic.ToString(),
            job.Payload,
            job.Priority,
            StatusString = job.Status.ToString(),
            job.Attempts,
            job.MaxAttempts,
            job.ScheduledAt,
            job.CreatedAt,
            job.IdempotencyKey
        });
        
        // Returns true if inserted, false if the IdempotencyKey was a duplicate
        return rowsAffected > 0;
    }

    public async Task<IReadOnlyList<Job>> DequeueReadyJobsAsync(int batchSize, CancellationToken cancellationToken)
    {
        // The magic SKIP LOCKED query for the Scheduler
        const string sql = @"
            UPDATE Jobs 
            SET Status = 'Queued' 
            WHERE Id IN (
                SELECT Id FROM Jobs 
                WHERE Status = 'Pending' AND ScheduledAt <= NOW() 
                ORDER BY Priority ASC, ScheduledAt ASC 
                LIMIT @BatchSize 
                FOR UPDATE SKIP LOCKED
            )
            RETURNING *;";

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        var jobs = await connection.QueryAsync<Job>(sql, new { BatchSize = batchSize });
        return jobs.AsList();
    }

    public async Task<IReadOnlyList<Job>> SweepExpiredLeasesAsync(CancellationToken cancellationToken)
    {
        // The Lease Sweeper query
        // Re-queues the job and increments attempts since the worker failed to complete it
        const string sql = @"
            UPDATE Jobs 
            SET Status = 'Queued', Attempts = Attempts + 1, WorkerId = NULL, LeaseUntil = NULL 
            WHERE Id IN (
                SELECT Id FROM Jobs 
                WHERE Status = 'Running' AND LeaseUntil < NOW() 
                FOR UPDATE SKIP LOCKED
            )
            RETURNING *;";

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        var jobs = await connection.QueryAsync<Job>(sql);
        return jobs.AsList();
    }

    public async Task UpdateJobAsync(Job job, CancellationToken cancellationToken)
    {
        const string sql = @"
            UPDATE Jobs SET 
                Status = @StatusString,
                Attempts = @Attempts,
                NextRetryAt = @NextRetryAt,
                WorkerId = @WorkerId,
                LeaseUntil = @LeaseUntil,
                StartedAt = @StartedAt,
                CompletedAt = @CompletedAt,
                Result = @Result::jsonb,
                Error = @Error
            WHERE Id = @Id;";

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        
        await connection.ExecuteAsync(sql, new 
        {
            StatusString = job.Status.ToString(),
            job.Attempts,
            job.NextRetryAt,
            job.WorkerId,
            job.LeaseUntil,
            job.StartedAt,
            job.CompletedAt,
            job.Result,
            job.Error,
            job.Id
        });
    }

    public async Task RenewLeasesAsync(IEnumerable<Guid> jobIds, DateTimeOffset newLeaseUntil, CancellationToken cancellationToken)
    {
        const string sql = @"
            UPDATE Jobs 
            SET LeaseUntil = @NewLeaseUntil 
            WHERE Id = ANY(@JobIds) AND Status = 'Running';";

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await connection.ExecuteAsync(sql, new { JobIds = jobIds.ToList(), NewLeaseUntil = newLeaseUntil });
    }
}

// A simple Dapper handler to automatically convert Enums to strings for the DB
public class EnumAsStringHandler<T> : SqlMapper.TypeHandler<T> where T : struct, Enum
{
    public override void SetValue(IDbDataParameter parameter, T value)
    {
        parameter.Value = value.ToString();
    }

    public override T Parse(object value)
    {
        return Enum.Parse<T>(value.ToString()!, ignoreCase: true);
    }
}
