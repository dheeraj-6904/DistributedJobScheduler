using Npgsql;
using StackExchange.Redis;
using Testcontainers.PostgreSql;
using Testcontainers.Redis;
using Xunit;

namespace DistributedJobScheduler.IntegrationTests;

public class IntegrationTestFixture : IAsyncLifetime
{
    public PostgreSqlContainer PostgresContainer { get; }
    public RedisContainer RedisContainer { get; }
    
    public IConnectionMultiplexer RedisConnection { get; private set; } = null!;
    public string PostgresConnectionString => PostgresContainer.GetConnectionString();

    public IntegrationTestFixture()
    {
        PostgresContainer = new PostgreSqlBuilder()
            .WithImage("postgres:15-alpine")
            .WithDatabase("testdb")
            .WithUsername("postgres")
            .WithPassword("postgres")
            .Build();

        RedisContainer = new RedisBuilder()
            .WithImage("redis:7.2-alpine")
            .Build();
    }

    public async Task InitializeAsync()
    {
        await Task.WhenAll(PostgresContainer.StartAsync(), RedisContainer.StartAsync());

        // Initialize schema
        var schemaSql = await File.ReadAllTextAsync("schema.sql");
        await using var conn = new NpgsqlConnection(PostgresConnectionString);
        await conn.OpenAsync();
        await using var cmd = new NpgsqlCommand(schemaSql, conn);
        await cmd.ExecuteNonQueryAsync();

        RedisConnection = await ConnectionMultiplexer.ConnectAsync(RedisContainer.GetConnectionString() + ",allowAdmin=true");
    }

    public async Task DisposeAsync()
    {
        if (RedisConnection != null)
        {
            await RedisConnection.DisposeAsync();
        }
        await Task.WhenAll(PostgresContainer.DisposeAsync().AsTask(), RedisContainer.DisposeAsync().AsTask());
    }

    public async Task ClearDatabaseAsync()
    {
        await using var conn = new NpgsqlConnection(PostgresConnectionString);
        await conn.OpenAsync();
        await using var cmd = new NpgsqlCommand("TRUNCATE TABLE Jobs;", conn);
        await cmd.ExecuteNonQueryAsync();
    }
    
    public async Task ClearRedisAsync()
    {
        var endpoints = RedisConnection.GetEndPoints();
        foreach (var endpoint in endpoints)
        {
            var server = RedisConnection.GetServer(endpoint);
            await server.FlushDatabaseAsync();
        }
    }
}

[CollectionDefinition("Integration Tests")]
public class IntegrationTestCollection : ICollectionFixture<IntegrationTestFixture>
{
}
