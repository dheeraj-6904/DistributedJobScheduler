

using DistributedJobScheduler.Core.Infrastructure;
using DistributedJobScheduler.Worker;
using DistributedJobScheduler.Worker.Handlers;
using DistributedJobScheduler.Core.Execution;
using Npgsql;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

// 1. Connection Strings
var pgConnectionString = builder.Configuration.GetConnectionString("Postgres") 
    ?? "Host=localhost;Database=DistributedJobScheduler;Username=postgres;Password=postgres";
var redisConnectionString = builder.Configuration.GetConnectionString("Redis") 
    ?? "localhost:6379";

// 2. Shared Connections
builder.Services.AddSingleton(NpgsqlDataSource.Create(pgConnectionString));
builder.Services.AddSingleton<IConnectionMultiplexer>(sp => ConnectionMultiplexer.Connect(redisConnectionString));

// 3. Repositories & Tracker & Handlers
builder.Services.AddSingleton<IJobRepository, PostgresJobRepository>();
builder.Services.AddSingleton<IQueueProvider, RedisQueueProvider>();
builder.Services.AddSingleton<ActiveJobTracker>(); // Singleton so both services share the exact same instance
builder.Services.AddSingleton<IJobHandlerFactory, DefaultJobHandlerFactory>();
builder.Services.AddTransient<SimulatedJobHandler>();
// 4. Background Services
builder.Services.AddHostedService<HeartbeatService>();
builder.Services.AddHostedService<JobExecutorService>();

// 5. Setup Telemetry
builder.Services.AddJobSchedulerTelemetry();

var app = builder.Build();
app.MapPrometheusScrapingEndpoint();

app.Run();
