using DistributedJobScheduler.Core.Infrastructure;
using DistributedJobScheduler.Scheduler;
using Npgsql;
using StackExchange.Redis;

var builder = Host.CreateApplicationBuilder(args);

// 1. Read connection strings (fallback to localhost defaults for easy local running)
var pgConnectionString = builder.Configuration.GetConnectionString("Postgres") 
    ?? "Host=localhost;Database=DistributedJobScheduler;Username=postgres;Password=postgres";
var redisConnectionString = builder.Configuration.GetConnectionString("Redis") 
    ?? "localhost:6379";

// 2. Register Database & Redis connection objects as Singletons
builder.Services.AddSingleton(NpgsqlDataSource.Create(pgConnectionString));
builder.Services.AddSingleton<IConnectionMultiplexer>(sp => ConnectionMultiplexer.Connect(redisConnectionString));

// 3. Register our Core Repositories
builder.Services.AddSingleton<IJobRepository, PostgresJobRepository>();
builder.Services.AddSingleton<IQueueProvider, RedisQueueProvider>();

// 4. Register our two Background Loops
builder.Services.AddHostedService<DispatcherService>();
builder.Services.AddHostedService<LeaseSweeperService>();

var host = builder.Build();
host.Run();
