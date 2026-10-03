using DistributedJobScheduler.Core.Domain;
using DistributedJobScheduler.Core.Infrastructure;
using Microsoft.AspNetCore.Mvc;
using Npgsql;

var builder = WebApplication.CreateBuilder(args);

// 1. Connection String
var pgConnectionString = builder.Configuration.GetConnectionString("Postgres") 
    ?? "Host=localhost;Database=DistributedJobScheduler;Username=postgres;Password=postgres";

// 2. DI Setup (API doesn't need Redis, it only talks to Postgres)
builder.Services.AddSingleton(NpgsqlDataSource.Create(pgConnectionString));
builder.Services.AddSingleton<IJobRepository, PostgresJobRepository>();

builder.Services.AddJobSchedulerTelemetry();

var app = builder.Build();

app.MapPrometheusScrapingEndpoint();

// 3. Endpoints

// GET /jobs/{id}
app.MapGet("/jobs/{id:guid}", async (Guid id, IJobRepository repo, CancellationToken token) =>
{
    var job = await repo.GetJobAsync(id, token);
    return job is not null ? Results.Ok(job) : Results.NotFound();
});

// POST /jobs
app.MapPost("/jobs", async ([FromBody] CreateJobRequest req, IJobRepository repo, CancellationToken token) =>
{
    // Ensure priority is valid
    if (!JobPriority.TryCreate(req.Priority, out var priority))
    {
        return Results.BadRequest(new { Error = "Priority must be between 0 (highest) and 10 (lowest)." });
    }

    var job = new Job
    {
        Id = Guid.NewGuid(),
        Type = req.Type,
        Topic = Enum.TryParse<JobTopic>(req.Topic, true, out var t) ? t : JobTopic.Default,
        Payload = req.Payload,
        Priority = priority,
        MaxAttempts = req.MaxAttempts > 0 ? req.MaxAttempts : 3,
        ScheduledAt = req.ScheduledAt ?? DateTimeOffset.UtcNow,
        CreatedAt = DateTimeOffset.UtcNow,
        IdempotencyKey = req.IdempotencyKey
    };

    var created = await repo.CreateJobAsync(job, token);

    if (!created)
    {
        return Results.Conflict(new { Error = $"A job with IdempotencyKey '{req.IdempotencyKey}' already exists." });
    }

    return Results.Created($"/jobs/{job.Id}", job);
});

app.Run();

// DTO for incoming requests
public record CreateJobRequest(
    string Type, 
    string Payload, 
    string Topic = "Default", 
    int Priority = 5, 
    int MaxAttempts = 3,
    DateTimeOffset? ScheduledAt = null,
    string? IdempotencyKey = null
);
