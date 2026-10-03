using Microsoft.Extensions.DependencyInjection;
using OpenTelemetry.Metrics;

namespace DistributedJobScheduler.Core.Infrastructure;

public static class TelemetryExtensions
{
    public static void AddJobSchedulerTelemetry(this IServiceCollection services)
    {
        services.AddOpenTelemetry()
            .WithMetrics(metrics =>
            {
                metrics.AddAspNetCoreInstrumentation() // Captures HTTP request metrics
                       .AddMeter("Npgsql") // Captures Postgres DB metrics
                       .AddMeter("DistributedJobScheduler") // Captures our custom JobMetrics
                       .AddPrometheusExporter(); // Exposes the /metrics endpoint
            });
    }
}
