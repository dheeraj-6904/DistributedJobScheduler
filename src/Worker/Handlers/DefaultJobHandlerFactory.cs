using Microsoft.Extensions.DependencyInjection;
using DistributedJobScheduler.Core.Execution;

namespace DistributedJobScheduler.Worker.Handlers;

public sealed class DefaultJobHandlerFactory : IJobHandlerFactory
{
    private readonly IServiceProvider _serviceProvider;

    public DefaultJobHandlerFactory(IServiceProvider serviceProvider)
    {
        _serviceProvider = serviceProvider;
    }

    public IJobHandler GetHandler(string jobType)
    {
        // For now, we resolve the default simulated handler for all jobs.
        // In a real system, you would switch on jobType to resolve different IJobHandler implementations.
        return _serviceProvider.GetRequiredService<SimulatedJobHandler>();
    }
}
