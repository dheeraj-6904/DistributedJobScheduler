using DistributedJobScheduler.Core.Execution;

namespace DistributedJobScheduler.IntegrationTests.TestHandlers;

/// <summary>Returns the same pre-configured handler for every job type.</summary>
public sealed class TestJobHandlerFactory : IJobHandlerFactory
{
    private readonly IJobHandler _handler;

    public TestJobHandlerFactory(IJobHandler handler)
    {
        _handler = handler;
    }

    public IJobHandler GetHandler(string jobType) => _handler;
}
