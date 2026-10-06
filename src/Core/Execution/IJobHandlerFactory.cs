namespace DistributedJobScheduler.Core.Execution;

public interface IJobHandlerFactory
{
    IJobHandler GetHandler(string jobType);
}
