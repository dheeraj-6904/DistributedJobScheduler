namespace DistributedJobScheduler.Core.Domain;

/// <summary>
/// Represents the type of compute resource a job requires.
/// Workers are tagged with the topics they can process.
/// Jobs are routed to the Redis queue matching their topic: jobs:ready:{topic}
/// </summary>
public enum JobTopic
{
    /// <summary>
    /// General-purpose jobs with no special resource requirements.
    /// All worker types should be capable of handling these.
    /// Redis key: jobs:ready:default
    /// </summary>
    Default,

    /// <summary>
    /// Jobs requiring significant CPU resources (e.g., data processing, compression).
    /// Should be handled by workers with high CPU capacity.
    /// Redis key: jobs:ready:cpu
    /// </summary>
    CpuIntensive,

    /// <summary>
    /// Jobs requiring GPU resources (e.g., ML inference, video rendering).
    /// Should only be handled by workers with GPU hardware.
    /// Redis key: jobs:ready:gpu
    /// </summary>
    GpuIntensive
}
