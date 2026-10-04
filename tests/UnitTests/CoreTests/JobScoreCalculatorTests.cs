using DistributedJobScheduler.Core.Domain;
using DistributedJobScheduler.Core.Scheduling;
using Xunit;

namespace DistributedJobScheduler.UnitTests.CoreTests;

public class JobScoreCalculatorTests
{
    [Fact]
    public void Calculate_AppliesPriorityAgingCorrectly()
    {
        var scheduledAt = DateTimeOffset.UtcNow;
        var p0 = JobPriority.From(0);
        var p5 = JobPriority.From(5);

        var score0 = JobScoreCalculator.Calculate(p0, scheduledAt, 600);
        var score5 = JobScoreCalculator.Calculate(p5, scheduledAt, 600);

        Assert.True(score0 < score5);
        Assert.Equal(score0 + 3000, score5);
    }
}
