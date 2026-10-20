using System.Collections.Concurrent;
using DistributedJobScheduler.Core.Domain;
using DistributedJobScheduler.Core.Execution;
using DistributedJobScheduler.Core.Infrastructure;
using DistributedJobScheduler.Worker;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

namespace DistributedJobScheduler.UnitTests.WorkerTests;

public class JobExecutorServiceTests
{
    private static Job CreateQueuedJob(Guid? id = null, int maxAttempts = 3, int attempts = 0)
    {
        var job = new Job
        {
            Id = id ?? Guid.NewGuid(),
            Type = "TestJob",
            Topic = JobTopic.Default,
            Priority = JobPriority.From(5),
            MaxAttempts = maxAttempts,
            ScheduledAt = DateTimeOffset.UtcNow.AddSeconds(-1),
            CreatedAt = DateTimeOffset.UtcNow.AddSeconds(-2)
        };
        typeof(Job).GetProperty(nameof(Job.Status))!.SetValue(job, JobStatus.Queued);
        typeof(Job).GetProperty(nameof(Job.Attempts))!.SetValue(job, attempts);
        return job;
    }

    [Fact]
    public async Task ExecuteAsync_ProcessesJobsSuccessfully()
    {
        var queueMock = new Mock<IQueueProvider>();
        var repoMock = new Mock<IJobRepository>();
        var tracker = new ActiveJobTracker();
        var loggerMock = new Mock<ILogger<JobExecutorService>>();

        var queuedJob = CreateQueuedJob();
        var poppedOnce = 0;

        queueMock.Setup(q => q.PopAsync(It.IsAny<JobTopic[]>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .Returns(async (JobTopic[] _, TimeSpan __, CancellationToken ct) =>
            {
                if (Interlocked.Increment(ref poppedOnce) == 1)
                {
                    return queuedJob.Id;
                }

                await Task.Delay(Timeout.Infinite, ct);
                return null;
            });

        repoMock.Setup(r => r.GetJobAsync(queuedJob.Id, It.IsAny<CancellationToken>()))
            .ReturnsAsync(queuedJob);

        var completedTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        repoMock.Setup(r => r.UpdateJobAsync(It.IsAny<Job>(), It.IsAny<CancellationToken>()))
            .Callback<Job, CancellationToken>((j, _) =>
            {
                if (j.Status == JobStatus.Completed)
                {
                    completedTcs.TrySetResult();
                }
            })
            .Returns(Task.CompletedTask);

        var handlerMock = new Mock<IJobHandler>();
        var factoryMock = new Mock<IJobHandlerFactory>();
        factoryMock.Setup(f => f.GetHandler(It.IsAny<string>())).Returns(handlerMock.Object);

        using var service = new JobExecutorService(queueMock.Object, repoMock.Object, tracker, factoryMock.Object, loggerMock.Object);

        using var cts = new CancellationTokenSource();
        await service.StartAsync(cts.Token);
        await completedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await cts.CancelAsync();
        await service.StopAsync(CancellationToken.None);

        Assert.Equal(JobStatus.Completed, queuedJob.Status);
        repoMock.Verify(r => r.UpdateJobAsync(It.IsAny<Job>(), It.IsAny<CancellationToken>()), Times.Exactly(2));
    }

    [Fact]
    public async Task ExecuteAsync_NeverExceedsConfiguredConcurrency()
    {
        const int concurrencyLimit = 3;
        const int totalJobs = 9;

        var queueMock = new Mock<IQueueProvider>();
        var repoMock = new Mock<IJobRepository>();
        var tracker = new ActiveJobTracker();
        var loggerMock = new Mock<ILogger<JobExecutorService>>();

        var jobs = Enumerable.Range(0, totalJobs).Select(_ => CreateQueuedJob()).ToDictionary(j => j.Id);
        var queue = new ConcurrentQueue<Guid>(jobs.Keys);

        queueMock.Setup(q => q.PopAsync(It.IsAny<JobTopic[]>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .Returns(async (JobTopic[] _, TimeSpan __, CancellationToken ct) =>
            {
                if (queue.TryDequeue(out var nextId))
                {
                    return nextId;
                }

                await Task.Delay(Timeout.Infinite, ct);
                return null;
            });

        repoMock.Setup(r => r.GetJobAsync(It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((Guid id, CancellationToken _) => jobs[id]);

        var completedCount = 0;
        var allCompletedTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        repoMock.Setup(r => r.UpdateJobAsync(It.IsAny<Job>(), It.IsAny<CancellationToken>()))
            .Callback<Job, CancellationToken>((j, _) =>
            {
                if (j.Status == JobStatus.Completed && Interlocked.Increment(ref completedCount) == totalJobs)
                {
                    allCompletedTcs.TrySetResult();
                }
            })
            .Returns(Task.CompletedTask);

        var currentInFlight = 0;
        var peakParallelism = 0;

        var handlerMock = new Mock<IJobHandler>();
        handlerMock.Setup(h => h.HandleAsync(It.IsAny<Job>(), It.IsAny<CancellationToken>()))
            .Returns(async (Job _, CancellationToken ct) =>
            {
                var active = Interlocked.Increment(ref currentInFlight);
                UpdateMax(ref peakParallelism, active);
                try
                {
                    await Task.Delay(80, ct);
                }
                finally
                {
                    Interlocked.Decrement(ref currentInFlight);
                }
            });

        var factoryMock = new Mock<IJobHandlerFactory>();
        factoryMock.Setup(f => f.GetHandler(It.IsAny<string>())).Returns(handlerMock.Object);

        var options = Options.Create(new WorkerOptions { Concurrency = concurrencyLimit });
        using var service = new JobExecutorService(queueMock.Object, repoMock.Object, tracker, factoryMock.Object, loggerMock.Object, options);

        using var cts = new CancellationTokenSource();
        await service.StartAsync(cts.Token);
        await allCompletedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await cts.CancelAsync();
        await service.StopAsync(CancellationToken.None);

        Assert.Equal(concurrencyLimit, peakParallelism);
        Assert.Equal(totalJobs, completedCount);
        Assert.Equal(0, tracker.Count);
    }

    [Fact]
    public async Task ExecuteAsync_FailingHandlerReleasesSlot_AndTransitionsToRetryingOrFailed()
    {
        var queueMock = new Mock<IQueueProvider>();
        var repoMock = new Mock<IJobRepository>();
        var tracker = new ActiveJobTracker();
        var loggerMock = new Mock<ILogger<JobExecutorService>>();

        // Job 1: Attempts=0, MaxAttempts=3 -> fails and transitions to Retrying (Attempts becomes 1 < 3)
        var retryableJob = CreateQueuedJob(maxAttempts: 3, attempts: 0);
        // Job 2: Attempts=2, MaxAttempts=3 -> fails and transitions to Failed (Attempts becomes 3 == 3)
        var terminalFailJob = CreateQueuedJob(maxAttempts: 3, attempts: 2);
        // Job 3: Succeeds -> proves semaphore slot (with Concurrency = 1) was released after both failures
        var succeedingJob = CreateQueuedJob(maxAttempts: 3, attempts: 0);

        var jobs = new Dictionary<Guid, Job>
        {
            [retryableJob.Id] = retryableJob,
            [terminalFailJob.Id] = terminalFailJob,
            [succeedingJob.Id] = succeedingJob
        };

        var queue = new ConcurrentQueue<Guid>(new[] { retryableJob.Id, terminalFailJob.Id, succeedingJob.Id });

        queueMock.Setup(q => q.PopAsync(It.IsAny<JobTopic[]>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .Returns(async (JobTopic[] _, TimeSpan __, CancellationToken ct) =>
            {
                if (queue.TryDequeue(out var nextId))
                {
                    return nextId;
                }

                await Task.Delay(Timeout.Infinite, ct);
                return null;
            });

        repoMock.Setup(r => r.GetJobAsync(It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((Guid id, CancellationToken _) => jobs[id]);

        var thirdJobCompletedTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        repoMock.Setup(r => r.UpdateJobAsync(It.IsAny<Job>(), It.IsAny<CancellationToken>()))
            .Callback<Job, CancellationToken>((j, _) =>
            {
                if (j.Id == succeedingJob.Id && j.Status == JobStatus.Completed)
                {
                    thirdJobCompletedTcs.TrySetResult();
                }
            })
            .Returns(Task.CompletedTask);

        var handlerMock = new Mock<IJobHandler>();
        handlerMock.Setup(h => h.HandleAsync(It.IsAny<Job>(), It.IsAny<CancellationToken>()))
            .Returns((Job job, CancellationToken _) =>
            {
                if (job.Id == retryableJob.Id || job.Id == terminalFailJob.Id)
                {
                    throw new InvalidOperationException("Simulated handler failure");
                }

                return Task.CompletedTask;
            });

        var factoryMock = new Mock<IJobHandlerFactory>();
        factoryMock.Setup(f => f.GetHandler(It.IsAny<string>())).Returns(handlerMock.Object);

        // Concurrency = 1 guarantees that if a failed job does not release its slot, the next job can never run
        var options = Options.Create(new WorkerOptions { Concurrency = 1 });
        using var service = new JobExecutorService(queueMock.Object, repoMock.Object, tracker, factoryMock.Object, loggerMock.Object, options);

        using var cts = new CancellationTokenSource();
        await service.StartAsync(cts.Token);
        await thirdJobCompletedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await cts.CancelAsync();
        await service.StopAsync(CancellationToken.None);

        Assert.Equal(JobStatus.Retrying, retryableJob.Status);
        Assert.NotNull(retryableJob.NextRetryAt);
        Assert.Equal(JobStatus.Failed, terminalFailJob.Status);
        Assert.Equal(JobStatus.Completed, succeedingJob.Status);
        Assert.Equal(0, tracker.Count);
    }

    [Fact]
    public async Task ExecuteAsync_DoesNotPopWhileAllSlotsAreBusy()
    {
        const int concurrencyLimit = 2;

        var queueMock = new Mock<IQueueProvider>();
        var repoMock = new Mock<IJobRepository>();
        var tracker = new ActiveJobTracker();
        var loggerMock = new Mock<ILogger<JobExecutorService>>();

        var job1 = CreateQueuedJob();
        var job2 = CreateQueuedJob();
        var job3 = CreateQueuedJob();
        var jobs = new Dictionary<Guid, Job>
        {
            [job1.Id] = job1,
            [job2.Id] = job2,
            [job3.Id] = job3
        };
        var queue = new ConcurrentQueue<Guid>(new[] { job1.Id, job2.Id, job3.Id });

        var popCalls = 0;
        var thirdPopTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        queueMock.Setup(q => q.PopAsync(It.IsAny<JobTopic[]>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .Returns(async (JobTopic[] _, TimeSpan __, CancellationToken ct) =>
            {
                var callNumber = Interlocked.Increment(ref popCalls);
                if (callNumber == 3)
                {
                    thirdPopTcs.TrySetResult();
                }

                if (queue.TryDequeue(out var nextId))
                {
                    return nextId;
                }

                await Task.Delay(Timeout.Infinite, ct);
                return null;
            });

        repoMock.Setup(r => r.GetJobAsync(It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((Guid id, CancellationToken _) => jobs[id]);

        var bothSlotsOccupiedTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirstSlotTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseAllSlotsTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handlersEntered = 0;

        var handlerMock = new Mock<IJobHandler>();
        handlerMock.Setup(h => h.HandleAsync(It.IsAny<Job>(), It.IsAny<CancellationToken>()))
            .Returns(async (Job job, CancellationToken _) =>
            {
                if (Interlocked.Increment(ref handlersEntered) == concurrencyLimit)
                {
                    bothSlotsOccupiedTcs.TrySetResult();
                }

                if (job.Id == job1.Id)
                {
                    await releaseFirstSlotTcs.Task;
                }
                else
                {
                    await releaseAllSlotsTcs.Task;
                }
            });

        var factoryMock = new Mock<IJobHandlerFactory>();
        factoryMock.Setup(f => f.GetHandler(It.IsAny<string>())).Returns(handlerMock.Object);

        var options = Options.Create(new WorkerOptions { Concurrency = concurrencyLimit });
        using var service = new JobExecutorService(queueMock.Object, repoMock.Object, tracker, factoryMock.Object, loggerMock.Object, options);

        using var cts = new CancellationTokenSource();
        await service.StartAsync(cts.Token);

        // Wait until both slots are occupied in the handler
        await bothSlotsOccupiedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Task.Delay(100);

        // Because the 3rd iteration blocks on _semaphore.WaitAsync BEFORE calling PopAsync, PopAsync must only have been called twice
        Assert.Equal(2, Volatile.Read(ref popCalls));
        Assert.Equal(JobStatus.Queued, job3.Status);

        // Release 1 slot and confirm the 3rd pop immediately occurs
        releaseFirstSlotTcs.TrySetResult();
        await thirdPopTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.True(Volatile.Read(ref popCalls) >= 3);

        releaseAllSlotsTcs.TrySetResult();
        await cts.CancelAsync();
        await service.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task ExecuteAsync_CancellationStopsPopping_AndWaitsForInFlightJobs()
    {
        var queueMock = new Mock<IQueueProvider>();
        var repoMock = new Mock<IJobRepository>();
        var tracker = new ActiveJobTracker();
        var loggerMock = new Mock<ILogger<JobExecutorService>>();

        var job = CreateQueuedJob();
        var popCount = 0;

        queueMock.Setup(q => q.PopAsync(It.IsAny<JobTopic[]>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .Returns(async (JobTopic[] _, TimeSpan __, CancellationToken ct) =>
            {
                if (Interlocked.Increment(ref popCount) == 1)
                {
                    return job.Id;
                }

                await Task.Delay(Timeout.Infinite, ct);
                return null;
            });

        repoMock.Setup(r => r.GetJobAsync(job.Id, It.IsAny<CancellationToken>()))
            .ReturnsAsync(job);

        var handlerStartedTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var finishHandlerTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var handlerMock = new Mock<IJobHandler>();
        handlerMock.Setup(h => h.HandleAsync(job, It.IsAny<CancellationToken>()))
            .Returns(async (Job _, CancellationToken __) =>
            {
                handlerStartedTcs.TrySetResult();
                await finishHandlerTcs.Task;
            });

        var factoryMock = new Mock<IJobHandlerFactory>();
        factoryMock.Setup(f => f.GetHandler(It.IsAny<string>())).Returns(handlerMock.Object);

        var options = Options.Create(new WorkerOptions
        {
            Concurrency = 2,
            ShutdownTimeout = TimeSpan.FromSeconds(5)
        });
        using var service = new JobExecutorService(queueMock.Object, repoMock.Object, tracker, factoryMock.Object, loggerMock.Object, options);

        using var cts = new CancellationTokenSource();
        await service.StartAsync(cts.Token);

        await handlerStartedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Initiate shutdown while the job is still in flight
        await cts.CancelAsync();
        var stopTask = service.StopAsync(CancellationToken.None);

        // Give the loop a moment to exit PopAsync and begin waiting on in-flight tasks
        await Task.Delay(100);
        Assert.False(stopTask.IsCompleted, "StopAsync should wait for the in-flight job to complete before returning.");
        var popsAtShutdown = Volatile.Read(ref popCount);

        // Allow the in-flight handler to finish
        finishHandlerTcs.TrySetResult();
        await stopTask.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal(popsAtShutdown, Volatile.Read(ref popCount));
        Assert.Equal(JobStatus.Completed, job.Status);
        Assert.Equal(0, tracker.Count);
    }

    [Fact]
    public async Task ProcessJobAsync_ExecutesLifecycleWithoutQueueDependency()
    {
        var queueMock = new Mock<IQueueProvider>(MockBehavior.Strict);
        var repoMock = new Mock<IJobRepository>();
        var tracker = new ActiveJobTracker();
        var loggerMock = new Mock<ILogger<JobExecutorService>>();

        var job = CreateQueuedJob();
        repoMock.Setup(r => r.GetJobAsync(job.Id, It.IsAny<CancellationToken>()))
            .ReturnsAsync(job);

        var handlerMock = new Mock<IJobHandler>();
        var factoryMock = new Mock<IJobHandlerFactory>();
        factoryMock.Setup(f => f.GetHandler(job.Type)).Returns(handlerMock.Object);

        using var service = new JobExecutorService(queueMock.Object, repoMock.Object, tracker, factoryMock.Object, loggerMock.Object);

        await service.ProcessJobAsync(job.Id, CancellationToken.None);

        Assert.Equal(JobStatus.Completed, job.Status);
        Assert.Equal(0, tracker.Count);
        queueMock.VerifyNoOtherCalls();
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(-10)]
    public void WorkerOptions_Validation_RejectsZeroOrNegativeConcurrency(int invalidConcurrency)
    {
        var validator = new WorkerOptionsValidator();
        var options = new WorkerOptions { Concurrency = invalidConcurrency };

        var result = validator.Validate(null, options);
        Assert.True(result.Failed);

        Assert.Throws<ArgumentOutOfRangeException>(() => options.Validate());

        var queueMock = new Mock<IQueueProvider>();
        var repoMock = new Mock<IJobRepository>();
        var tracker = new ActiveJobTracker();
        var factoryMock = new Mock<IJobHandlerFactory>();
        var loggerMock = new Mock<ILogger<JobExecutorService>>();

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new JobExecutorService(
                queueMock.Object,
                repoMock.Object,
                tracker,
                factoryMock.Object,
                loggerMock.Object,
                Options.Create(options)));
    }

    [Fact]
    public void WorkerOptions_ConfigurationBinding_HandlesMissingAndOverriddenValues()
    {
        // 1. Missing value -> falls back to DefaultConcurrency (4)
        var emptyConfig = new ConfigurationBuilder().Build();
        var services = new ServiceCollection();
        services.AddSingleton<IConfiguration>(emptyConfig);
        services.AddSingleton<IValidateOptions<WorkerOptions>, WorkerOptionsValidator>();
        services.AddOptions<WorkerOptions>()
            .BindConfiguration(WorkerOptions.SectionName)
            .ValidateOnStart();

        using (var provider = services.BuildServiceProvider())
        {
            var bound = provider.GetRequiredService<IOptions<WorkerOptions>>().Value;
            Assert.Equal(WorkerOptions.DefaultConcurrency, bound.Concurrency);
        }

        // 2. Explicit valid override -> binds properly
        var customConfig = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Worker:Concurrency"] = "8"
            })
            .Build();

        var customServices = new ServiceCollection();
        customServices.AddSingleton<IConfiguration>(customConfig);
        customServices.AddSingleton<IValidateOptions<WorkerOptions>, WorkerOptionsValidator>();
        customServices.AddOptions<WorkerOptions>()
            .BindConfiguration(WorkerOptions.SectionName)
            .ValidateOnStart();

        using (var provider = customServices.BuildServiceProvider())
        {
            var bound = provider.GetRequiredService<IOptions<WorkerOptions>>().Value;
            Assert.Equal(8, bound.Concurrency);
        }

        // 3. Invalid value in configuration -> throws OptionsValidationException
        var invalidConfig = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Worker:Concurrency"] = "0"
            })
            .Build();

        var invalidServices = new ServiceCollection();
        invalidServices.AddSingleton<IConfiguration>(invalidConfig);
        invalidServices.AddSingleton<IValidateOptions<WorkerOptions>, WorkerOptionsValidator>();
        invalidServices.AddOptions<WorkerOptions>()
            .BindConfiguration(WorkerOptions.SectionName)
            .ValidateOnStart();

        using (var provider = invalidServices.BuildServiceProvider())
        {
            Assert.Throws<OptionsValidationException>(() => provider.GetRequiredService<IOptions<WorkerOptions>>().Value);
        }
    }

    private static void UpdateMax(ref int target, int value)
    {
        int current;
        do
        {
            current = Volatile.Read(ref target);
            if (value <= current) return;
        }
        while (Interlocked.CompareExchange(ref target, value, current) != current);
    }
}
