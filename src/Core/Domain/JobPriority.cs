namespace DistributedJobScheduler.Core.Domain;

/// <summary>
/// Represents the priority of a job. Lower numeric value = higher priority.
/// Valid range: 0 (highest) to 10 (lowest).
/// </summary>
/// <remarks>
/// Priority 0 will always be processed before Priority 10.
/// Jobs with the same priority are ordered by ScheduledAt (earliest first),
/// then by JobId as a deterministic tie-breaker.
/// Priority aging is applied via the Redis score formula to prevent starvation.
/// </remarks>
public readonly struct JobPriority : IComparable<JobPriority>, IEquatable<JobPriority>
{
    public const int MinValue = 0;
    public const int MaxValue = 10;

    public int Value { get; }

    private JobPriority(int value) => Value = value;

    /// <summary>
    /// Creates a JobPriority from a raw integer.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">
    /// Thrown if value is outside the 0–10 range.
    /// Use <see cref="TryCreate"/> to avoid exceptions.
    /// </exception>
    public static JobPriority From(int value)
    {
        if (value < MinValue || value > MaxValue)
            throw new ArgumentOutOfRangeException(
                nameof(value),
                $"Priority must be between {MinValue} (highest) and {MaxValue} (lowest). Got: {value}.");

        return new JobPriority(value);
    }

    /// <summary>
    /// Tries to create a JobPriority without throwing an exception.
    /// Returns false if the value is out of range.
    /// </summary>
    public static bool TryCreate(int value, out JobPriority priority)
    {
        if (value < MinValue || value > MaxValue)
        {
            priority = default;
            return false;
        }

        priority = new JobPriority(value);
        return true;
    }

    // Predefined convenience priorities
    public static readonly JobPriority Highest = new(0);
    public static readonly JobPriority High     = new(2);
    public static readonly JobPriority Normal   = new(5);
    public static readonly JobPriority Low      = new(8);
    public static readonly JobPriority Lowest   = new(10);

    // Lower value = higher priority, so comparison is inverted for natural ordering
    public int CompareTo(JobPriority other) => Value.CompareTo(other.Value);

    public bool Equals(JobPriority other) => Value == other.Value;
    public override bool Equals(object? obj) => obj is JobPriority p && Equals(p);
    public override int GetHashCode() => Value.GetHashCode();
    public override string ToString() => Value.ToString();

    public static bool operator ==(JobPriority left, JobPriority right) => left.Equals(right);
    public static bool operator !=(JobPriority left, JobPriority right) => !left.Equals(right);
    public static bool operator <(JobPriority left, JobPriority right)  => left.Value < right.Value;
    public static bool operator >(JobPriority left, JobPriority right)  => left.Value > right.Value;

    // Allow implicit conversion from int for ergonomic use: JobPriority p = 5;
    public static implicit operator int(JobPriority p) => p.Value;
}
