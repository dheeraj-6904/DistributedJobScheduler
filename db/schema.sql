CREATE TABLE IF NOT EXISTS Jobs (
    Id UUID PRIMARY KEY,
    Type VARCHAR(255) NOT NULL,
    Topic VARCHAR(255) NOT NULL,
    Payload JSONB NOT NULL,
    Priority INT NOT NULL,
    Status VARCHAR(50) NOT NULL,
    Attempts INT NOT NULL DEFAULT 0,
    MaxAttempts INT NOT NULL,
    ScheduledAt TIMESTAMPTZ NOT NULL,
    NextRetryAt TIMESTAMPTZ,
    WorkerId VARCHAR(255),
    LeaseUntil TIMESTAMPTZ,
    CreatedAt TIMESTAMPTZ NOT NULL,
    StartedAt TIMESTAMPTZ,
    CompletedAt TIMESTAMPTZ,
    IdempotencyKey VARCHAR(255) UNIQUE,
    Result JSONB,
    Error TEXT
);

-- Index for the Scheduler's fast SKIP LOCKED query
-- We only index Pending jobs because those are the only ones the scheduler cares about
CREATE INDEX IF NOT EXISTS IDX_Jobs_Scheduler 
ON Jobs (Priority ASC, ScheduledAt ASC) 
WHERE Status = 'Pending';

-- Index for the Lease Sweeper to quickly find dead workers
CREATE INDEX IF NOT EXISTS IDX_Jobs_LeaseSweeper 
ON Jobs (LeaseUntil) 
WHERE Status = 'Running';
