CREATE TABLE orchestrator_control.workflow_ledger (
    workflow_id uuid PRIMARY KEY,
    thread_id uuid NOT NULL UNIQUE,
    request_hash char(64) NOT NULL CHECK (request_hash ~ '^[0-9a-f]{64}$'),
    operation text NOT NULL CHECK (operation IN ('assess', 'inbound_lookup')),
    sku text NOT NULL,
    location_id text NOT NULL,
    trace_id text NOT NULL,
    correlation_id text NOT NULL,
    lifecycle_state text NOT NULL CHECK (
        lifecycle_state IN (
            'CREATED',
            'RUNNING',
            'SUBMISSION_UNKNOWN',
            'CANCELLATION_REQUESTED',
            'COMPLETED',
            'FAILED',
            'CANCELLED'
        )
    ),
    dispatch_state text NOT NULL CHECK (
        dispatch_state IN (
            'NOT_STARTED',
            'INTENT_COMMITTED',
            'SUBMISSION_UNKNOWN',
            'TASK_RECORDED',
            'COMPLETED',
            'FAILED',
            'CANCELLED'
        )
    ),
    cancellation_requested_at timestamptz,
    cancellation_completed_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (
        (cancellation_requested_at IS NULL)
        OR lifecycle_state IN (
            'CANCELLATION_REQUESTED',
            'COMPLETED',
            'FAILED',
            'CANCELLED'
        )
    )
);

CREATE TABLE orchestrator_control.dispatch_attempts (
    attempt_id uuid PRIMARY KEY,
    workflow_id uuid NOT NULL UNIQUE
        REFERENCES orchestrator_control.workflow_ledger (workflow_id) ON DELETE RESTRICT,
    attempt_number integer NOT NULL CHECK (attempt_number > 0),
    dispatch_state text NOT NULL CHECK (
        dispatch_state IN (
            'INTENT_COMMITTED',
            'SUBMISSION_UNKNOWN',
            'TASK_RECORDED',
            'COMPLETED',
            'FAILED',
            'CANCELLED'
        )
    ),
    owner_id text NOT NULL,
    fencing_token bigint NOT NULL CHECK (fencing_token > 0),
    lease_expires_at timestamptz NOT NULL,
    task_id text,
    cancellation_requested_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    error_code text CHECK (
        error_code IS NULL OR error_code ~ '^[A-Z][A-Z0-9_]{0,63}$'
    ),
    UNIQUE (workflow_id, attempt_number),
    CHECK (task_id IS NULL OR length(task_id) BETWEEN 1 AND 512)
);

CREATE INDEX dispatch_attempts_expired_lease_idx
    ON orchestrator_control.dispatch_attempts (lease_expires_at)
    WHERE dispatch_state IN (
        'INTENT_COMMITTED',
        'SUBMISSION_UNKNOWN',
        'TASK_RECORDED'
    );

CREATE TABLE orchestrator_control.workflow_audit (
    audit_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    workflow_id uuid NOT NULL
        REFERENCES orchestrator_control.workflow_ledger (workflow_id) ON DELETE RESTRICT,
    attempt_id uuid
        REFERENCES orchestrator_control.dispatch_attempts (attempt_id) ON DELETE RESTRICT,
    actor_id text NOT NULL,
    event_type text NOT NULL CHECK (event_type ~ '^[a-z][a-z0-9_]{0,63}$'),
    from_lifecycle_state text,
    to_lifecycle_state text,
    from_dispatch_state text,
    to_dispatch_state text,
    fencing_token bigint,
    task_id text,
    reason_code text CHECK (
        reason_code IS NULL OR reason_code ~ '^[A-Z][A-Z0-9_]{0,63}$'
    ),
    occurred_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX workflow_audit_workflow_time_idx
    ON orchestrator_control.workflow_audit (workflow_id, occurred_at, audit_id);
