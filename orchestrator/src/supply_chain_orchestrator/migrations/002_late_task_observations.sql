CREATE UNIQUE INDEX IF NOT EXISTS dispatch_attempts_workflow_attempt_unique_idx
    ON orchestrator_control.dispatch_attempts (workflow_id, attempt_id);

CREATE TABLE orchestrator_control.late_task_observations (
    workflow_id uuid NOT NULL,
    attempt_id uuid NOT NULL,
    task_id text NOT NULL CHECK (length(task_id) BETWEEN 1 AND 512),
    first_observed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (workflow_id, attempt_id, task_id),
    FOREIGN KEY (workflow_id, attempt_id)
        REFERENCES orchestrator_control.dispatch_attempts (workflow_id, attempt_id)
        ON DELETE RESTRICT
);

INSERT INTO orchestrator_control.late_task_observations (
    workflow_id, attempt_id, task_id, first_observed_at
)
SELECT workflow_id, attempt_id, task_id, min(occurred_at)
FROM orchestrator_control.workflow_audit
WHERE event_type = 'late_task_id_observed'
  AND attempt_id IS NOT NULL
  AND task_id IS NOT NULL
GROUP BY workflow_id, attempt_id, task_id
ON CONFLICT (workflow_id, attempt_id, task_id) DO NOTHING;
