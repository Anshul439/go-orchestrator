CREATE TABLE workflow_runs (
    id          SERIAL PRIMARY KEY,
    workflow_name TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'running',
    current_step INT NOT NULL DEFAULT 0,
    total_steps  INT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE jobs
    ADD COLUMN workflow_run_id INT REFERENCES workflow_runs(id),
    ADD COLUMN step_index      INT;
