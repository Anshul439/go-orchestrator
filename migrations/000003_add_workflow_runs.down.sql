ALTER TABLE jobs
    DROP COLUMN step_index,
    DROP COLUMN workflow_run_id;

DROP TABLE workflow_runs;
