-- Index the terminal business timestamp used by bounded execution-history retention.
-- Status leads because cleanup admits only four terminal states and uses a different cutoff
-- for success than for the longer-lived audit outcomes.
ALTER TABLE job_execution
    ADD KEY idx_job_execution_retention (status, finished_at, id);

UPDATE schema_identity
SET schema_version = '2'
WHERE lock_row = 1 AND schema_version = '1';
