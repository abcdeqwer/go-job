-- Preserve the operator-facing description supplied by each live executor. It is metadata,
-- not routing authority: handler_key remains the only dispatch key.
ALTER TABLE job_executor_handler
    ADD COLUMN description VARCHAR(512) NOT NULL DEFAULT '' AFTER handler_key;

UPDATE schema_identity
SET schema_version = '3'
WHERE lock_row = 1 AND schema_version = '2';
