-- Append-only enforcement for the workflow audit trail.
-- Applied by the privileged migrator (SET ROLE orchestrator_owner) in the same
-- transaction that records this migration in schema_migrations.
CREATE OR REPLACE FUNCTION orchestrator_control.reject_audit_mutation()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $function$
BEGIN
    RAISE EXCEPTION 'workflow_audit is append-only'
        USING ERRCODE = 'insufficient_privilege';
END;
$function$;

DROP TRIGGER IF EXISTS workflow_audit_append_only_row
    ON orchestrator_control.workflow_audit;
CREATE TRIGGER workflow_audit_append_only_row
    BEFORE UPDATE OR DELETE ON orchestrator_control.workflow_audit
    FOR EACH ROW EXECUTE FUNCTION orchestrator_control.reject_audit_mutation();

DROP TRIGGER IF EXISTS workflow_audit_append_only_truncate
    ON orchestrator_control.workflow_audit;
CREATE TRIGGER workflow_audit_append_only_truncate
    BEFORE TRUNCATE ON orchestrator_control.workflow_audit
    FOR EACH STATEMENT EXECUTE FUNCTION orchestrator_control.reject_audit_mutation();
