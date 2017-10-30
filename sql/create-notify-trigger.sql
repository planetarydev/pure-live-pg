DROP TRIGGER IF EXISTS "@@triggerName" ON "@@schemaAndTable";

CREATE TRIGGER "@@triggerName" AFTER INSERT OR UPDATE OR DELETE ON "@@schemaAndTable"
	FOR EACH STATEMENT EXECUTE PROCEDURE "@@ownSchema".notify_changes();
