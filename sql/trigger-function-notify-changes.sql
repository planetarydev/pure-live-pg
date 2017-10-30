CREATE OR REPLACE FUNCTION "@@ownSchema".notify_changes() RETURNS TRIGGER AS $$
	DECLARE
		client_id			TEXT;
		statement_id		TEXT;
		statement_target	TEXT;
	BEGIN
		BEGIN
			client_id = current_setting('pg_reactive_settings.client_id')::text;
			statement_id = current_setting('pg_reactive_settings.statement_id')::text;
			statement_target = current_setting('pg_reactive_settings.statement_target')::text;
		EXCEPTION WHEN others THEN
			client_id = NULL;
			statement_id = NULL;
			statement_target = NULL;
		END;

		IF ( CONCAT(TG_TABLE_SCHEMA, '.', TG_TABLE_NAME) != statement_target ) THEN
			statement_id = NULL;
		END IF;

		PERFORM pg_notify(
			'@@notifyEventName',
			json_build_object(
				'type', 'STATEMENT',
				'schema', TG_TABLE_SCHEMA,
				'table', TG_TABLE_NAME,
				'action', TG_OP,
				'client_id', client_id,
				'statement_id', statement_id,
				'statement_target', statement_target
			)::text
		);

		-- Result is ignored since this is an AFTER trigger
		RETURN NULL;
	END;

$$ LANGUAGE plpgsql;
