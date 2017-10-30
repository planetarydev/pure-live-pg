CREATE TABLE IF NOT EXISTS "@@ownSchema"."reactivity"
(
	query_id	TEXT 	NOT NULL,
	row_id		TEXT	NOT NULL,
	hash		TEXT	NOT NULL,

	CONSTRAINT pk_reactivity_query_id PRIMARY KEY (query_id, row_id)
)
WITH (
	OIDS = FALSE
)
TABLESPACE "@@tableSpace";

TRUNCATE "@@ownSchema"."reactivity";
