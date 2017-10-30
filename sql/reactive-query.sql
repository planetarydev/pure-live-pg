WITH query AS (
	@@Query
),
hashed_query AS (
	SELECT
		_id::text 						AS _id,
		md5(row_to_json(query.*)::text) AS hash
	FROM
		query
),
changed AS (
	UPDATE
		"@@ownSchema".reactivity
	SET
		hash = hashed_query.hash
	FROM
		hashed_query
	WHERE
		hashed_query.hash != "@@ownSchema".reactivity.hash
	AND "@@ownSchema".reactivity.query_id = '@@QueryId'
	AND "@@ownSchema".reactivity.row_id = hashed_query._id
	RETURNING
		"@@ownSchema".reactivity.row_id AS _id
),
added AS (
	INSERT INTO "@@ownSchema".reactivity (
		query_id,
		row_id,
		hash
	)
	SELECT
		'@@QueryId' 		AS query_id,
		_id 				AS row_id,
		hashed_query.hash 	AS hash
	FROM
		hashed_query
	LEFT JOIN "@@ownSchema".reactivity
		ON "@@ownSchema".reactivity.query_id = '@@QueryId' AND "@@ownSchema".reactivity.row_id = hashed_query._id
	WHERE
		"@@ownSchema".reactivity.query_id IS NULL
	RETURNING row_id AS _id
),
removed AS (
	DELETE FROM "@@ownSchema".reactivity
	WHERE
		"@@ownSchema".reactivity.query_id = '@@QueryId'
	AND NOT EXISTS (
		SELECT 1 FROM hashed_query
		WHERE
			"@@ownSchema".reactivity.row_id = hashed_query._id
		AND "@@ownSchema".reactivity.query_id = '@@QueryId'
	)
	RETURNING "@@ownSchema".reactivity.row_id AS _id
)
-- output data
(
	SELECT
		'changed'				AS "action",
		changed._id				AS "_id",
		row_to_json(query.*)	AS "data"
	FROM changed
	INNER JOIN query ON changed._id = query._id::text
)
UNION ALL
(
	SELECT
		'added'					AS "action",
		added._id				AS "_id",
		row_to_json(query.*)	AS "data"
	FROM added
	INNER JOIN query ON added._id = query._id::text
)
UNION ALL
(
	SELECT
		'removed'		AS "action",
		removed._id 	AS "_id",
		NULL			AS "data"
	FROM removed
)
