'use strict';

const PGQuery			= require('./pg-query').PGQuery;
const JSONSQLBuilder	= require('json-sql-builder');
const sqlBuilder		= new JSONSQLBuilder('postgreSQL');

const async			= require('async');
const md5 			= require('md5');
const _				= require('lodash')

const REACTIVE_QUERY_TEMPLATE = `
	WITH query AS (
	    @@Query
	),
	hashed_query AS (
	    SELECT
	    	_id::text AS _id,
	    	md5(row_to_json(query.*)::text) AS hash
	   	FROM
	    	query
	),
	changed AS (
	    UPDATE
			core_reactive
	    SET
			hash = hashed_query.hash
	    FROM
	    	hashed_query
	    WHERE
	    	hashed_query.hash != core_reactive.hash
	    AND core_reactive.query_id = @@QueryId
	    AND core_reactive.row_id = hashed_query._id
	    RETURNING
			core_reactive.row_id AS _id
	),
	added AS (
	    INSERT INTO core_reactive (
	    	query_id,
	        row_id,
	        hash
	    )
		SELECT
	    	@@QueryId 	AS query_id,
	    	_id AS row_id,
	    	hashed_query.hash AS hash
	    FROM
	    	hashed_query
	    LEFT JOIN core_reactive
	    	ON core_reactive.query_id = @@QueryId AND core_reactive.row_id = hashed_query._id
	    WHERE
	    	core_reactive.query_id IS NULL
	    RETURNING row_id AS _id
	),
	removed AS (
		DELETE FROM core_reactive
	    WHERE
			core_reactive.query_id = @@QueryId
		AND NOT EXISTS (
	        SELECT 1 FROM hashed_query
	        WHERE
	        	core_reactive.row_id = hashed_query._id
	        AND core_reactive.query_id = @@QueryId
	    )
	    RETURNING core_reactive.row_id AS _id
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
`; // const REACTIVE_QUERY_TEMPLATE

let queryIdCounter = 0;

class PGReactiveQuery extends PGQuery {
	constructor(basetableObject, queryObject) {
		super(basetableObject, queryObject);

		this._queryId = queryIdCounter++;
		this._salt = null;

		this._pagination = {
			key: 'pagination',
			enabled: false,     // will be enabled by run-method on expecting the find-options {$pagination: {...}}
			published: false,   // set to true from the publishPagination-method if pagination is enabled on executeQuery
			currentOffset: null,
			limit: null,
			totalRowCount: null,
			ready: false
		};

		this._lazy = {
			key: 'lazy',
			enabled: false,     // will be enabled by run-method on expecting the find-options {$lazy: {...}}
			published: false,   // set to true from the publishLazy-method if $lazy is enabled on executeQuery
			currentOffset: null,
			inc: null,
			totalRowCount: null,
			ready: false
		};

		this._bindedExecuteQuery = null;
	}

	destroy(done) {
		this._stoped = true;
		var superDestroy = super.destroy.bind(this);

		if (!_.isFunction(done)) {
			done = function(){};
		};

		// delete the hashed reactive data from the core_reactive for this query
		async.series([
			(callback) => {
				this._database.getConnection((error, connection) => {
					if (error) return callback(error);

					// console.log('DELETE FROM core_reactive WHERE query_id = $1', self.queryId);
					this._database.query(connection, 'DELETE FROM core_reactive WHERE query_id = $1', [this._queryId], (error, result) => {
						this._database.releaseConnection(connection);

						if (error) return callback(error);
						return callback();
					});
				});
			},
			(callback) => {
				// delete the published Info documents

				/*var infoCollection = self._getInfoCollection();

				if (self._pagination.published) {
					var docId = 'P' + self._queryObject.invocation._subscriptionId;
					self._queryObject.invocation.removed(infoCollection, docId);
				}
				if (self._lazy.published) {
					var docId = 'L' + self._queryObject.invocation._subscriptionId;
					self._queryObject.invocation.removed(infoCollection, docId);
				}
				*/


				// remove the query from the query-cache
				this._database._unCacheQuery(this._queryId, this._queryObject.tableDependencies);
				superDestroy();
				return callback();
			}
		], function(error){
			if (error) console.log('ERROR DESTROY:', error);
			return done(error);
		});
	}

	stop(){
		// stop the reactivity of this query
		this._stoped = true;
	}

	run(callback) {
		this._running = true;
		callback = callback || function(){};

		let query = {},
			options = this._queryObject.options;

		query.$select = _.extend(query.$select, options);

		// override the basic query-options, they couldnt modified by the user
		query.$select.$from = this._queryObject.tableOrViewName;
		query.$select.$where = this._queryObject.selector

		// remove internal options that should not used for the query itself
		delete query.$select.salt;
		delete query.$select.preparation;
		delete query.$select.clientId;

		// the server-side pagination option exclude the use of offset and limit
		if ((options.$limit || options.$offset) && options.pagination){
			throw new Error('Can\'t use option \'offset\' or \'limit\' together with \'pagination\'. Remove either \'offset\' and \'limit\' or \'pagination\' option.');
		}

		if ((options.$limit || options.$offset) && options.lazyload){
			throw new Error('Can\'t use option \'offset\' or \'limit\' together with \'lazyload\'. Remove either \'offset\' and \'limit\' or \'lazyload\' option.');
		}

		// check for the salt option
		if (options.salt){
			this._salt = this._queryObject.invocation && this._queryObject.invocation._subscriptionId;
		}

		if (options.pagination){
			this._pagination.enabled = true;
			this._pagination.limit = options.pagination.limit || 10;

			query.$select.$offset = this._pagination.currentOffset || options.pagination.offset || 0;
			query.$select.$limit = this._pagination.limit;
			query.calcFoundRows = true;

			// enable for a maybe next run
			this._queryObject.options.calcFoundRows = true;
		}

		if (options.lazyload && options.pagination){
			throw new Error('Can\'t use option \'lazyload\' together with \'pagination\'. Remove either \'lazyload\' or \'pagination\' option.');
		}

		if (options.lazyload){
			this._lazy.enabled = true;
			this._lazy.limit = options.lazyload.limit || 10;
			query.$select.$offset = this._lazy.currentOffset || options.lazyload.offset || 0;
			query.$select.$limit = this._lazy.limit;
			query.calcFoundRows = true;

			// enable for a maybe next run
			this._queryObject.options.calcFoundRows = true;
		}

		let stmt = sqlBuilder.build(query);
		//console.log(stmt.sql, stmt.values);
		this._stmt = {
			// after building the SQL Stamtent we have to inject this into the rective query template
			sql: REACTIVE_QUERY_TEMPLATE.split('@@QueryId').join(this._queryId).replace('@@Query', stmt.sql),
			values: stmt.values
		}

		this.executeQuery(false/*rerun*/, (error, result)=>{
			this._running=false;
			return callback(error, result);
		});
	}

	executeQuery(reRun, callback){
		if (this._pagination.enabled){
			this._pagination.ready = false;
			this.publishPaginationInfo();
		}

		if (this._lazy.enabled){
			this._lazy.ready = false;
			this.publishLazyInfo();
		}

		//console.log('reactive.executeQuery', reRun);
		super.executeQuery(reRun, (error, result) => {
			if (error) return callback(error);

			if (result.rowCount > 0) {
				_.forEach(result.rows, (row) => {
					if (row.action == 'added') {
						return this._added(row._id, row.data);
					}

					if (row.action == 'changed') {
						return this._changed(row._id, row.data);
					}

					if (row.action == 'removed') {
						return this._removed(row._id);
					}
				});
			}

			this.ready();

			/*
			if (this._pagination.enabled) {
				this.removeAllOldPublishedDocuments();
			}

			// publish the results only when not in lazy mode
			// by using the lazy option we will always add the new docs
			// BUT! remember the reRun -> in case of a reRun the lazyload option will get all records
			if (reRun || !this._lazy.enabled){
				this.publishQueryResults(results);
			} else {
				// using the lazyload option we only add new documents
				if (!this._publishedIds) {
					this._publishedIds = {};
				}

				this.iterateDocuments(results, (id, document) => {
					// add new document
					this._added(id, document);
				});
				this.clonePublishedIds();
				this.ready();
			}
			*/

			if (this._pagination.enabled){
				this._pagination.ready = true;
				this.publishPaginationInfo();
			}

			if (this._lazy.enabled){
				this._lazy.ready = true;
				this.publishLazyInfo();
			}

			return callback();
		});
	}

	reRun(callback) {
		if (this._running) {
			if (this._database._staledQueries.indexOf(this._queryId) == -1) {
				this._database._staledQueries.push(this._queryId);
			}
			return callback();
		}

		/*if (this._running) {
			var intv = setInterval(()=>{
				if (!this._running){
					clearInterval(intv);

					this._running = true;
					this.executeQuery(true, (error, result) => {
						this._running = false;
						return callback(error, result);
					});
				}
			}, 50);
			//console.log('out');
			//this._database._staledQueries.push(this._queryId);
			return; // callback();
		}*/
		if (this._stoped) return callback();
		if (this.lazyLoading) return callback();

		this._running = true;

		this.executeQuery(true/*reRun*/, (error, result) => {
			this._running = false;
			return callback(error, result);
		});
	}

	_getInfoCollection(){
		return this._queryObject.collectionName + '->$info';
	}

	_added(id, doc){
		if (this._salt){ // add salt to the document to find the docs from a specific subscription
			doc[this._salt] = true;
		}

		/*if (_.isFunction(this.onAdded)) {
			this.onAdded(id, doc);
		}*/
		this.emit('added', id, doc);
	}

	_changed(id, doc){
		if (this._salt){ // add salt to the document to find the docs from a specific subscription
			doc[this._salt] = true;
		}
		this.emit('changed', id, doc);
	}

	_removed(id){
		this.emit('removed', id);
	}

	/*
	_addedOptimistic(id, doc){
		// register the new id as a published document
		this._publishedIds[id] = md5(JSON.stringify(doc));
		// additionally add it to the oldPublished id's, so when the
		// real - record will be received from the binlogListner we always
		// have the id published and we send a changed-message with maybe Default-values
		// and calculated fields...
		this._oldPublishedIds[id] = this._publishedIds[id];
		this._added(id, doc);
	}

	_removedOptimistic(id){
		// unregister the optimistic/new id
		delete this._publishedIds[id];
		delete this._oldPublishedIds[id];
		this._removed(id);
	}
	*/


	ready(){
		this.emit('state', 'ready');
	}

	/*
	iterateDocuments(documents, callback){
		// Publish each row from the new resultSet
		for (var i=0, max=documents.rows.length; i<max; i++) {
			var doc = documents.rows[i],
				id = doc._id || this._baseTableObject._options._idGenerator(doc);
			if (!id){
				throw new Error('No valid _id', 'Please check if you support a column named "_id" or "id" or use the option "idGenerator" for the table "' + this._baseName + '".');
			}

			// hash the document details for the check of changes, when changes occur by listing to the binlog
			this._publishedIds[id] = md5(JSON.stringify(doc));

			// perform the detail work on the document
			callback(id, doc);
		}
	}
	*/
	/*
	getDeleted(callback){
		// check for old results
		if (!this._oldPublishedIds) {
			return;
		}

		var oldIds = Object.keys(this._oldPublishedIds);

		for (var i=0, max=oldIds.length; i<max; i++){
			// check if the oldId always exists in the new result
			if (!this._publishedIds[oldIds[i]]) {
				// doesnt exists anymore, so remove from the publication
				callback(oldIds[i]);
			}
		}
	}
	*/
	/*
	isNewDocument(id){
		return !(this._oldPublishedIds && this._oldPublishedIds[id]);
	}
	*/
	/*
	hasDocumentChanged(id){
		// check the hash-value for changes on the document with the given id
		return this._oldPublishedIds && (this._oldPublishedIds[id] !== this._publishedIds[id]);
	}
	*/
	/*
	clonePublishedIds(){
		this._oldPublishedIds = JSON.parse(JSON.stringify(this._publishedIds));
	}
	*/

	/*
	// used from the pagination
	removeAllOldPublishedDocuments(){
		if (!this._oldPublishedIds) return;

		var oldIds = Object.keys(this._oldPublishedIds);
		for (var i=0, max=oldIds.length; i<max; i++){
			this._removed(oldIds[i]);
		}

		this._oldPublishedIds = null;
	}
	*/

	publishPaginationInfo(){
		var infoCollection = this._getInfoCollection();
		var docId = 'P' + this._queryObject.invocation._subscriptionId;

		this._pagination.totalPages = Math.ceil(this._pagination.totalRowCount / this._pagination.limit);

		if ((this._pagination.currentOffset || 0) == 0){
			this._pagination.currentPage = 1;
		} else {
			this._pagination.currentPage = (this._pagination.currentOffset || 0) / this._pagination.limit + 1;
		}

		if (!this._pagination.published) {
			this._pagination.published = true;
			this._queryObject.invocation.added(infoCollection, docId, this._pagination);
		} else {
			this._queryObject.invocation.changed(infoCollection, docId, this._pagination);
		}
	}

	publishLazyInfo(){
		var infoCollection = this._getInfoCollection();
		var docId = 'L' + this._queryObject.invocation._subscriptionId;

		if (!this._lazy.published) {
			this._lazy.published = true;
			this._queryObject.invocation.added(infoCollection, docId, this._lazy);
		} else {
			this._queryObject.invocation.changed(infoCollection, docId, this._lazy);
		}
	}

	/*
	publishQueryResults(documents){
		// remove all the previous published id's
		this._publishedIds = {};

		// check all the results
		this.iterateDocuments(documents, (id, document) => {
			// check for a changed row
			if (this.isNewDocument(id)) {
				// add new document
				this._added(id, document);
			} else {
				// document always published before, check for changes
				if (this.hasDocumentChanged(id)){
					// document-details changed
					this._changed(id, document);
				} else {
					// it's the same old row --> do nothing, because it's
					// already published and there are no changes on the document
				}
			}
		});

		// now check for deleted items
		this.getDeleted( (id) => {
			this._removed(id);
		});

		this.ready();

		return this.clonePublishedIds();
	}
	*/
}
module.exports.PGReactiveQuery = PGReactiveQuery;
