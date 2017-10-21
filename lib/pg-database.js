'use strict';

const Pool		= require('pg').Pool;
const Client	= require('pg').Client;
const PGTable	= require('./pg-table').PGTable;
const PGView	= require('./pg-view').PGView;

const Synchron	= require('synchron').Synchron;

const async		= require('async');
const _			= require('lodash');

let dbCounter = 0;

class PGDatabase {
	constructor(options) {
		this._queryCount = 0;
		this.id = dbCounter++;
		this._pool = null;
		this._client = null;
		this._connected = false;

		this.defaultSchema = 'public';
		this.tableSpace = 'pg_default';

		this._queryPreparators = [];

		// cache each query Object itself, the prop is equal to queryId
		this._queryCache = {};

		// cache each queryId by Table to perform a rerun on change
		// example:
		// _queriesByTable = {
		// 		"public.people": [ // --> client and queryId's that reference to the queryCache
		// 			{ clientId: "cvbcsh", queryId: 56 },
		// 			{ clientId: "cvbcsh", queryId: 34 },
		// 			{ clientId: "324rew", queryId: 442 }
		// 		],
		// 		"public.hobbies": [ ... ]
		// }
		this._queriesByTable = {};

		// hash table with all currently running insert, update and delete's called by
		// the table-object to wait for the notify-message to be done.
		// This is used for an optimistic ui because if there are reactive queries for a table where
		// the insert, update or delete is performed we have to wait for reactivity is done
		this._runningStatements = {};

		this._staledQueries = [];

		// client-connection to listen on notifications
		this._listnerClient = null;

		this._options = {
			enablePooling: ((options && options.enablePooling) || false),
			connectObject: options && options.connect
		};

		// cache all tablenames that have already the trigger to notify changes
		// example:
		// this._installedNotifyTriggers = {
		// 		"public.people": true,
		// 		"public.hobbies": true
		// };
		this._installedNotifyTriggers = {};
	}

	_initNotifyTrigger(tableName, callback) {
		// check if the notify-trigger already installed for this table
		if (this._installedNotifyTriggers[tableName]) return callback();

		// maybe there was a schema explicit defined
		// so replace the tablename's <schema>.<tablename> with an underscore
		// --> tiggername like then: public_mytable_notify, where 'public' is the schema
		var schemaAndTable = tableName.indexOf('.') > -1 ? tableName : this.defaultSchema + '.' + tableName,
			triggerName = schemaAndTable.replace('.', '_') + '_notify',
			triggerNameStmt = schemaAndTable.replace('.', '_') + '_notify_stmt',
			schemaAndTable = schemaAndTable.replace('.', '"."');

		var	sql =  `DROP TRIGGER IF EXISTS "${triggerName}" ON "${schemaAndTable}";

					CREATE TRIGGER "${triggerName}" AFTER INSERT OR UPDATE OR DELETE ON "${schemaAndTable}"
						FOR EACH STATEMENT EXECUTE PROCEDURE core_notify_changes();`;

		this.getConnection((error, connection) => {
			if (error) return callback(error);

			this.query(connection, sql, [], (error, result) => {
				this.releaseConnection(connection);

				if (!error){
					this._installedNotifyTriggers[tableName] = true;
				}

				return callback(error, result);
			});
		});
	}

	_initReactivePg(callback){
		var sql = `
			CREATE TABLE IF NOT EXISTS "${this.defaultSchema}"."core_reactive"
			(
			    query_id	BIGINT 		NOT NULL,
			    row_id		VARCHAR(45) NOT NULL,
			    hash		VARCHAR(45) NOT NULL,

			    CONSTRAINT core_reactive_pkey PRIMARY KEY (query_id, row_id)
			)
			WITH (
			    OIDS = FALSE
			)
			TABLESPACE ${this.tableSpace};

			TRUNCATE "${this.defaultSchema}"."core_reactive";`;

		this.getConnection((error, connection) => {
			if (error) return callback(error);

			this.query(connection, sql, [], (error, result) => {
				this.releaseConnection(connection);
				return callback(error, result);
			});
		});
	}

	_initNotifyFunction(callback) {
		var sql = `
			CREATE OR REPLACE FUNCTION core_notify_changes() RETURNS TRIGGER AS $$
				DECLARE
					client_id			VARCHAR(45);
					statement_id		VARCHAR(45);
					statement_target	VARCHAR(255);
				BEGIN
					BEGIN
						client_id = current_setting('core_settings.client_id')::text;
						statement_id = current_setting('core_settings.statement_id')::text;
						statement_target = current_setting('core_settings.statement_target')::text;
					EXCEPTION WHEN others THEN
						client_id = NULL;
						statement_id = NULL;
						statement_target = NULL;
					END;

					IF ( CONCAT(TG_TABLE_SCHEMA, '.', TG_TABLE_NAME) != statement_target ) THEN
						statement_id = NULL;
					END IF;

					PERFORM pg_notify(
						'core_reactive_event',
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

			$$ LANGUAGE plpgsql;`;

		this.getConnection((error, connection) => {
			if (error) return callback(error);

			this.query(connection, sql, [], (error, result) => {
				this.releaseConnection(connection);
				return callback(error, result);
			});
		});
	}

	_resolveTableName(tableName) {
		// the tableName is always used with the schema
		// like schema.table
		// if there was no schema define we use the default schema
		if (tableName.indexOf('.') == -1){
			return this.defaultSchema + '.' + tableName;
		}
		// the schema was already defined.
		return tableName;
	}

	_cacheQuery(query){
		// cache the query for a later rerun done by the the notification events
		this._queryCache[query.queryId] = query;

		// register the query for each related table it depends on
		_.forEach(query._queryObject.tableDependencies, (tableName) => {
			tableName = this._resolveTableName(tableName);

			if (!this._queriesByTable[tableName]) {
				this._queriesByTable[tableName] = [];
			}
			this._queriesByTable[tableName].push({
				queryId: query.queryId,
				clientId: query._queryObject.options.clientId
			});
		});
	}

	_unCacheQuery(queryId, tableDependencies){
		// remove the references from the _queriesByTable
		_.forEach(tableDependencies, (tableName) => {
			tableName = this._resolveTableName(tableName);

			if (this._queriesByTable[tableName]){
				let getArrIndex = (tableName, queryId) => {
					let arr = this._queriesByTable[tableName];

					for (var i=0, max=arr.length; i<max; i++){
						if (arr[i].queryId == queryId)
							return i;
					}
					return -1;
				}
				var index = getArrIndex(tableName, queryId);
				if (index > -1) {
					this._queriesByTable[tableName].splice(index, 1);
				}
				//this._arrayIndexthis._queriesByTable[tableName].indexOf(query._queryId);
			}
		});

		this._queryCache[queryId] = null;
		//console.log('uncache/delete', queryId, tableDependencies);
		delete this._queryCache[queryId];
	}

	_onNotify(notification){
		if (notification.channel !== 'core_reactive_event') return;

		// work on changes made on the database
		var data = JSON.parse(notification.payload);

		//console.log(data.trigger, data.action);
		//console.log(data.statement_id + ' - ' + data.client_id + ' - ' + data.schema + '.' + data.table);
		//console.log(this._queriesByTable);
		//console.log('onNotify:', data);

		var relatedTable = data.schema + '.' + data.table;
		// get all queries for the related table
		var firstQueries = [];
		var secondQueries = [];
		var max = (this._queriesByTable[relatedTable] && this._queriesByTable[relatedTable].length) || -1;
		//console.log("max relatedTable:", max);
		for (var i=0; i<max; i++){
			var q = this._queriesByTable[relatedTable][i];
			if (q.clientId && q.clientId == data.client_id) {
				firstQueries.push(q);
			} else {
				secondQueries.push(q);
			}
		}

		// check if there are currently reactive queries registered
		// if not, still exit!
		if (firstQueries.length == 0) {
			if (data.statement_id && this._runningStatements[data.statement_id]){
				delete this._runningStatements[data.statement_id];
			}
		}

		// excute all prior/first queries in parallel
		async.each(firstQueries, (q, callback) => {
			this._queryCache[q.queryId].reRun(callback);
		}, (error)=>{
			if (error){
				console.log('FIRST ERROR!!!!!!!!!!!!', error);
			}
			if (data.statement_id && this._runningStatements[data.statement_id]){
				delete this._runningStatements[data.statement_id];
			}

			for(var i=0, max=secondQueries.length; i<max; i++){
				var qid = secondQueries[i].queryId;
				if (this._staledQueries.indexOf(qid) == -1)
					this._staledQueries.push(qid);
			}
		});
	}

	_initStaledQueriesInterval(){
		this._staledRunning = false;
		var staledInterval = setInterval(()=>{
			if (this._staledRunning) return;

			this._staledRunning=true;

			var staledQueries = [];
			while (this._staledQueries.length > 0 && staledQueries.length < 200) {
				staledQueries.push(this._staledQueries.shift());
			}

			async.eachLimit(staledQueries, 50, (qid, callback) => {
				if (this._queryCache[qid]) {
					this._queryCache[qid].reRun(callback);
				}
			}, (error)=>{
				// still ignore errors
				if (error){
					console.log('RUN STALED ERROR!!!!!!!!!!!!', error);
				}
				//console.log('Finished staledQueries', staledQueries.length);
				this._staledRunning = false;
			});
		}, 10);
	}

	connect(callback) {
		var self = this;
		var connectObject = this._options.connectObject;
		callback = callback || function(){};

		async.series([
			(callback) => {
				// make a new connection for the listner
				this._listnerClient = new Client(connectObject);
				this._listnerClient.connect(callback);
			},
			(callback) => {
				// now listen to notifications
				this._listnerClient.query('LISTEN core_reactive_event');
				this._listnerClient.on('notification', this._onNotify.bind(this));
				return callback();
			},
			(callback) => {
				// connect to the pg-database
				if (this._options.enablePooling){
					this._pool = new Pool(connectObject);
					return callback();
				} else {
					this._client = new Client(connectObject);
					this._client.connect((error) => {
						return callback(error);
					});
				}
			},
			(callback) => {
				this._initReactivePg(callback);
			},
			(callback) => {
				// after connecting the listner, check for the neccessary functions to support
				// realtime informations about changes
				this._initNotifyFunction(callback);
			}
		], (error) => {
			if (!error) this._connected = true;

			// enable rerunStaled Queries
			this._initStaledQueriesInterval();

			return callback(error);
		});
	}

	connectSync(){
		var self = this;
		var doConnectSync = new Synchron(function(){
			self.connect((error, result) => {
				if (error) return this.throw(error);

				return this.return(result);
			});
		});

		return doConnectSync();
	}

	end(){
		if (!this._connected) return;

		if (!this._options.enablePooling) {
			this._client.end();
		} else {
			this._pool.end();
		}

		this._listnerClient.query('UNLISTEN core_reactive_event', (error, result) => {
			this._listnerClient.end();
		});
	}

	Table(tableName, options, callback) {
		var __callback = callback,
			__options = options

		if (!_.isString(tableName)) {
			throw new Error('First argument "tableName" must be type of string.');
		}

		if (!_.isFunction(callback) && _.isFunction(options)) {
			// no options supported
			__callback = options;
			__options = {};
		}
		return new PGTable(this, tableName, __options, __callback);
	}

	View(viewName, options, callback){
		var __callback = callback,
			__options = options

		if (!_.isString(viewName)) {
			throw new Error('First argument "viewName" must be type of string.');
		}

		if (!_.isFunction(callback) && _.isFunction(options)) {
			// no options supported
			__callback = options;
			__options = {};
		}

		return new PGView(this, viewName, __options, __callback);
	}

	queryPreparations(preperatorCallback){
		this._queryPreparators.push(preperatorCallback);
	}

	/**
     * @method getConnection
     * @memberOf PGDatabase
     * @locus Server
     *
     * @summary Asynchronous function to get a new connection depending on if you are using pooling or not.
     *
     * If you are not using connection pooling you will receive the current established connection.
     *
     * @param   {Function}      callback    function(error, connection)
     */
	getConnection(callback){
		if (!this._options.enablePooling) {
			return callback(null, this._client);
		}

		return this._pool.connect((err, client, release) => {
			if (err) return callback(err);

			client.release = release;
			return callback(null, client);
		});
	}

	getConnectionSync(){
		if (!this._options.enablePooling) {
			return this._client;
		}
		return this._getConnectionFromPoolSync();
	}

	_getConnectionFromPoolSync(){
		var self = this;
		var connectSync = new Synchron(function(){
			self._pool.connect((err, client) => {
				if (err){
					this.throw(err);
				} else {
					this.return(client);
				}
			});
		});
		return connectSync();
	}

	/**
     * @method releaseConnection
     * @memberOf PGDatabase
     * @locus Server
     *
     * @summary Releases the given connection and returns it to the pool.
     * If you are not using connection-pooling there will be no operation.
     *
     * See method [getConnection()](#PGDatabase-getConnection).
     *
	 * @param {Object} client Specifies the connection that should be released.
	 *
     * @return   {Object} Connection to use for your next query operations.
     */
	releaseConnection(client){
		if (!this._options.enablePooling) {
			return;
		}

		return client.release();
	}

	/**
     * @method wpQuerySync
     * @memberOf PGDatabase
     *
     * @summary Execute the given query and return the results of the query. If a query preparator is defined, this will be invoked before executing the query.
     *
	 * @param  {Object}     connection Specifies the connection to be used for the query. If no connection would be passed the method will manage a new connection by itself.
     * @param  {String}     sql Specifies the SQL command to execute.
     * @param  {Array}      values Specifies the values if you are using placeholders like $1, $2, etc.
     *
     * @return {object} results of the query
     */
	/*wpQuerySync(connection, sql, values){
		// call each global query-preparator
		var queryObj = {
			sql: sql,
			values: values
		};
		for (var i=0, max=this._queryPreparators.length; i<max; i++){
			var preperator = this._queryPreparators[i];
			if (typeof preperator === 'function') {
				preperator.call(this, connection, queryObj);
			}
		}

		return this.querySync(connection, queryObj.sql, queryObj.values);
	}*/

	/**
     * @method wpQuery
     * @memberOf PGDatabase
     *
     * @summary Execute the given query **asynchron** and return the results of the query. If a query preparator is defined, this will be invoked before executing the query.
     *
     * @param  {Object}     connection Specifies the connection to be used for the query. If no connection would be passed the method will manage a new connection by itself.
     * @param  {String}     sql Specifies the SQL command to execute.
     * @param  {Array}      values Specifies the values if you are using placeholders like $1, $2, etc.
	 * @param  {Function}   callback Specifies the callback function. callback(error, results, fields)
     */
	wpQuery(connection, sql, values, callback) {
		// call each global query-preparator
		var queryObj = {
			sql: sql,
			values: values
		};

		for (var i=0, max=this._queryPreparators.length; i<max; i++){
			var preperator = this._queryPreparators[i];
			if (typeof preperator === 'function') {
				preperator.call(this, connection, queryObj);
			}
		}

		this.query(connection, queryObj.sql, queryObj.values, callback);
		//connection.query(sql, values, callback);
	}

	/**
     * @method query
     * @memberOf PGDatabase
     *
     * @summary Execute the given query and return the results.
     *
     * @param  {Object}     connection Specifies the connection to be used for the query. If no connection would be passed the method will manage a new connection by itself.
     * @param  {String}     sql Specifies the SQL command to execute.
     * @param  {Array}      values Specifies the values if you are using placeholders like $1, $2, etc.
	 * @param  {Function}   callback Specifies the callback function. callback(error, results, fields)
     */
	query(connection, sql, values, callback) {
		this._queryCount++;
		//console.log(sql, values);
		connection.query(sql, values, (error, result)=>{
			if (error) console.log('onQuery ERROR:', error);
			return callback(error, result);
		});
	}

	/**
     * @method querySync
     * @memberOf PGDatabase
     *
     * @summary Execute the given sql-query and return the results.
     *
     * @param  {Object}     connection Specifies the connection to be used for the query.
     * @param  {Object}     sql Specifies the SQL commands with all details of the query.
	 * @param  {Object}     values Specifies the values used in the query.
     *
     * @return {Object} Query results or throws an error
     */
	querySync(connection, sql, values){
		var self = this;
		var querySync = new Synchron(function(sql, values){
			self.query(connection, sql, values, (error, results) => {
				if (error) {
					this.throw(error);
				} else {
					this.return(results);
				}
			});
		});

		return querySync(sql, values);
	}
}

module.exports.PGDatabase = PGDatabase;
