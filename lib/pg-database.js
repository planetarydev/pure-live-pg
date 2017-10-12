'use strict';

const Pool		= require('pg').Pool;
const Client	= require('pg').Client;
const PGTable	= require('./pg-table').PGTable;
const PGView	= require('./pg-view').PGView;

const Synchron	= require('synchron').Synchron;

const async		= require('async');
const _			= require('lodash');

class PGDatabase {
	constructor(options) {
		this._pool = null;
		this._client = null;

		this._queryPreparators = [];
		// cache each query Object itself, the prop is equal to queryId
		this._queryCache = {};
		// cache each queryId by Table to perform a rerun on change
		// example:
		// _queriesByTable = {
		// 		"core_users": [12, 34, 78, 4542], --> Id's that reference to the queryCache
		// 		"core_apps": [32, 38, 2378, 23242] --> Id's that reference to the queryCache
		// }
		this._queriesByTable = {};

		// client-connection to listen on notifications
		this._listnerClient = null;

		this._options = {
			enablePooling: ((options && options.enablePooling) || false),
			connectObject: options && options.connect
		};

		// cache all tablenames that have already a trigger installed
		// example:
		// this._installedNotifyTriggers = {
		// 		core_users: true,
		// 		core_roles: true
		// };
		this._installedNotifyTriggers = {};
	}

	_initNotifyTrigger(tableName, callback) {
		// check if the notify-trigger already installed for this table
		if (this._installedNotifyTriggers[tableName]) return callback();

		var triggerName = tableName + '_notify',
			sql =  `DROP TRIGGER IF EXISTS "${triggerName}" ON "${tableName}";

					CREATE TRIGGER "${triggerName}" AFTER INSERT OR UPDATE OR DELETE ON "${tableName}"
						FOR EACH ROW EXECUTE PROCEDURE core_notify_changes();`;

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
			CREATE TABLE IF NOT EXISTS "public"."core_reactive"
			(
			    query_id	BIGINT 		NOT NULL,
			    row_id		VARCHAR(45) NOT NULL,
			    hash		VARCHAR(45) NOT NULL,

			    CONSTRAINT core_reactive_pkey PRIMARY KEY (query_id, row_id)
			)
			WITH (
			    OIDS = FALSE
			)
			TABLESPACE pg_default;

			TRUNCATE "public"."core_reactive";`;

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
					olddata json;
					newdata json;
					notification json;
					notify_id INTEGER;

				BEGIN
					-- Convert the old and new row to JSON, based on the kind of action.
					-- Action = DELETE?             -> OLD row
					-- Action = INSERT or UPDATE?   -> NEW row
					IF (TG_OP = 'DELETE') THEN
						olddata = row_to_json(OLD);
					ELSE
						newdata = row_to_json(NEW);
						IF (TG_OP = 'UPDATE') THEN
							olddata = row_to_json(OLD);
						END IF;
					END IF;

					-- Contruct the notification as a JSON string.
					notification = json_build_object(
						'table', TG_TABLE_NAME,
						'action', TG_OP,
						'oldrow', olddata,
						'newrow', newdata
					);

					INSERT INTO core_notifications(details) VALUES (notification) RETURNING _id INTO notify_id;

					-- Execute pg_notify(channel, notification)
					-- PERFORM pg_notify('core_reactive_event', notify_id::text);
					PERFORM pg_notify(
						'core_reactive_event',
						json_build_object(
							'table', TG_TABLE_NAME,
							'action', TG_OP,
							'notify_id', notify_id
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

	_onNotify(notification){
		if (notification.channel !== 'core_reactive_event') return;

		// work on changes made on the database
		var data = JSON.parse(notification.payload);
		var queryIds = this._queriesByTable[data.table];

		var queryRunners = queryIds.map(id => {
			var self = this,
				queryId = id;
			return function(callback) {
				try {
					self._queryCache[queryId].reRun();
				} catch(e) {
					return callback(e);
				}
				return callback();
			}
		});

		async.parallel(queryRunners, (error) => {
			//console.log(error);
		})

		/*_.forEach(queryIds, (queryId) => {
			this._queryCache[queryId].reRun();
		});*/
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
		], function (error, result) {
			return callback(error, result);
		});
	}

	connectSync(){
		var self = this;
		var doConnectSync = new Synchron(function(){
			self.connect((error, result) => {
				if (error) return this.throw(error);

				this.return(result);
			});
		});

		return doConnectSync();
	}

	end(){
		if (!this._options.enablePooling) {
			this._client.end();
		} else {
			this._pool.end();
		}
	}

	/**
     * @method Table
     * @memberOf PGDatabase
     * @locus Server
     *
     * @summary Creates a new instance of a PGTable object.
     *
     * @param  	{String} 		tableName
     * Name of the table in the postgreSQL database.
     * @param  	{Object} 		[options]
     * Specifies the options for the new table instances. Details see [new PGTable()](Api-PGTable.html#PGTable).
     */
	Table(tableName, options) {
		return new PGTable(this, tableName, options);
	}

	/**
     * @method View
     * @memberOf PGDatabase
     * @locus Anywhere
     *
     * @summary Creates a new instance of a PGView object.
     *
     * @param  	{String} 		tableName
     * Name of the view in the postgreSQL database.
     * @param  	{Object} 		[options]
     * | Option              | Type     | Description                                                                   |
     * |---------------------|----------|-------------------------------------------------------------------------------|
     * | `alias`             | String   | Defines an alias for this table. |
     * | `as`                | String   | See alias |
     * | `idGenerator`       | Function | **Server-side only** A callback function to define an `_id` column if the table does not provide a column named `_id` or `id`. |
     * | `removeUnknownCols` | Boolean  | **Server-side only** If `true` this option will remove all unknown columns used within an object before trying to insert or update a record. |
     *
     */
	View(viewName, options){
		return new PGView(this, viewName, options);
	}

	/**
     * @method queryPreparations
     * @memberOf PGDatabase
     * @locus Server
     *
     * @summary Registers a new callback-function that will be called each time a query will be executed.
     *
     * This will be helpful if you have todo some generall preparations before executing the target query.
     *
     * @after
     * # Using Query-Preparations
     *
     * In our project we are working with a huge count of views. Most of them should be executed in the context of the current user.
     * Here we have the possibility to set a session variable @currentUser that will be consumed from most of the views.
     *
     * To avoid having to set this variable again with each query, and especially to think about it, there is a central way to do this.
     *
     * **Example**
     *
     * ```javascript
     * import { PGDatabase }    from 'meteor/planetarydev:mysql';
     *
     * export const MyDb = new PGDatabase({
     *      enablePooling: true
     * });
     *
     * // connect to the database
     * [...]
     *
     * // declare the query-preparator
     * MyDb.queryPreparations(function(connection, queryObjToRun, currentInvocation) {
     *      if (currentInvocation) {
     *          var userId = currentInvocation && currentInvocation.userId;
     *          // the function runs in the context of the PGDatabase object, so we can use this to access each method
     *          this.querySync(connection, 'SET @currentUser = ?;', [userId]);
     *      }
     * });
     * ```
     *
     * @param  	{Function} 		callback Function to call each time before the query will be executed. `function(connection, queryObject, currentInvocation) { ... }`
     * | Parameter           | Type     | Description                                                                   |
     * |---------------------|----------|-------------------------------------------------------------------------------|
     * | `connection`        | Object   | Specifies the connection for the query to be executed on. |
     * | `queryObject`       | Object   | Specifies the query object that will be executed after the function returns. |
     * | `currentInvocation` | Array    | Specifies the Meteors method- or publication-context that gives access to the `userId` and some other stuff. |
     *
     */
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

		return this._pool.getConnection(callback);
	}

	/**
     * @method getConnectionSync
     * @memberOf PGDatabase
     * @locus Server
     *
     * @summary Returns a new connection depending on if you are using pooling or not.
     *
     * @return   {Object}      Connection to use for your next query operations.
     *
     * @after
     * # Connections
     *
     * To execute one or more database operations like `querySync` you need a valid connection.
     * The `getConnection` and `getConnectionSync` will give you a connection depending on the option if you are using pooling or not.
     * If you are not using connection pooling you will receive the current established connection.
     *
     * > **Important**
     * >
     * > After you finished work be sure that you release the connection you have received.
     *
     * ```javascript
     * import { PGDatabase }    from 'meteor/planetarydev:mysql';
     *
     * export const MyDb = new PGDatabase({
     *      enablePooling: true
     * });
     *
     * // connect to the database
     * [...]
     *
     * // get a new or the established connection
     * var connection = MyDb.getConnectionSync();
     *
     * // do some query
     * MyDb.querySync(connection, 'UPDATE mytable SET mycol = 1 WHERE id = 2');
     * MyDb.querySync(connection, 'DELETE FROM mytable WHERE id = 3');
     *
     * // this is the important thing - release the connection after you have finished work
     * MyDb.releaseConnection(connection);
     * ```
     */
	getConnectionSync(){
		if (!this._options.enablePooling) {
			return this._client;
		}
		return this._getConnectionFromPoolSync();
	}

	/**
     * @method _getConnectionFromPoolSync
     * @memberOf PGDatabase
     * @locus Server
     *
     * @summary Returns a new connection from the pool.
     *
     * @return   {Object}      Connection to use for your next query operations.
     */
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

		return client.release(true);
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
	wpQuerySync(connection, sql, values){
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
	}

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

		this.query(connection, sql, values, callback);
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
		//console.log(sql, values);
		connection.query(sql, values, callback);
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
