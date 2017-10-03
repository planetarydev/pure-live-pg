'use strict';

const { Pool, Client } 	= require('pg');
const { PGTable }		= require('./pg-table');
const { PGView }		= require('./pg-view');

const { Synchron }		= require('synchron');

const async			= require('async');
const _				= require('lodash');

/**
 * PGDatabase
 * @summary Use the `PGDatabase` to create a new database instance which ...
 * @namespace PGDatabase
 * @locus Anywhere
 * @importFromPackage Import { PGDatabase } from 'planetarydev:pg'
 *
 * @before
 * # PGDatabase
 * <div class="sub-title">
 * 		Documentation of the Client-/ and Server-side API
 * </div>
 * On the following you can see all functionality given by the `pure-postgres` package.
 * The package provides you all neccessary objects and methods to work well with your postgreSQL-database.
 *
 * @param  	{Object} 		[options]
 * Options for the new instance of the PGDatabase.
 * | Option            | Type    | Description                                                                   |
 * |-------------------|---------|-------------------------------------------------------------------------------|
 * | `enablePooling`   | Boolean | You can connect to the database with a single connection or use the connection-pooling. Further informations on that see the official [docs for node-postgres](https://node-postgres.com/api/pool).|
 * | `connectObject`   | Object  | The detailed connection-object with host, port, database, username and password to connect to the database.|
 *
 */
class PGDatabase {
	constructor(options) {
		this._pool = null;
		this._client = null;
		this._listners = [];
		this._queryPreparators = [];
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
	}

	_initNotifyTrigger(tableName, callback) {
		var triggerName = tableName + '_notify',
			sql =  `DROP TRIGGER IF EXISTS "${triggerName}" ON "${tableName}";

					CREATE TRIGGER "${triggerName}" AFTER INSERT OR UPDATE OR DELETE ON "${tableName}"
						FOR EACH ROW EXECUTE PROCEDURE core_notify_changes();`;

		this.getConnection((error, connection) => {
			if (error) return callback(error);

			this.query(connection, sql, [], (error, result) => {
				this.releaseConnection(connection);
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

			TRUNCATE "public"."core_reactive"`;

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
					-- Convert the old or new row to JSON, based on the kind of action.
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

	/**
     * @method connect
     * @memberOf PGDatabase
     * @locus Server
     *
     * @summary Connect to the given postgres-database.
     *
     * @param  	{Object} 		connectObject
     * Connection Object with all neccessary informations to connect to database.
     *
     * The connection-object is the same as you are using with the nodejs mysql package to connect to your database.
     * The options listed below will show the most important options.
     *
     * For a full list of all options have a look at [https://www.npmjs.com/package/mysql#connection-options](https://www.npmjs.com/package/mysql#connection-options).
     *
     * | Option             | Type   | Description                                                                   |
     * |--------------------|--------|-------------------------------------------------------------------------------|
     * | `host`             | String | Host where the mysql-server is installed |
     * | `database`         | String | Name of the database/schema |
     * | `user`             | String | Username to connect with |
     * | `password`         | String | Password of the user |
     * | `password`         | String | Password of the user |
     * | `connectionLimit`  | Number | Limt of connections if you are using pooling. Default 100. |
     *
     * @after
     *
     * **Example**
     *
     * Shows the connect to a database.
     * ```javascript
     * import { PGDatabase }    from 'meteor/planetarydev:mysql';
     *
     * export const MyDb = new PGDatabase({
     *      enablePooling: true
     * });
     *
     * MyDb.connect({
     *      host     : 'localhost',
     *      user     : 'root',
     *      password : 'your password',
     *      database : 'your database'
     * });
     * ```
     *
     */
	connect(callback) {
		var self = this;
		var connectObject = this._options.connectObject;
		callback = callback || function(){};

		async.series([
			function(callback){
				// make a new connection for the listner
				self._listnerClient = new Client(connectObject);
				self._listnerClient.connect(callback);
			},
			function(callback){
				// now listen to notifications
				self._listnerClient.query('LISTEN core_reactive_event');
				self._listnerClient.on('notification', function(notification){
					// work on changes made on the database
					var data = JSON.parse(notification.payload);
					var queryIds = self._queriesByTable[data.table];
					//console.log(notification);

					_.forEach(queryIds, function(queryId) {
						console.log('ReRUN:', queryId);

						var query = self._queryCache[queryId];
						query.reRun();
					});
				});
				return callback();
			},
			function(callback){
				// connect to the pg-database
				if (self._options.enablePooling){
					self._pool = new Pool(connectObject);
					return callback();
				} else {
					self._client = new Client(connectObject);
					self._client.connect((error) => {
						return callback(error);
					});
				}
			},
			function(callback) {
				self._initReactivePg(callback);
			},
			function(callback) {
				// after connecting the listner, check for the neccessary functions to support
				// realtime informations about changes
				self._initNotifyFunction(callback);
			}
		], function (error, result){
			return callback(error, result);
		});
	}

	/**
     * @method end
     * @memberOf PGDatabase
     * @locus Server
     *
     * @summary Ends all connections to the database.
     *
     * > From the docs of the NPM-Package mysql
     *
     * > _When you are done using the connection or pool, you have to end all the connections or the Node.js event loop will stay active until the connections are_
     * > _closed by the postgres server. This is typically done if the pool is used in a script or when trying to gracefully shutdown a server._
     * > _To end all the connections in the pool, use the end method on the pool._
     *
     *
     * @param  	{Function} 		[callback] function callback(error)
     * The end method takes an optional callback that you can use to know once all the connections have ended.
     * The connections end gracefully, so all pending queries will still complete and the time to end the pool will vary.
     *
     * @after
     * ## Terminating connections
     *
     * There are two ways to end a connection. Terminating a connection gracefully is done by calling the end() method:
     *
     * ```javascript
     * MyDb.end(function(err) {
     *      // The connection is terminated now
     * });
     * ```
     * This will make sure all previously enqueued queries are still before sending a `COM_QUIT` packet to the postgreSQL server. If a
     * fatal error occurs before the `COM_QUIT` packet can be sent, an err argument will be provided to the callback,
     * but the connection will be terminated regardless of that.
     *
     * An alternative way to end the connection is to call the `destroy()` method. This will cause an immediate termination of the underlying socket.
     * Additionally destroy() guarantees that no more events or callbacks will be triggered for the connection.
     *
     * ```javascript
     * MyDb.destroy();
     * ```
     *
     * Unlike end() the destroy() method does not take a callback argument.
     */
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
     * @method _registerListner
     * @memberOf PGDatabase
     * @locus Server
     *
     * @summary Registers a new callback-function for the mysql binlog which is called every time changes detect on the mysql binlog.
     *
     * @param  	{Function} 		callback Function to call each time the binlog got new data. `function(eventName, tablename, rows, evt) { ... }`
     * | Parameter           | Type     | Description                                                                   |
     * |---------------------|----------|-------------------------------------------------------------------------------|
     * | `eventName`         | String   | Specifies the kind of change. The eventName could be `writerows`, `updaterows` or `deleterows` |
     * | `tableName`         | String   | Name of the table where the changes took place |
     * | `rows`              | Array    | An Array of Row-Objects with the old and new datarows. |
     * | `evt`               | Boolean  | Original event-Object from the zongji-binlog listner |
     *
     */
	_registerListner(callback){
		this._listners.push(callback);
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
