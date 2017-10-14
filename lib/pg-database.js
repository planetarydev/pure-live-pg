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
		this._connected = false;

		this.defaultSchema = 'public';
		this.tableSpace = 'pg_default';

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

		// maybe there was a schema explicit defined
		// so replace the tablename's <schema>.<tablename> with an underscore
		// --> tiggername like then: public_mytable_notify, where 'public' is the schema
		var triggerName = tableName.replace('.', '_') + '_notify',
			sql =  `DROP TRIGGER IF EXISTS "${triggerName}" ON "${tableName.replace('.', '"."')}";

					CREATE TRIGGER "${triggerName}" AFTER INSERT OR UPDATE OR DELETE ON "${tableName.replace('.', '"."')}"
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
					id				VARCHAR(45);
					optimistic_id	VARCHAR(45);
					olddata 		json;
					newdata 		json;
					notification 	json;
					notify_id 		INTEGER;

				BEGIN
					-- Convert the old and new row to JSON, based on the kind of action.
					-- Action = DELETE?             -> OLD row
					-- Action = INSERT or UPDATE?   -> NEW row
					IF (TG_OP = 'DELETE') THEN
						olddata = row_to_json(OLD);
						id = OLD._id;
						optimistic_id = OLD._optimistic_id;
					ELSE
						id = NEW._id;
						optimistic_id = NEW._optimistic_id;
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
							'_id', id,
							'optimistic_id', optimistic_id,
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

		// check if there are currently reactive queries register
		// if not, still exit!
		if (!queryIds) return;
		if (queryIds.length == 0) return;
		
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
		});
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

	View(viewName, options){
		return new PGView(this, viewName, options);
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
