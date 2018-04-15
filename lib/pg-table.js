'use strict';

const PGBaseTable	= require('./pg-base-table').PGBaseTable;

const async			= require('async');
const _				= require('lodash');

const LOOKUP_INTERVAL = 5;

class PGTable extends PGBaseTable {
	constructor(database, tableName, options, callback) {
		super(database, tableName, options, (error) => {
			if (error) return callback(error);

			async.series([
				(callback) => {
					// create Table if there are options supported for that
					if (! options) {
						return callback();
					}

					// remove the options that does'nt relate to a create table
					var opt = _.cloneDeep(options);
					if (opt.as) delete opt.as;
					if (opt.alias) delete opt.alias;

					// move each identifier to the $define-object
					// and remove it from the current opt object
					// so that there are still options remaining
					var definition = {};
					_.forEach(opt, (value, key) => {
						if (this._database.sqlBuilder.isIdentifier(key)) {
							definition[key] = value;
							delete opt[key];
						}
					})

					var createTable = {
						$createTable: {
							$ine: true,
							$table: this._schemaName + '.' + this._baseName
						}
					};

					// extend additional create-options
					_.extend(createTable.$createTable, opt);
					createTable.$createTable.$define = definition;

					var stmt = this._database.sqlBuilder.build(createTable);
					this._database.getConnection((error, connection) => {
						if (error) return callback(error);

						this._database.query(connection, stmt.sql, stmt.values, (error, result) => {
							this._database.releaseConnection(connection);

							if (error) return callback(error);
							return callback();
						});
					});
				},
				// Create Notify-Trigger after successful creation of the table
				(callback) => {
					database._initNotifyTrigger(this._schemaName + '.' + this._baseName, (error, result) => {
						if (error) return callback(error);

						return callback();
					});
				}
			], (error, results) => {
				// ignore results
				return callback(error, this);
			});
		});
	}

	_removeUnknownColumns(doc){
		var columns = Object.keys(doc),
			newDoc = {};

		for (var i=0, max=columns.length;i<max;i++) {
			var column = columns[i];
			// check if column exists
			if (this._columnInfos[column]){
				newDoc[column] = doc[column];
			}
		}

		return newDoc;
	}


	_execStmt(stmt, schemaAndTable, options, callback){
		let transactionHandle = options && options.transaction || null,
			clientId = options && options.clientId || null;

		if (!_.isFunction(callback) && _.isFunction(options)) {
			transactionHandle = null;
			clientId = null;
			callback = options;
		}
		if (!_.isString(clientId)) {
			// no client or session id supported
			// take a pseudo client-id, because postgreSQL
			// will thrown an exeption by using current_config()
			clientId = PGTable.NO_CLIENT_ID_SUPPORTED;
		}

		async.waterfall([
			(callback) => {
				// if we have a transactionHandle, we does'nt need a new connection
				// instead we have to execute the statement on the given connection
				// from the transactionHandle
				if (transactionHandle) {
					return callback(null, transactionHandle);
				}

				// no transaction support, get a new connection from the pool
				this._database.getConnection((error, connection) => {
					return callback(error, connection);
				});
			},
			(connection, callback) => {
				// set the unique id for this statement
				var statementId = Math.random().toString(26).slice(2);
				connection.query(`SELECT set_config('pg_reactive_settings.client_id', $1, false);`, [clientId]);
				connection.query(`SELECT set_config('pg_reactive_settings.statement_id', $1, false);`, [statementId]);
				connection.query(`SELECT set_config('pg_reactive_settings.statement_target', $1, false);`, [schemaAndTable]);
				if (clientId != PGTable.NO_CLIENT_ID_SUPPORTED) {
					this._database._runningStatements[statementId] = true;
				}
				//console.log('RUN Action:', clientId);
				this._database.wpQuery(connection, stmt.sql, stmt.values, (error, result) => {
					// only release the connection, if this is a connection
					// currently born by getConnection in this function. If we
					// have a transactionHandle, we leaf the connection open
					// to work on this in future - for transactional insert, deletes and update -
					// till there was a commit or rollback
					if (!transactionHandle){
						this._database.releaseConnection(connection);
					}

					if (error) return callback(error);

					return callback(null, result, statementId);
				});
			},
			(result, statementId, callback) => {
				// waiting till clientId was received for the notify-listner and all
				// reactive queries are performed for this client.
				//
				// If there was an active transaction we can't wait, because the pg_notify events
				// are transactional. By using transactions there is no chance for optimistic UI
				// As alternative all statements can issue within a storeProcedure that executes
				// all statements an manages the transaction at start and end of the procedure itself.
				if (!transactionHandle && clientId != PGTable.NO_CLIENT_ID_SUPPORTED && result.rowCount > 0) {
					var interv = setInterval(()=>{
						if (!this._database._runningStatements[statementId]) {
							clearInterval(interv);

							//console.log('DONE Action:', clientId);
							return callback(null, result);
						}
					}, LOOKUP_INTERVAL);
				} else {
					if (clientId != PGTable.NO_CLIENT_ID_SUPPORTED){
						delete this._database._runningStatements[statementId];
					}
					//console.log('OUT');
					return callback(null, result);
				}
			}
		],
		(error, results) => {
			//setTimeout(() => {
				return callback(error, results);
			//}, 0);
		});
	}

	insert(docOrDocs, options, callback) {
		let schemaAndTable = this._schemaName + '.' + this._baseName,
			query = {
				$insert: {
					$table: schemaAndTable,
					$documents: docOrDocs
				}
			};

		if (options && options.return) {
			query.$insert.$returning = options.return;
		}
		let stmt = this._database.sqlBuilder.build(query);
		this._execStmt(stmt, schemaAndTable, options, callback);
	}

	update(selector, document, options, callback){
		let schemaAndTable = this._schemaName + '.' + this._baseName,
			query = {
				$update: {
					$table: schemaAndTable,
					$set: document,
					$where: this._checkSelector(selector)
				}
			};

		if (this._database._debug) console.log('PGTable.update - Query before build:', JSON.stringify(query, null, 4));
		let stmt = this._database.sqlBuilder.build(query);

		if (this._database._debug) console.log('PGTable.update - Query stmt:', JSON.stringify(stmt, null, 4));
		this._execStmt(stmt, schemaAndTable, options, callback);
	}

	upsert(checkConflict, document, options, callback) {
		let schemaAndTable = this._schemaName + '.' + this._baseName,
			query = {
				$insert: {
					$table: schemaAndTable,
					$documents: document,
					$onConflict: {
						//$checkColumns: checkConflict,
						$columns: checkConflict,
						$doUpdateSet: document
					}
				}
			};

		let stmt = this._database.sqlBuilder.build(query);
		this._execStmt(stmt, schemaAndTable, options, callback);
	}

	remove(selector, options, callback){
		let schemaAndTable = this._schemaName + '.' + this._baseName,
			query = {
				$delete: {
					$from: schemaAndTable,
					$where: this._checkSelector(selector)
				}
			};

		let stmt = this._database.sqlBuilder.build(query);
		this._execStmt(stmt, schemaAndTable, options, callback);
	}
}

PGTable.NO_CLIENT_ID_SUPPORTED = '$$NO-CLIENT-ID-SUPPORTED$$';

module.exports.PGTable = PGTable;
