'use strict';

const PGBaseTable	= require('./pg-base-table').PGBaseTable;

const SQLBuilder	= require('json-sql-builder');
const sqlBuilder	= new SQLBuilder('postgreSQL');
const async			= require('async');
const _				= require('lodash');

const LOOKUP_INTERVAL = 5;

class PGTable extends PGBaseTable {
	constructor(database, tableName, options, callback) {
		super(database, tableName, options, (error) => {
			database._initNotifyTrigger(this._schemaName + '.' + this._baseName, (error, result) => {
				if (error) return callback(error);

				return callback();
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

	/*
	_insert(docOrDocs, optimistic, subscriptionId, callback){
		let methodInvocation = DDP._CurrentMethodInvocation.get();
		let _docOrDocs = docOrDocs,
			query,
			stmt;

		// delete all properties that are not a column of this table
		if (this._options._removeUnknownCols){
			if (_.isObject(docOrDocs)) {
				// we have a single document = record to insert
				_docOrDocs = this._removeUnknownColumns(docOrDocs);
			} else if (_.isArray(docOrDocs)) {
				_docOrDocs = _docOrDocs.map( (doc) => {
					return this._removeUnknownColumns(doc);
				});
			} else {
				throw new Meteor.Error('Insert failed', 'Insert failed on wrong parameters. The first parameter "docOrDocs" must be an object=single document or an array of objects');
			}
		}

		query = {
			type: 'insert',
			table: this._baseName,
			values: _docOrDocs
		};
		stmt = SQLBuilder.buildStatement(query);

		let result = this._database.wpQuerySync(stmt.toString(), stmt.values, methodInvocation);

		return result;
	}

	optimisticInsert(doc, subscriptionId, callback){
		// generate a new id like the id on the client
		var published = false;
		var reactiveQuery;

		if (!this._hasMongoLikeIDCol()){
			throw new Meteor.Error('No support for optimistic-insert, because the table "' + this._baseName + '" has no column named _id.');
		}

		// check if a id is already generated - if not generate
		// a new id like this generation from the client
		// TODO: Clarify --> What is about the randomSeed and randomStream???
		if (!doc._id) {
			doc._id = this._makeNewID();
		}

		try {
			var reactiveQuery = this._database._queryCache[subscriptionId];
			reactiveQuery._addedOptimistic(doc._id, doc);
			published = true;

			var results = this._insert(doc, true, subscriptionId, callback);
		} catch (ex) {
			if (published){
				reactiveQuery._removedOptimistic(doc._id);
			}
			// re-throw exeption
			throw ex;
		}

		return results;
	}*/
	_execStmt(stmt, clientId, callback){

		if (!_.isFunction(callback) && _.isFunction(clientId)) {
			callback = clientId;
		}
		if (!_.isString(clientId)) {
			// no client or session id suppoerted
			// take a pseudo client-id, because postgreSQL
			// will thrown an exeption by using current_config()
			clientId = PGTable.NO_CLIENT_ID_SUPPORTED;
		}

		async.waterfall([
			(callback) => {
				this._database.getConnection((error, connection) => {
					return callback(error, connection);
				});
			},
			(connection, callback) => {
				// set the unique id for this statement
				var statementId = Math.random().toString(26).slice(2);
				connection.query(`SELECT set_config('core_settings.client_id', $1, false);`, [clientId]);
				connection.query(`SELECT set_config('core_settings.statement_id', $1, false);`, [statementId]);
				if (clientId != PGTable.NO_CLIENT_ID_SUPPORTED) {
					this._database._runningDbActions[statementId] = true;
				}
				//console.log('RUN Action:', clientId);
				this._database.wpQuery(connection, stmt.sql, stmt.values, (error, result) => {
					this._database.releaseConnection(connection);

					if (error) return callback(error);

					return callback(null, result, statementId);
				});
			},
			(result, statementId, callback) => {
				//console.log('EXECUTED');
				// waiting till clientId was received for the notify-listner and all reactive queries are performed
				if (clientId != PGTable.NO_CLIENT_ID_SUPPORTED && result.rowCount > 0) {
					var interv = setInterval(()=>{
						if (!this._database._runningDbActions[statementId]) {
							clearInterval(interv);

							//console.log('DONE Action:', clientId);
							return callback(null, result);
						}
					}, LOOKUP_INTERVAL);
				} else {
					if (clientId != PGTable.NO_CLIENT_ID_SUPPORTED){
						delete this._database._runningDbActions[statementId];
					}
					//console.log('OUT');
					return callback(null, result);
				}
			}
		],
		(error, results) => {
			setTimeout(() => {
				return callback(error, results);
			}, 0);
		});
	}

	insert(docOrDocs, clientId, callback) {
		let query = {
			$insert: {
				$into: this._schemaName + '.' + this._baseName,
				$documents: docOrDocs
			}
		};

		let stmt = sqlBuilder.build(query);
		this._execStmt(stmt, clientId, callback);
	}

	update(filter, document, clientId, callback){
		let query = {
			$update: {
				$table: this._schemaName + '.' + this._baseName,
				$set: document,
				$where: filter
			}
		};

		let stmt = sqlBuilder.build(query);
		this._execStmt(stmt, clientId, callback);
	}

	remove(filter, clientId, callback){
		let query = {
			$delete: {
				$table: this._schemaName + '.' + this._baseName,
				$where: filter
			}
		};

		let stmt = sqlBuilder.build(query);
		this._execStmt(stmt, clientId, callback);
	}
}

PGTable.NO_CLIENT_ID_SUPPORTED = '$$NO-CLIENT-ID-SUPPORTED$$';

module.exports.PGTable = PGTable;
