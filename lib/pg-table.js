'use strict';

const PGBaseTable	= require('./pg-base-table').PGBaseTable;

const SQLBuilder	= require('json-sql-builder');
const sqlBuilder	= new SQLBuilder('postgreSQL');
const async			= require('async');

const LOOKUP_INTERVAL = 10;

class PGTable extends PGBaseTable {
	constructor(database, tableName, options, callback) {
		super(database, tableName, options, (error) => {
			database._initNotifyTrigger(tableName, (error, result) => {
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

	insert(docOrDocs, callback) {
		//return this._insert(docOrDocs, false/*optimistic*/, null/*subscriptionId*/, callback);

		async.waterfall([
			(callback) => {
				let query = {
					$insert: {
						$into: this._baseName,
						$documents: docOrDocs
					}
				};

				let stmt = sqlBuilder.build(query);

				return callback(null, stmt)
			},
			(stmt, callback) => {
				this._database.getConnection((error, connection) => {
					return callback(error, stmt, connection);
				});
			},
			(stmt, connection, callback) => {
				// set the unique id
				var uid = Math.random().toString(26).slice(2);
				this._database._runningDbActions[uid] = true;
				connection.query(`SELECT set_config('core_settings.client_id', '${uid}', false);`);
				console.log('RUN Action:', uid);
				this._database.wpQuery(connection, stmt.sql, stmt.values, (error, results) => {
					this._database.releaseConnection(connection);

					if (error) return callback(error);

					return callback(null, results, uid);
				});
			},
			(result, uid, callback) => {
				// waiting till uid was received for the notify-listner and all reactive queries are performed
				if (result.rowCount > 0) {
					var interv = setInterval(()=>{
						if (!this._database._runningDbActions[uid]) {
							clearInterval(interv);

							console.log('DONE Action:', uid);
							return callback(null, result);
						}
					}, LOOKUP_INTERVAL);
				} else {
					delete this._database._runningDbActions[uid];
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

	update(filter, document, callback){
		callback = callback || function(){};

		async.waterfall([
			(callback) => {
				let query = {
					$update: {
						$table: this._baseName,
						$set: document,
						$where: filter
					}
				};

				let stmt = sqlBuilder.build(query);
				return callback(null, stmt)
			},
			(stmt, callback) => {
				this._database.getConnection((error, connection) => {
					return callback(error, stmt, connection);
				});
			},
			(stmt, connection, callback) => {
				// set the unique id
				var uid = Math.random().toString(26).slice(2);
				this._database._runningDbActions[uid] = true;
				connection.query(`SELECT set_config('core_settings.client_id', '${uid}', false);`);
				console.log('RUN Action:', uid);
				this._database.wpQuery(connection, stmt.sql, stmt.values, (error, results) => {
					this._database.releaseConnection(connection);

					if (error) return callback(error);

					return callback(null, results, uid);
				});
			},
			(result, uid, callback) => {
				// waiting till uid was received for the notify-listner and all reactive queries are performed
				if (result.rowCount > 0) {
					var interv = setInterval(()=>{
						if (!this._database._runningDbActions[uid]) {
							clearInterval(interv);

							console.log('DONE Action:', uid);
							return callback(null, result);
						}
					}, LOOKUP_INTERVAL);
				} else {
					delete this._database._runningDbActions[uid];
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

	remove(filter, callback){
		async.waterfall([
			(callback) => {
				let query = {
					$delete: {
						$table: this._baseName,
						$where: filter
					}
				};

				let stmt = sqlBuilder.build(query);

				return callback(null, stmt)
			},
			(stmt, callback) => {
				this._database.getConnection((error, connection) => {
					return callback(error, stmt, connection);
				});
			},
			(stmt, connection, callback) => {
				// set the unique id
				var uid = Math.random().toString(26).slice(2);
				this._database._runningDbActions[uid] = true;
				connection.query(`SELECT set_config('core_settings.client_id', '${uid}', false);`);
				console.log('RUN Action:', uid);
				this._database.wpQuery(connection, stmt.sql, stmt.values, (error, results) => {
					this._database.releaseConnection(connection);

					if (error) return callback(error);

					return callback(null, results, uid);
				});
			},
			(result, uid, callback) => {
				// waiting till uid was received for the notify-listner and all reactive queries are performed
				if (result.rowCount > 0) {
					var interv = setInterval(()=>{
						if (!this._database._runningDbActions[uid]) {
							clearInterval(interv);

							console.log('DONE Action:', uid);
							return callback(null, result);
						}
					}, LOOKUP_INTERVAL);
				} else {
					delete this._database._runningDbActions[uid];
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
}
module.exports.PGTable = PGTable;
