'use strict';

const { PGBaseTable }	= require('./pg-base-table');

const SQLBuilder	= require('json-sql-builder');
const sqlBuilder	= new SQLBuilder('postgreSQL');
const async			= require('async');

class PGTable extends PGBaseTable {
	constructor(database, tableName, options) {
		super(database, tableName, options);

		this._pkColumns = this._getPkColumns(this._baseName);

		this._initNotifyTriggerSync(this._database, this._baseName);
	}

	_getPkColumns(baseName){
		let connection = this._database.getConnectionSync();

		let sql=`SELECT
                	string_agg(k.column_name, ',') AS pk_columns
                FROM
                	INFORMATION_SCHEMA.TABLE_CONSTRAINTS t
                LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE k
                	USING (CONSTRAINT_NAME, TABLE_SCHEMA, TABLE_NAME)
                WHERE
                	t.constraint_type = 'PRIMARY KEY'
                AND t.table_catalog = current_database()
                AND t.table_name = $1
				GROUP BY
					k.ordinal_position
                ORDER BY
                	k.ordinal_position;`;
		var results = this._database.querySync(connection, sql, [baseName]);
		this._database.releaseConnection(connection);

		if (results.rowCount == 0){
			return null;
		} else {
			return results.rows[0].pk_columns.split(',');
		}
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

			var results = this._insert(doc, true/*optimistic*/, subscriptionId, callback);
		} catch (ex) {
			if (published){
				reactiveQuery._removedOptimistic(doc._id);
			}
			// re-throw exeption
			throw ex;
		}

		return results;
	}

	insert(docOrDocs, callback) {
		return this._insert(docOrDocs, false/*optimistic*/, null/*subscriptionId*/, callback);
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
				this._database.wpQuery(connection, stmt.sql, stmt.values, (error, results) => {
					this._database.releaseConnection(connection);

					if (error) return callback(error);

					return callback(null, results);
				});
			}
		],
		(error, results) => {
			setTimeout(() => {
				return callback(error, results);
			}, 0);
		});
	}

	remove(filter){
		let methodInvocation = DDP._CurrentMethodInvocation.get();
		let query = {
			type: 'delete',
			table: this._baseName,
			where: filter
		};
		let stmt = SQLBuilder.buildStatement(query);

		return this._database.wpQuerySync(stmt.toString(), stmt.values, methodInvocation);
	}
}
module.exports.PGTable = PGTable;
