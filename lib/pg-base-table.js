'use strict';

const PGQuery 			= require('./pg-query').PGQuery;
const PGReactiveQuery	= require('./pg-reactive-query').PGReactiveQuery;
const Synchron			= require('synchron').Synchron;

const md5	= require('md5');
const async	= require('async');
const _ 	= require('lodash');

class PGBaseTable {
	constructor(dbInstance, baseName, options, callback) {
		callback = callback || function(){};

		this._database = dbInstance;
		this._schemaName = baseName.indexOf('.') > -1 ? baseName.split('.')[0] : this._database.defaultSchema;
		this._baseName = baseName.indexOf('.') > -1 ? baseName.split('.')[1] : baseName;
		this._tableDependencies = [this._schemaName + '.' + this._baseName];
		this._alias = options && (options.as || options.alias);

		// Options
		this._options = {};
		this._options._alias = options && (options.as || options.alias);
		this._options._removeUnknownCols = options && (options.removeUnknownCols === true || options.removeUnknownCols === false) ? options.removeUnknownCols : true;

		this._getColumnInformations(this._baseName, (error, columnInfos) => {
			if (error) return callback(error);

			this._columnInfos =	columnInfos;
			return callback();
		});
	}

	get collectionName(){
		return this._alias || this._baseName;
	}

	/**
     * _hasMongoLikeIDCol
     * Checks if the table has a columne named '_id' and returns true or false.
     *
     * @return {Boolean}  True if the table has a column named _id otherwise false.
     */
	_hasMongoLikeIDCol(){
		return _.isObject(this._columnInfos['_id']);
	}

	_getColumnInformations(baseName, callback) {
		var tableName = baseName,
			schemaName = this._database.defaultSchema || 'public';

		if (baseName.indexOf('.') > -1) {
			var schemaAndTable = baseName.split('.')[1];
			schemaName = schemaAndTable[0];
			tableName = schemaAndTable[1];
		}

		async.waterfall([
			(callback) => {
				this._database.getConnection( (error, connection) => {
					callback(error, connection);
				});
			},
			(connection, callback) => {
				let sql=`SELECT
		                	column_name                AS columnname,
		                    is_nullable                AS isnullable,
		                    data_type                  AS datatype,
		                    character_maximum_length   AS charactermaximumlength
		                FROM
		                    information_schema.columns
		                WHERE
		                    table_name = $1
						AND schema_name = $2
		                AND table_catalog = current_database();`;

				this._database.query(connection, sql, [tableName, schemaName], (error, result) => {
					this._database.releaseConnection(connection);

					if (error) return callback(error);

					// transform the columns array into an object
					var transformedResult = {};
					for (var i=0, max=result.rows.length; i < max; i++){
						var column = result.rows[i];
						transformedResult[column.columnname] = column;
					}
					return callback(null, transformedResult);
				});
			}
		], (error, result) => {
			callback(error, result);
		});
	}

	find(selector, options) {
		var queryObject = {
			tableOrViewName: this._schemaName + '.' + this._baseName,
			tableDependencies: this._tableDependencies,
			selector: selector,
			options: options
		}
		var query = new PGReactiveQuery(this, queryObject);

		this._database._cacheQuery(query);

		return query;
	}

	select(selector, options, callback) {
		var query = new PGQuery(this, {
			tableOrViewName: this._baseName,
			selector: selector,
			options: options
		});

		query.run((error, result) => {
			query.destroy();
			query = null;

			return callback(error, result);
		});
	}
}

module.exports.PGBaseTable = PGBaseTable;
