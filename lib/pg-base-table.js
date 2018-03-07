'use strict';

const PGQuery 			= require('./pg-query').PGQuery;
const PGReactiveQuery	= require('./pg-reactive-query').PGReactiveQuery;

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

		this._getColumnInformations(this._schemaName, this._baseName, (error, columnInfos) => {
			if (error) return callback.call(this, error);

			this._columnInfos =	columnInfos;
			return callback.call(this);
		});
	}

	get namedIdentifier(){
		return this._alias || this._schemaName + '.' + this._baseName;
	}

	_checkAndTurnQueryOptions(options){
		if (!options) return;

		// turn fields: {...} to $columns: [...]
		if (options.fields){
			if (options.$columns) throw new Error('Can\'t support fields and $columns option together. Please specify only one - fields or $columns option.');

			var fieldList = Object.keys(options.fields);
			var restrictedColumns = [];
			var allowedColumns = fieldList.filter(function(col){
				if (options.fields[col] === 1 || options.fields[col] === true){
					return true
				} else {
					restrictedColumns.push(col);
					return false;
				}
			});

			if (restrictedColumns.length > 0 && allowedColumns.length > 0){
				throw new Error('Can\'t use allow (1) and restrict (0) for fields option on the same time. ' + JSON.stringify(options.fields));
			}
			delete options.fields;

			if (allowedColumns.length > 0){
				options.$columns = allowedColumns;
			} else {
				options.$columns = [];
				_.forEach(this._columnInfos, (col) => {
					// check each column if it's restricted on select
					if (restrictedColumns.indexOf(col.columnname) == -1){
						options.$columns.push(col.columnname);
					}
				});
			}

			// always support the column _id
			if (options.$columns.indexOf('_id') == -1){
				options.$columns.push('_id');
			}
		}

		return options;
	}

	_checkSelector(selector){
		if (_.isString(selector)) {
			return { _id: selector };
		}
		return selector;
	}


	_getColumnInformations(schemaName, tableName, callback) {
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
						AND table_schema = $2
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
			selector: this._checkSelector(selector),
			options: this._checkAndTurnQueryOptions(options)
		}
		var query = new PGReactiveQuery(this, queryObject);

		this._database._cacheQuery(query);

		return query;
	}

	select(selector, options, callback) {
		let __options = options,
			__callback = callback;

		if (_.isFunction(options) && !callback){
			__callback = options;
			__options = {};
		}
		__callback = __callback || function(){};

		var query = new PGQuery(this, {
			tableOrViewName: this._schemaName + '.' + this._baseName,
			selector: this._checkSelector(selector),
			options: this._checkAndTurnQueryOptions(__options)
		});

		query.run((error, result) => {
			query.destroy();
			query = null;

			return __callback(error, result);
		});
	}
}

module.exports.PGBaseTable = PGBaseTable;
