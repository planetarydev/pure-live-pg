'use strict';

const PGQuery 			= require('./pg-query').PGQuery;
const PGReactiveQuery	= require('./pg-reactive-query').PGReactiveQuery;
const Synchron			= require('synchron').Synchron;

const md5	= require('md5');
const _ 	= require('lodash');

class PGBaseTable {
	constructor(dbInstance, baseName, options) {
		this._baseName = baseName;
		this._database = dbInstance;
		this._tableDependencies = [baseName];
		this._alias = options && (options.as || options.alias);

		// Options
		this._options = {};
		this._options._alias = options && (options.as || options.alias);
		this._options._removeUnknownCols = options && (options.removeUnknownCols === true || options.removeUnknownCols === false) ? options.removeUnknownCols : true;
		this._options._idGenerator = (doc) => {
			// generate the _id for the document
			// for this - by default - we check the primaryKey cols
			// and return the value of this column(s). If the pk contains on
			// more then one column we concat each and return a hashed value
			var _id = '';
			if (this._pkColumns){
				for (var i=0, max=this._pkColumns.length; i<max; i++){
					_id += doc[this._pkColumns[i]] && doc[this._pkColumns[i]].toString() || '';
				}
			} else {
				_id = doc.id || doc.aid;
			}

			if (!_id || _id.length == 0){
				throw new Error('Invalid _id: Can\'t create an _id column using the default idGenerator.');
			}

			return md5(_id);
		};
		if (options && _.isFunction(options.idGenerator)) {
			this._options._idGenerator = options.idGenerator;
		}

		this._columnInfos = this._getColumnInformations(this._baseName);
		this._pkColumns = null; // set by the mysql-table constructor

		this._initNotifyTriggerSync = new Synchron(function(dbInstance, tableName){
			dbInstance._initNotifyTrigger(tableName, (error, result) => {
				if (error) return this.throw(error);
				return this.done();
			});
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

	_getColumnInformations(baseName){
		let connection = this._database.getConnectionSync();
		let sql=`SELECT
                	column_name                AS columnname,
                    is_nullable                AS isnullable,
                    data_type                  AS datatype,
                    character_maximum_length   AS charactermaximumlength
                FROM
                    information_schema.columns
                WHERE
                    table_name = $1
                AND table_catalog = current_database();`;

		var result = this._database.querySync(connection, sql, [baseName]);
		this._database.releaseConnection(connection);

		// transform the columns array into an object
		var transformedResult = {};
		for (var i=0, max=result.rows.length; i < max; i++){
			var column = result.rows[i];
			transformedResult[column.columnname] = column;
		}
		return transformedResult;
	}

	find(selector, options) {
		var queryObject = {
			tableOrViewName: this._baseName,
			tableDependencies: this._tableDependencies,
			selector: selector,
			options: options
		}
		var query = new PGReactiveQuery(this, queryObject);

		// cache the query for a later rerun done by the the notification events
		this._database._queryCache[query.queryId] = query;
		// register the query for each related table it depends on
		_.forEach(query._queryObject.tableDependencies, (tableName) => {
			if (!this._database._queriesByTable[tableName]) {
				this._database._queriesByTable[tableName] = [];
			}
			this._database._queriesByTable[tableName].push(query.queryId);
		});
		return query;
	}

	select(selector, options) {
		var query = new PGQuery(this, {
			tableOrViewName: this._baseName,
			selector: selector,
			options: options
		});

		var result = query.run();
		query.destroy();

		return result;
	}
}

module.exports.PGBaseTable = PGBaseTable;
