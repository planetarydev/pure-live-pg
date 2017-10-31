'use strict';

const SQLBuilder	= require('json-sql-builder');
const sqlBuilder	= new SQLBuilder('postgreSQL');
const async			= require('async');
const md5			= require('md5');
const EventEmitter  = require('events');
const _				= require('lodash');

class PGQuery extends EventEmitter {
	constructor(basetableObject, queryObject) {
		super();

		this._baseTableObject = basetableObject;
		this._database = basetableObject._database;
		this._queryObject = queryObject;
		this._defaultOptions = {
			salt: false,
			preparation: false
		};
		// extend the user-options with the defaults
		this._queryObject.options = _.extend(this._defaultOptions, this._queryObject.options);

		this._sql = null;
		this._parameters = [];
		this._salt = null;
	}

	destroy(){
		// remove the reference of the baseTableObject and database-object
		this._baseTableObject = null;
		this._database = null;
	}

	get queryId(){
		return this._queryId;
	}

	set queryId(newId){
		this._queryId = newId;
	}

	run(callback){
		callback = callback || function(){};

		let query = {
				preparation: true,
				salt: true
			},
			options = this._queryObject.options;

		//query = _.extend(query, options);

		// override the basic query-options, they could'nt modified by the user
		query = {
			$select: {
				$from: this._queryObject.tableOrViewName,
				$where: this._queryObject.selector
			}
		};

		query.$select = _.extend(query.$select, options);
		// remove internal options that should not used for the query itself
		delete query.$select.salt;
		delete query.$select.preparation;

		// the server-side pagination option exclude the use of offset and limit
		/*if ((options.limit || options.offset) && options.pagination){
			throw new Error('Can\'t use option \'offset\' or \'limit\' together with \'pagination\'. Remove either \'offset\' and \'limit\' or \'pagination\' option.');
		}

		if ((options.limit || options.offset) && options.lazyload){
			throw new Error('Can\'t use option \'offset\' or \'limit\' together with \'lazyload\'. Remove either \'offset\' and \'limit\' or \'lazyload\' option.');
		}

		// check for the salt option
		if (options.salt){
			this._salt = this._queryObject.invocation && this._queryObject.invocation._subscriptionId;
		}

		if (options.pagination){
			this._pagination.enabled = true;
			this._pagination.limit = options.pagination.limit || 10;

			query.$offset = this._pagination.currentOffset || options.pagination.offset || 0;
			query.$limit = this._pagination.limit;
			query.calcFoundRows = true;

			// enable for a maybe next run
			this._queryObject.options.calcFoundRows = true;
		}

		if (options.lazyload && options.pagination){
			throw new Error('Can\'t use option \'lazyload\' together with \'pagination\'. Remove either \'lazyload\' or \'pagination\' option.');
		}

		if (options.lazyload){
			this._lazy.enabled = true;
			this._lazy.limit = options.lazyload.limit || 10;
			query.$offset = this._lazy.currentOffset || options.lazyload.offset || 0;
			query.$limit = this._lazy.limit;
			query.calcFoundRows = true;

			// enable for a maybe next run
			this._queryObject.options.calcFoundRows = true;
		}*/

		let stmt = sqlBuilder.build(query);

		this._stmt = stmt;

		this.executeQuery(false, callback);
	}

	executeQuery(reRun, callback){
		async.waterfall([
			(callback) => {
				this._database.getConnection((error, connection) => {
					return callback(error, connection);
				});
			},
			(connection, callback) => {
				var query = { sql: this._stmt.sql, values: this._stmt.values.slice() };
				var queryFn;

				//OLD if (reRun && this._lazy.enabled) {
					// if we are on a rerun and lazyload option is active
					// we have to rest the offset with 0, otherwise if there
					// are changes made on records before the offset the client subscription
					// will not be updated
				//OLD 	if (this._sql.indexOf(' offset $') > -1) { // check explict for offset, because on the firstrun we only have the limit
						// offset is always the last parameter value
						// overwrite the current offset with 0 to check all records loaded before
				//OLD 		query.values[query.values.length - 1 /*offset*/] = 0;
						// set the limit of records to the current offset
						// because we need all records till the end of the currentOffset
				//OLD 		query.values[query.values.length - 2 /*limit*/] = this._parameters[query.values.length - 1 /*offset*/];
				//OLD 	}
				//OLD }

				if (this._queryObject.options.preparation) {
					queryFn = this._database.wpQuery;
				} else {
					queryFn = this._database.query;
				}

				queryFn.call(this._database, connection, query.sql, query.values, (error, queryResult) => {
					if (error){
						this._database.releaseConnection(connection);
						return callback(error);
					}

					if (this._dataLoading && (this._dataLoading.type === 'paging' || this._dataLoading.type === 'incremental')) {
						
						return queryFn.call(this._database,
							connection,
							this._dataLoading.stmt.sql,
							this._dataLoading.stmt.values,
							(error, totalRowsResult) => {
								this._database.releaseConnection(connection);

								if (error) return callback(error);

								this._dataLoading.totalRowCount = totalRowsResult.rowCount == 1 ? +totalRowsResult.rows[0].__totalRowCount__ : null;
								return callback(null, queryResult);
						});
					}

					this._database.releaseConnection(connection);
					return callback(error, queryResult);
				});
			}
		], (error, result) => {
			callback(error, result);
		});

		/*var connection = this._database.getConnectionSync();
		var query = { sql: this._stmt.sql, values: this._stmt.values.slice() };

		if (reRun && this._lazy.enabled) {
			// if we are on a rerun and lazyload option is active
			// we have to rest the offset with 0, otherwise if there
			// are changes made on records before the offset the client subscription
			// will not be updated
			if (this._sql.indexOf(' offset $') > -1) { // check explict for offset, because on the firstrun we only have the limit
				// offset is always the last parameter value
				// overwrite the current offset with 0 to check all records loaded before
				query.values[query.values.length - 1 ] = 0;
				// set the limit of records to the current offset
				// because we need all records till the end of the currentOffset
				query.values[query.values.length - 2 ] = this._parameters[query.values.length - 1 ];
			}
		}

		if (this._queryObject.options.preparation) {
			var results = this._database.wpQuerySync.call(this._database, connection, query.sql, query.values);
		} else {
			var results = this._database.querySync.call(this._database, connection, query.sql, query.values);
		}
		if (this._queryObject.options.calcFoundRows){
			var cfrResult = this._database.querySync(connection, 'SELECT found_rows() AS `totalRowCount`', []);
			this._lazy.totalRowCount = cfrResult[0].totalRowCount;
			this._pagination.totalRowCount = cfrResult[0].totalRowCount;
		}
		this._database.releaseConnection(connection);
		return results;*/
	}
}
module.exports.PGQuery = PGQuery;
