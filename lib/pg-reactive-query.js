'use strict';

const PGQuery			= require('./pg-query').PGQuery;
const JSONSQLBuilder	= require('json-sql-builder');
const sqlBuilder		= new JSONSQLBuilder('postgreSQL');

const async			= require('async');
//const md5 			= require('md5');
const shortid 		= require('shortid');
const _				= require('lodash')

let queryIdCounter = 0;

class PGReactiveQuery extends PGQuery {
	constructor(basetableObject, queryObject) {
		super(basetableObject, queryObject);

		this._queryId = shortid.generate() + '-' + new Date().getTime();
		queryIdCounter++;
		this._salt = null;

		this._dataLoading = {
			type: 'complete',  // options are "complete", "paging", "incremental"
			// setup the limit for records each page will have (using "paging")
			// or the number of records to load more using "incremental"
			limit: null,
			// holds the current offset of the query wich will be recalculatet
			// by the method gotoPage
			currentOffset: null,
			// it's the total rowcount of the query
			totalRowCount: null,
			// the query to calculate the totalRowCount
			query: null,
			// a deep clone of the user-defined query to change the $offset or $limit
			// by calling the methods loadNext, gotoPage and rebuild and run the query again
			originalQuery: null,
			// determines if the query is running or ready
			ready: null,
			// used for type "paging" and holds the currentPage
			currentPage: null,
			// used for type "paging" and holds the max pages info
			pageCount: null
		}

		this._bindedExecuteQuery = null;
	}

	destroy(done) {
		let superDestroy = super.destroy.bind(this);

		this._stoped = true;

		let _destroy = (callback) => {
			if (this._running) console.log('RUNNING!');

			if (!_.isFunction(callback)) {
				done = function(){};
			};

			// delete the hashed reactive data from the reactivity table for this query
			async.series([
				(callback) => {
					this._database.getConnection((error, connection) => {
						if (error) return callback(error);

						// console.log('DELETE FROM core_reactive WHERE query_id = $1', self.queryId);
						this._database.query(connection, `DELETE FROM "${this._database._ownSchema}".reactivity WHERE query_id = $1`, [this._queryId], (error, result) => {
							this._database.releaseConnection(connection);
							return callback(error, result);
						});
					});
				},
				(callback) => {
					// delete the published Info documents

					/*var infoCollection = self._getInfoCollection();

					if (self._pagination.published) {
						var docId = 'P' + self._queryObject.invocation._subscriptionId;
						self._queryObject.invocation.removed(infoCollection, docId);
					}
					if (self._lazy.published) {
						var docId = 'L' + self._queryObject.invocation._subscriptionId;
						self._queryObject.invocation.removed(infoCollection, docId);
					}
					*/


					// remove the query from the query-cache
					this._database._unCacheQuery(this._queryId, this._queryObject.tableDependencies);
					superDestroy();
					return callback();
				}
			], function(error){
				if (error) console.log('ERROR DESTROY:', error);
				return callback(error);
			});
		}

		// wait til the query is running
		var intv = setInterval(()=>{
			if (this._running) return;
			clearInterval(intv);
			_destroy(done);
		}, 0);
	}

	stop(){
		// stop the reactivity of this query
		this._stoped = true;
	}

	run(callback) {
		this._running = true;
		callback = callback || function(){};

		let query = {},
			options = this._queryObject.options;

		query.$select = _.extend(query.$select, options);

		// override the basic query-options, they could'nt modified by the user
		query.$select.$from = this._queryObject.tableOrViewName;
		query.$select.$where = this._queryObject.selector

		// remove internal options that should not used for the query itself
		delete query.$select.salt;
		delete query.$select.preparation;
		delete query.$select.clientId;

		// the server-side pagination option exclude the use of offset and limit
		if ((options.$limit || options.$offset) && options.loading){
			throw new Error('Can\'t use option \'$offset\' or \'$limit\' together with \'loading\'. Remove either \'$offset\' and \'$limit\' or \'loading\' option.');
		}

		if (options.$limit || options.$offset){
			this._dataLoading.currentOffset = options.$offset;
			this._dataLoading.limit = options.$limit;
		}

		// check for the salt option
		if (options.salt){
			this._salt = options.salt; //this._queryObject.invocation && this._queryObject.invocation._subscriptionId;
		}

		if (this._dataLoading.type == 'complete' && options.loading){
			this._dataLoading.type = options.loading.type;
			this._dataLoading.limit = options.loading.limit || 10;
			this._dataLoading.currentOffset = 0;
			this._dataLoading.ready = false;

			delete query.$select.loading;
		}

		query.$select.$offset = this._dataLoading.currentOffset;
		query.$select.$limit = this._dataLoading.limit;

		if (this._dataLoading.type === 'paging' || this._dataLoading.type === 'incremental'){
			// copy the query to use it later for the dataLoader option "paging" and "incremental"
			// to query to complete count of al rows
			this._dataLoading.query = _.cloneDeep(query);
			// now change the columns defined with "count(*)" as single column
			this._dataLoading.query.$select.$columns = { __totalRowCount__: { $count: '*' } };
			// and we have to remove any $sort, $orderBy, $limt and $offset, to get all rows for a total count
			delete this._dataLoading.query.$select.$orderBy;
			delete this._dataLoading.query.$select.$sort;
			delete this._dataLoading.query.$select.$limit;
			delete this._dataLoading.query.$select.$offset;

			this._dataLoading.stmt = sqlBuilder.build(this._dataLoading.query);
		}

		// keep the a clone of the json query to use later on loadNext and change the offset and limit
		// and force a rebuild of the new/current query
		this._dataLoading.originalQuery = _.cloneDeep(query);;

		this._rebuildAndExecQuery(query, callback);
	}

	_rebuildAndExecQuery(query, callback){
		this._running = true;

		let stmt = sqlBuilder.build(query);

		// this._stmt will always be used by the executeQuery method
		// called by this function and any reRun() called by reactive changes
		this._stmt = {
			// after building the SQL Stamtent we have to inject this into the reactive query template
			sql: this._database._sql.reactiveQuery
					.split('@@QueryId').join(this._queryId)
					.split('@@ownSchema').join(this._database._ownSchema)
					.replace('@@Query', stmt.sql),
			values: stmt.values
		}

		this.executeQuery(false/*rerun*/, (error, result)=>{
			this._running=false;
			return callback(error, result);
		});
	}

	loadNext(callback){
		// this method could only be called, when dataLoading is set to "incremental"
		// to load the next n records from the database
		if (this._dataLoading.type !== 'incremental'){
			throw new Error('loadNext() only available for incremental dataloading option.');
		}

		// check the current limit against the totalRowCount. We can't load more records
		// as available on the database, so get out if the currentLimit is higher
		if (this._dataLoading.totalRowCount <= this._dataLoading.originalQuery.$select.$limit){
			return callback();
		}
		// calc the new limit and make a rebuild of the query
		// we only have to bump the limit and because of the rerun and reactivity table
		// the query automatically returns only the next n records
		this._dataLoading.originalQuery.$select.$limit += this._dataLoading.limit;

		this._rebuildAndExecQuery(this._dataLoading.originalQuery, callback);
	}

	// pageNo will start with 0
	gotoPage(pageNo, callback){
		// this method could only be called, when dataLoading is set to "paging"
		// to load the next n records from the database
		if (this._dataLoading.type !== 'paging'){
			throw new Error('gotoPage() only available for dataloading option type "paging".');
		}

		// calc the new offset and make a rebuild of the query
		// we only have to change the offset and because of the rerun and reactivity table
		// the query automatically removes the "old" records and adds the records of the new page
		this._dataLoading.originalQuery.$select.$offset = pageNo * this._dataLoading.limit;
		this._dataLoading.currentPage = pageNo;

		this._rebuildAndExecQuery(this._dataLoading.originalQuery, callback);
	}

	executeQuery(reRun, callback){
		super.executeQuery(reRun, (error, result) => {
			if (error) return callback(error);

			if (this._dataLoading.totalRowCount && (this._dataLoading.type === 'paging' || this._dataLoading.type === 'incremental')) {
				if (this._dataLoading.type === 'paging'){
					// recalculate the totalPages
					this._dataLoading.totalPages = Math.ceil(this._dataLoading.totalRowCount / this._dataLoading.limit);
				}
				this._info(this._dataLoading);
			}

			if (result.rowCount > 0) {
				_.forEach(result.rows, (row) => {
					this['_' + row.action](row._id, row.data);
					/*if (row.action == 'added') {
						return this._added(row._id, row.data);
					}

					if (row.action == 'changed') {
						return this._changed(row._id, row.data);
					}

					if (row.action == 'removed') {
						return this._removed(row._id);
					}*/
				});
			}

			this.ready();

			return callback();
		});
	}

	reRun(callback) {
		if (this._running) {
			if (this._database._staledQueries.indexOf(this._queryId) == -1) {
				this._database._staledQueries.push(this._queryId);
			}
			return callback();
		}

		if (this._stoped) return callback();

		this._running = true;
		this.executeQuery(true/*reRun*/, (error, result) => {
			this._running = false;
			return callback(error, result);
		});
	}

	_getInfoCollection(){
		return this._queryObject.collectionName + '.$info';
	}

	_added(id, doc){
		if (this._salt){ // add salt to the document to find the docs from a specific subscription
			doc[this._salt] = true;
		}
		this.emit('added', id, doc);
	}

	_changed(id, doc){
		if (this._salt){ // add salt to the document to find the docs from a specific subscription
			doc[this._salt] = true;
		}
		this.emit('changed', id, doc);
	}

	_removed(id){
		this.emit('removed', id);
	}

	_info(data){
		this.emit('info', data);
	}

	ready(){
		this.emit('state', 'ready');
		this.emit('ready');
	}
}
module.exports.PGReactiveQuery = PGReactiveQuery;
