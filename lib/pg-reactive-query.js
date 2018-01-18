'use strict';

const PGQuery			= require('./pg-query').PGQuery;

const async			= require('async');
const shortid 		= require('shortid');
const _				= require('lodash')

class PGReactiveQuery extends PGQuery {
	constructor(basetableObject, queryObject) {
		super(basetableObject, queryObject);

		this._queryId = shortid.generate() + '-' + new Date().getTime();
		this._salt = null;

		this._running = false;
		this._stoped = false;

		// setup the reactivity for this query -> default is set to always
		// if the user does'nt change explicit the reactivity option
		/* Valid option values are:
		'none'			// will disable reactivity after first time published query results
		'own' 			// get reactivity for changes made by own queries
		'manually'		// the query will me 'marked' as staled but won't rerun. The rerun should be executed by the application itself
		'always'		// will be reactively updated each time any change occours on the related tables
		function(){		// User-defined function that was called each time a change applies to a dependent table of this query
			if (this.isStaled || this.isRunning) return;

			setTimeout(()=>{
				this.staled();
			}, 3000);
		}*/
		this._reactivity = queryObject.options && queryObject.options.reactivity || 'always';

		this._dataLoading = {
			type: 'complete',  // options are "complete" = default, "paging", "incremental"
			// setup the limit of records for each page will have (using "paging")
			// or the number of records to load more using "incremental"
			limit: null,
			// holds the current Limit of the query wich will be recalculatet
			// by the method loadNext
			currentLimit: null,
			// holds the current offset of the query wich will be recalculatet
			// by the method gotoPage
			currentOffset: null,
			// it's the total rowcount of the query
			totalRowCount: null,
			// used for type "paging" and holds the currentPage
			currentPage: null,
			// used for type "paging" and holds the max pages info
			totalPageCount: null,
			// the query to calculate the totalRowCount
			query: null,
			// a deep clone of the user-defined query to change the $offset or $limit
			// by calling the methods loadNext, gotoPage and rebuild and run the query again
			originalQuery: null,
			// determines if the query is running or ready
			ready: null,
			// determines the actual state of the reactiveQuery
			// created, running, staled, ready
			state: 'created'
		}
	}

	staled(notificationPayload) {
		// if the query is'nt staled add it to the staled queries
		if ( ! this.isStaled ) {
			this._database._staledQueries.push({
				queryId: this._queryId,
				notificationPayload: notificationPayload
			});
		}

		this.markAsStaled(true/*set ready to false*/);
	}

	markAsStaled(setReadyToFalse){
		this._dataLoading.state = 'staled';
		if (setReadyToFalse) this._dataLoading.ready = false;

		this._info(this._dataLoading);
	}

	get isStaled() {
		let staledQueries = this._database._staledQueries;
		for (var i=0, max = staledQueries.length; i < max; i++){
			if (staledQueries[i].queryId === this._queryId) {
				return true;
			}
		}
		return false;
		//return (this._database._staledQueries.indexOf(this._queryId) > -1 || this._dataLoading.state === 'staled');
	}

	get isRunning() {
		return this._running;
	}

	_onReactivity(changesMadeByOwnQuery, data, callback) {
		let reactivityLevel = _.isString(this._reactivity) ? this._reactivity : 'userdefined';
		callback = callback || function(){};

		// if the reactivity is set to "none" we can directly exit here
		if (reactivityLevel == 'none') {
			return callback();
		}

		// get out if the reactivityLevel is set to own, but the
		// changes are made by another client, only set the
		// current state to "staled" and let the application define
		// to handle this
		if (reactivityLevel == 'own' && !changesMadeByOwnQuery) {
			this.markAsStaled();
			return callback();
		}

		// if the reactivityLevel is set to "own" and the changes are cause by on query operations
		// or reactivity is set to "always" we will directly reRun the query and publish the updated results
		if ((reactivityLevel == 'own' && changesMadeByOwnQuery) || reactivityLevel == 'always') {
			return this.reRun(callback);
		}

		// if the reactivityLevel is et to "manually" we only mark this query as staled and the
		// client / application can setup and ui feature to rerun this query
		if (reactivityLevel == 'manually') {
			this.markAsStaled();
			return callback();
		}

		// do everything the user wants to do
		if (reactivityLevel == 'userdefined') {
			if (_.isFunction(this._reactivity)){
				return this._reactivity.apply(this, [changesMadeByOwnQuery, data, callback]);
			}
		}

		// on all the other time we setup the query as staled an let the
		// staledRunner rerun the query
		this.staled(data);
		return callback();
	}

	destroy(done) {
		let superDestroy = super.destroy.bind(this);

		this._stoped = true;

		let _destroy = (callback) => {
			if (this._running) console.log('RUNNING!');

			this.removeAllListeners();

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
					// remove the query from the query-cache
					this._database._unCacheQuery(this._queryId, this._queryObject.tableDependencies);
					superDestroy();
					return callback();
				}
			], (error) => {
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

		this._dataLoading.state = 'stoped';
		this._info(this._dataLoading);
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
		delete query.$select.reactivity;

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

		// maybe $offset or $limit is null so we should delete this
		if (query.$select.$offset === null) delete query.$select.$offset;
		if (query.$select.$limit === null) delete query.$select.$limit;

		if (this._dataLoading.type === 'paging' || this._dataLoading.type === 'incremental'){
			// copy the query to use it later for the dataLoader option "paging" and "incremental"
			// to query to complete count of al rows
			this._dataLoading.query = _.cloneDeep(query);
			// now change the columns defined with "count(*)" as single column
			this._dataLoading.query.$select.$columns = { __totalRowCount__: { $count: '*' } };
			// and we have to remove any $sort, $orderBy, $limt and $offset, to get all rows for a total count
			delete this._dataLoading.query.$select.$orderBy;
			delete this._dataLoading.query.$select.$sort;
			delete this._dataLoading.query.$select.$orderBy;
			delete this._dataLoading.query.$select.$limit;
			delete this._dataLoading.query.$select.$offset;

			// setup defaults for paging
			if (this._dataLoading.type === 'paging'){
				// we always start with page 1
				this._dataLoading.currentPage = 1;
			}

			if (this._dataLoading.type === 'incremental'){
				this._dataLoading.currentLimit = this._dataLoading.limit;
			}
			this._dataLoading.stmt = this._database.sqlBuilder.build(this._dataLoading.query);
		}

		// keep the a clone of the json query to use later on loadNext and change the offset and limit
		// and force a rebuild of the new/current query
		this._dataLoading.originalQuery = _.cloneDeep(query);;

		this._rebuildAndExecQuery(query, callback);
	}

	_rebuildAndExecQuery(query, callback){
		this._running = true;

		this._dataLoading.ready = false;
		this._dataLoading.state = 'running';
		this._info(this._dataLoading);

		let stmt = this._database.sqlBuilder.build(query);

		// this._stmt will always be used by the executeQuery method
		// called by this function and any reRun() called by reactive changes
		this._stmt = {
			// after building the SQL Stamtent we have to inject this into the reactive query template
			sql: this._database._sqlTemplates.reactiveQuery
					.split('@@QueryId').join(this._queryId)
					.split('@@ownSchema').join(this._database._ownSchema)
					.replace('@@Query', stmt.sql),
			values: stmt.values
		}

		this.executeQuery(false/*rerun*/, (error, result)=>{
			this._running = false;

			this._dataLoading.ready = true;
			this._dataLoading.state = 'ready';
			this._info(this._dataLoading);

			this.ready();

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
		this._dataLoading.currentLimit += this._dataLoading.limit
		this._dataLoading.originalQuery.$select.$limit = this._dataLoading.currentLimit;

		this._rebuildAndExecQuery(this._dataLoading.originalQuery, callback);
	}

	gotoPage(pageNo/*starts with 1*/, callback){
		// this method could only be called, when dataLoading is set to "paging"
		// to load the next n records from the database
		if (this._dataLoading.type !== 'paging'){
			throw new Error('gotoPage() only available for loading option type "paging".');
		}

		// check if pageNo is valid
		if ((pageNo - 1) * this._dataLoading.limit > this._dataLoading.totalRowCount)
			return callback();

		// calc the new offset and make a rebuild of the query
		// we only have to change the offset and because of the rerun and reactivity table
		// the query automatically removes the "old" records and adds the records of the new page
		this._dataLoading.currentPage = pageNo;
		this._dataLoading.currentOffset = (pageNo - 1) * this._dataLoading.limit;
		this._dataLoading.originalQuery.$select.$offset = this._dataLoading.currentOffset;

		this._rebuildAndExecQuery(this._dataLoading.originalQuery, callback);
	}

	executeQuery(reRun, callback){
		super.executeQuery(reRun, (error, result) => {
			if (error) return callback(error);

			if (this._dataLoading.totalRowCount && (this._dataLoading.type === 'paging' || this._dataLoading.type === 'incremental')) {
				if (this._dataLoading.type === 'paging'){
					// recalculate the totalPageCount
					this._dataLoading.totalPageCount = Math.ceil(this._dataLoading.totalRowCount / this._dataLoading.limit);
				}
			}

			if (result.rowCount > 0) {
				_.forEach(result.rows, (row) => {
					this['_' + row.action](row._id, row.data);
				});
			}

			return callback();
		});
	}

	reRun(callback) {
		if (this._stoped) return callback();

		if (this._running) {
			this.staled();
			return callback();
		}

		this._running = true;
		this._dataLoading.state = 're-running';
		this._dataLoading.ready = false;
		this._info(this._dataLoading);

		this.executeQuery(true/*reRun*/, (error, result) => {
			this._running = false;

			this._dataLoading.ready = true;
			this._dataLoading.state = 'ready';
			this._info(this._dataLoading);

			this.ready();

			return callback(error, result);
		});
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
		this.emit('info', {
			type: data.type,
			limit: data.limit,
			currentLimit: data.currentLimit,
			currentOffset: data.currentOffset,
			totalRowCount: data.totalRowCount,
			currentPage: data.currentPage,
			totalPageCount: data.totalPageCount,
			// --> dont publish the query and originalQuery
			//query: null,
			//originalQuery: null,
			ready: data.ready,
			state: data.state
		});
	}

	ready(){
		this.emit('state', 'ready');
		this.emit('ready');
	}
}
module.exports.PGReactiveQuery = PGReactiveQuery;
