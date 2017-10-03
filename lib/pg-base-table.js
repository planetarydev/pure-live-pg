'use strict';

const { PGQuery } 			= require('./pg-query');
const { PGReactiveQuery }	= require('./pg-reactive-query');
const { Synchron }			= require('synchron');

const md5	= require('md5');
const _ 	= require('lodash');

/**
 * @summary Use the `MySQLTable` to create a new Table instance to wroks with.
 * @namespace PGTable
 *
 * @before
 * # MySQLTable
 * <div class="sub-title">
 * 		Working with tables
 * </div>
 * Let's say we have the following table named `people`:
 *
 * ```sql
 * CREATE TABLE `people` (
 *      `people_id`     INT(11) 	NOT NULL 	AUTO_INCREMENT,
 *      `first_name`    VARCHAR(50),
 *      `last_name`     VARCHAR(50),
 *      `email`         VARCHAR(50),
 *      `gender`        VARCHAR(50),
 *      `ip_address`    VARCHAR(20),
 *      `avatar`        VARCHAR(255),
 *      `city`          VARCHAR(50) DEFAULT 'New York',
 *      `postalcode`    VARCHAR(50),
 *      `street`        VARCHAR(50),
 *
 *      PRIMARY KEY (`people_id`)
 * )  ENGINE=INNODB AUTO_INCREMENT=1 DEFAULT CHARSET=UTF8;
 * ```
 * ## Table declarations in meteor
 *
 * Define each of the table you need to work with in your meteor app with `MySQLDatabase.table()`.
 * On the server-side meteor will work with the mysql-database directly. You can insert, update or delete records or query/select data as you want.
 * On top of this you can publish records from a table like you do with a mongo-collection.
 *
 * On the client-side the data will be stored in a minimongo-collection and wrapped by the client's MySQLTable object.
 *
 * ```javascript
 * // define your people-table from your MySQL-Database to use on server and client
 * export const People = MyDb.table('people');
 * ```
 *
 * ## Using an alias
 *
 * Like the example above - when you publish any data from this People-table - it still ends in a client-side collection named *people*.
 * Each publication done from meteor will merged in the same collection on the client-side. If you need explicit another client collection-store
 * you can use the option `alias.`.
 *
 * ```javascript
 * // define your people-table from your MySQL-Database to use
 * export const MyPeople = MyDb.table('people', {
 *      alias:'my-people' // each published data end in a client-side collection named **my-people**
 * });
 * ```
 * > **Be sure using the same declaration on server and client side!**
 * >
 * > The recommended way to do this is having an import-directory where you declare all your tables and import these on the client- and server-side.
 * > From the example above you ca easily import the People and MyPeople declaration.
 * >
 * > ```javascript
 * > // Import your declaration from the import or api directory
 * > import { People }      from '../../imports/api/people';
 * > ...
 * > ```
 * *
 * @param  	{Object} 		mysqlDatabase   Specifies the instance of your database you are connected to.
 * @param   {String}        tableName       Specifies the name of the table as created in the MySQL database.
 * @param   {Object}        [options]       Options for the new instance of the MySQLTable.
 * | Option              | Type     | Description                                                                   |
 * |---------------------|----------|-------------------------------------------------------------------------------|
 * | `alias`             | String   | Defines an alias for this table. |
 * | `as`                | String   | See alias |
 * | `idGenerator`       | Function | **Server-side only** A callback function to define an `_id` column if the table does not provide a column named `_id` or `id`. |
 * | `removeUnknownCols` | Boolean  | **Server-side only** If `true` this option will remove all unknown columns used within an object before trying to insert or update a record. |
 *
 * @after
 *
 * ## Define an `idGenerator`
 *
 * Each record that you publish to a client needs a column named `_id` which is the unique
 * identifier for this document (record) on the client side.
 *
 * In best your table will provide such a column. If not, the default idGenerator
 * will use the primaryKey-definition and concats all columns from the primary key and hashes them to a new `_id` value.
 *
 * If you need to support another algorithm you can pass the option idGenerator.
 *
 * ```javascript
 * export const People = MyDb.table('people', {
 *      idGenerator: function(doc){
 *          // return the new _id for each document
 *          return 'X' + doc.people_id;
 *      }
 * });
 * ```
 *
 *
 */
class PGBaseTable {
	constructor(mysqlDatabase, baseName, options) {
		this._baseName = baseName;
		this._database = mysqlDatabase;
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

		// listen to changes
		this._database._registerListner( (eventName, tablename, rows, evt) => {
			// check whether these changes are relevant to this view
			if (this._tableDependencies.indexOf(tablename) == -1) {
				return;
			}

			// the changes are relevant, so rerun each query
			var queries = Object.keys(this._database._queryCache);
			for (var i=0, max=queries.length; i<max; i++) {
				var query = this._database._queryCache[queries[i]];
				query.reRun(eventName, tablename, rows, evt);
			}
		});

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


	/**
     * _getColumnInformations
     * Returns all columns of this table or view that are declared in the database.
     *
     * @param  {String} baseName    Name of the table or view
     * @return {Object} Object with all columns like the sample:
     * {
     *      people_id: {
     *          columnName: 'people_id',
     *          isNullable: false,
     *          dataType: 'INT',
     *          characterMaximumLength: 11
     *      },
     *      first_name: {
     *          columnName: 'first_name',
     *          isNullable: true,
     *          dataType: 'VARCHAR',
     *          characterMaximumLength: 50
     *      },
     *      ...
     * }
     */
	_getColumnInformations(baseName){
		let connection = this._database.getConnectionSync();
		let sql=`SELECT
                	column_name                AS columnName,
                    is_nullable                AS isNullable,
                    data_type                  AS dataType,
                    character_maximum_length   AS characterMaximumLength
                FROM
                    information_schema.columns
                WHERE
                    table_name = $1
                AND table_catalog = current_database();`;

		var results = this._database.querySync(connection, sql, [baseName]);
		this._database.releaseConnection(connection);

		// transform the columns array into an object
		var transformedResult = {};
		for (var i=0, max=results.length;i<max;i++){
			var column = results[i];
			transformedResult[column.columnName] = column;
		}
		return transformedResult;
	}

	/**
     * @method find
     * @memberOf MySQLTable
     * @locus Server
     *
     * @summary Creates a reactive query and publishes the results to the client which subscribes to.
     *
     *
     * @param  	{Object} 		selector     Specifies the selector for the query.
     * @param   {Object}        [options]    Optional. Specifies the options for this query.
     * | Option         | Type     | Description                                                                   |
     * |----------------|----------|-------------------------------------------------------------------------------|
     * | `$columns`     | Array    | Specifies the columns that are within the select statement |
     * | `$sort`        | Array    | Specifies the ORDER BY - Clause |
     * | `$offset`      | Number   | Specifies the OFFSET within the SELECT statement |
     * | `$limit`       | Number   | Specifies the LIMIT within the SELECT statement |
     * | `$pagination`  | Object   | **Special Package option :-)** Enables auto pagination for the client subscription
     * | `$lazy`        | Object   | **Special Package option :-)** Enables the lazyload-Feature for the client subscription
     *
     * @after
     *
     * ## Publishing data
     *
     * __server.js__
     * ```javascript
     * import { People }       from '../api/people/people.js';
     *
     * Meteor.publish('People.all', function() {
     *      return People.find({});
     * });
     * ```
     *
     * __client.js__
     * ```javascript
     * import { People }       from '../api/people/people.js';
     *
     * Template.hello.onCreated(function () {
     *      // subscribe to People.all
     *      this.people = People.subscribe('People.all');
     * });
     *
     * Template.hello.onDestroyed(function () {
     *      this.people.stop();
     * });
     *
     * Template.hello.helpers({
     *      allPeoples(){
     *          return Template.instance().people.find({});
     *      }
     * });
     * ```
     *
     * __client.html__
     * ```html
     * <template name="hello">
     *      <h1>All People</h1>
     *      <ul>
     *          {{#each people in allPeoples}}
     *              <li>
     *                  {{people.last_name}}, {{people.first_name}}
     *              </li>
     *          {{/each}}
     *      </ul>
     * </template>
     * ```
     */
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

	/**
     * @method select
     * @memberOf MySQLTable
     * @locus Server
     *
     * @summary Executes a query with the given selector, all passed options and returns the results without beeing reactive.
     *
     * @param  	{Object} 		selector       Specifies the selector for the query.
     * @param   {Object}        [options]    Optional. Specifies the options for this query.
     * | Option         | Type     | Description                                                                   |
     * |----------------|----------|-------------------------------------------------------------------------------|
     * | `$columns`     | Array    | Specifies the columns that are within the select statement |
     * | `$sort`        | Array    | Specifies the ORDER BY - Clause |
     * | `$offset`      | Number   | Specifies the OFFSET within the SELECT statement |
     * | `$limit`       | Number   | Specifies the LIMIT within the SELECT statement |
     */
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
