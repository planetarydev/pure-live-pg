'use strict';

const PGBaseTable	= require('./pg-base-table').PGBaseTable;
const async			= require('async');
const _ 			= require('lodash');

class PGView extends PGBaseTable {
	constructor(database, viewName, options, callback) {
		super(database, viewName, options, (error) => {
			this._getViewDependencies(this._schemaName, this._baseName, (error, dependencies) => {
				if (error) return callback(error);

				this._tableDependencies = dependencies;
				// after setting the view dependencies we have to check for the notify triggers
				// that are relevant to listen for changes
				async.each(this._tableDependencies, (tableName, callback) => { // the tablename includes always the schema done by getViewDependencies
					database._initNotifyTrigger(tableName, (error, result) => {
						if (error) return callback(error);

						return callback();
					});
				}, callback);
			});
		});
	}

	_getViewDependencies(schemaName, viewName, callback) {
		let depTables = [];
		let getDepsFromInfoSchema = (connection, schemaName, objectName, callback) => {
			//https://stackoverflow.com/questions/4229468/getting-a-list-of-tables-that-a-view-table-depends-on-in-postgresql
			var sql = `
				SELECT
					ns_d.nspname 	AS "table_schema",
					cl_d.relname 	AS "table_name",
				    tab.table_type	AS "table_type"
				FROM
					pg_rewrite AS r
				JOIN pg_class AS cl_r ON r.ev_class=cl_r.oid
				JOIN pg_namespace AS ns_r ON cl_r.relnamespace = ns_r.oid
				JOIN pg_depend AS d ON r.oid=d.objid
				JOIN pg_class AS cl_d ON d.refobjid=cl_d.oid
				JOIN pg_namespace AS ns_d ON cl_d.relnamespace = ns_d.oid
				JOIN information_schema.tables tab ON tab.table_schema = ns_d.nspname AND tab.table_name = cl_d.relname
				WHERE
					cl_d.relkind IN ('r','v')
				AND ns_r.nspname = $1
				AND cl_r.relname = $2
				AND cl_d.relname != $3
				GROUP BY
					ns_d.nspname, cl_d.relname, tab.table_type
				ORDER BY
					ns_d.nspname, cl_d.relname, tab.table_type;`

			/*
			sql += 'SELECT ';
			sql += '    tab.TABLE_NAME		AS table_name, ';
			sql += '    tab.TABLE_SCHEMA	AS table_schema, ';
			sql += '    tab.TABLE_TYPE		AS table_type ';
			sql += 'FROM ';
			sql += '    INFORMATION_SCHEMA.TABLES AS tab ';
			sql += 'INNER JOIN INFORMATION_SCHEMA.VIEWS AS views ';
			sql += '    ON (views.VIEW_DEFINITION LIKE CONCAT(\'%(\', tab.TABLE_NAME, \'\n%\') OR ';
			sql += '        views.VIEW_DEFINITION LIKE CONCAT(\'%(\', tab.TABLE_NAME, \' %\') OR ';
			sql += '        views.VIEW_DEFINITION LIKE CONCAT(\'% \', tab.TABLE_NAME, \' %\') OR ';
			sql += '        views.VIEW_DEFINITION LIKE CONCAT(\'%"\', tab.TABLE_NAME, \'"%\')) ';
			sql += '    AND views.TABLE_CATALOG = tab.TABLE_CATALOG ';
			sql += 'WHERE ';
			sql += '    views.TABLE_NAME = $1 ';
			sql += 'AND views.TABLE_SCHEMA = $2 ';
			sql += 'AND tab.TABLE_CATALOG = current_database();';*/

			this._database.query(connection, sql, [/* $1 */schemaName, /* $2 */objectName, /* $3 */objectName], (error, result) => {
				if (error) return callback(error);

				async.each(result.rows, (dep, callback) => {
					if (dep.table_type == 'VIEW') {
						getDepsFromInfoSchema(connection, dep.table_schema, dep.table_name, callback);
					} else { // BASE TABLE
						// add only if the table-dependency is unknown at this time
						if (depTables.indexOf(dep.table_schema + '.' + dep.table_name) == -1) {
							depTables.push(dep.table_schema + '.' + dep.table_name);
						}
						return callback();
					}
				}, (error) => {
					if (error) return callback(error);

					return callback(null, depTables);
				});
			});

			/*
			var depResult = this._database.querySync(connection, sql, [objectName, schemaName]);

			for (var i=0, max=depResult.rows.length; i<max; i++){
				var dep = depResult.rows[i];
				// check wether the type is a VIEW or BASE TABLE
				// if it's a VIEW we have to get the dependencies of this view
				if (dep.table_type == 'VIEW'){
					getDepsFromInfoSchema(dep.table_name);
				} else { // BASE TABLE
					// add only if the table-dependency is unknown at this time
					if (depTables.indexOf(dep.table_name) == -1) {
						depTables.push(dep.table_name);
					}
				}
			}*/
		};

		async.waterfall([
			(callback) => {
				this._database.getConnection((error, connection)=>{
					return callback(error, connection);
				});
			},
			(connection, callback) => {
				getDepsFromInfoSchema(connection, this._schemaName, this._baseName, (error, tableDependencies) => {
					this._database.releaseConnection(connection);
					return callback(error, tableDependencies);
				});
			}
		], callback);
	}
}
module.exports.PGView = PGView;
