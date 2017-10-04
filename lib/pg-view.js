'use strict';

const { PGBaseTable } = require('./pg-base-table');
const _ = require('lodash');

class PGView extends PGBaseTable {
	constructor(mysqlDatabase, viewName, options) {
		super(mysqlDatabase, viewName, options);

		this.setViewDependencies(this._baseName);
		// after setting the view dependencies we have to check for the notify triggers
		// that are relevant to listen for changes
		_.forEach(this._tableDependencies, tableName => {
			this._initNotifyTriggerSync(this._database, tableName);
		});
	}

	setViewDependencies(viewName) {
		let depTables = [],
			connection = this._database.getConnectionSync();

		let getDepsFromInfoSchema = (objectName) => {
			var sql = '';
			sql += 'SELECT ';
			sql += '    tab.TABLE_NAME AS table_name, ';
			sql += '    tab.TABLE_TYPE AS table_type ';
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
			sql += 'AND tab.TABLE_CATALOG = current_database();';

			var depResult = this._database.querySync(connection, sql, [objectName]);

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
			}
		};

		getDepsFromInfoSchema(viewName);
		this._database.releaseConnection(connection);

		// overwrite the dependencies from viewName with the dependend tables
		this._tableDependencies = depTables;
	}
}
module.exports.PGView = PGView;
