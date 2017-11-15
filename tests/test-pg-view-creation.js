'use strict';

const expect     		= require('chai').expect;
const _		     		= require('lodash');
const async				= require('async');

const { PGDatabase }	= require('../index');
const { PGTable }		= require('../lib/pg-table');
const { PGView }		= require('../lib/pg-view');

const DB_HOST = '127.0.0.1';
const DB_DATABASE = 'pureworkx';
const DB_USER = 'pureworkx';
const DB_PASSWORD = 'testtest';
const DB_PORT = /*version 10.0*/ 5433; // or port /*version 9.5*/ 5432;
const DB_MAX_CONNECTION = 90;

describe('PgView', function() {
	describe('CREATE VIEW support', function() {
		it('should async define a new View Object and create it on db if it doesnt exists', function(done) {
			let db = new PGDatabase({
				enablePooling: true,
				connect: {
					user: DB_USER,
					host: DB_HOST,
					database: DB_DATABASE,
					password: DB_PASSWORD,
					port: DB_PORT,
					max: DB_MAX_CONNECTION
				}
			});
			var PeopleView;

			async.waterfall([
				function(callback){
					db.connect(callback);
				},
				function(callback){
					PeopleView = db.View('v_people', {
						$select: {
							$from: 'people',
							$columns: [
								'first_name',
								'last_name'
							]
						}
					}, callback);
				},
				function(callback){
					db.getConnection((error, connection)=>{
						if (error) return callback(error);
						return callback(null, connection);
					});
				},
				function(connection, callback){
					connection.query('SELECT * FROM information_schema.tables WHERE table_name = $1', [PeopleView._baseName], (error, result)=>{
						if (error) {
							db.releaseConnection(connection);
							return callback(error);
						}

						expect(result.rowCount).to.equal(1);

						connection.query('DROP VIEW v_people;', (error, result)=>{
							if (error) {
								db.releaseConnection(connection);
								return callback(error);
							}
							return callback(null, connection);
						});
					});
				},
				function(connection, callback){
					db.releaseConnection(connection);
					return callback();
				}
			], (error)=>{
				db.end();
				return done(error);
			});
		});

	});
});
