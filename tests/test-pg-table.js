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

describe('PGTable', function() {
	describe('Mongo option fields: {}', function() {
		it('should turn mongo fields-option to $columns', function(done) {
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
			var People;

			async.series([
				function(callback){
					db.connect(callback);
				},
				function(callback){
					People = db.Table('people', callback);
				},
				function(callback){
					var newOptions = People._checkAndTurnQueryOptions({
						fields: {
							first_name: 1,
							last_name: 1
						}
					});
					expect(newOptions.$columns.length).to.equal(3);
					expect(newOptions.$columns).to.include.members(['_id', 'first_name', 'last_name']);

					var newOptions = People._checkAndTurnQueryOptions({
						fields: {
							first_name: 0,
							last_name: 0
						}
					});
					expect(newOptions.$columns.length).to.equal(8);
					expect(newOptions.$columns).to.include.members(['_id', 'email', 'gender', 'ip_address', 'avatar', 'city', 'postalcode', 'street']);

					return callback();
				}
			], (error)=>{
				db.end();
				return done(error);
			});
		});

	});
});
