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

describe('PgDatabase', function() {
	describe('Upsert Support', function() {
		it('should insert if not exists and update if confict is given', function(done) {
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

			async.waterfall([
				function(callback){
					db.connect(callback);
				},
				function(callback){
					People = db.Table('people', callback);
				},
				function(callback){
					// remove all people named transaction to run the tests twice
					People.remove({
						_id: 'Upsert001'
					}, (error, result) => {
						if (error) return callback(error);
						return callback();
					});
				},
				function(callback){
					People.upsert('_id', {
						_id: 'Upsert001',
						first_name: 'Mike',
						last_name: 'Upsert'
					}, (error, result) => {
						if (error) return callback(error);
						return callback()
					});
				},
				function(callback){
					People.select({ _id: 'Upsert001' }, (error, result) => {
						if (error) return callback(error);

						expect(result.rowCount).to.equal(1);
						expect(result.rows[0].last_name).to.equal('Upsert');
						return callback();
					});
				},
				function(callback){
					People.upsert('_id', {
						_id: 'Upsert001',
						first_name: 'Mike',
						last_name: 'Upsert-Test'
					}, (error, result) => {
						if (error) return callback(error);
						return callback()
					});
				},
				function(callback){
					People.select({ _id: 'Upsert001' }, (error, result) => {
						if (error) return callback(error);

						expect(result.rowCount).to.equal(1);
						expect(result.rows[0].last_name).to.equal('Upsert-Test');
						return callback();
					});
				}
			], (error)=>{
				db.end();
				return done(error);
			});
		});

	});
});
