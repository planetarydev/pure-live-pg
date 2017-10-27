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
	describe('Transaction Support', function() {
		describe('beginTransaction, commit', function() {
			it('should async start a new transaction and commit changes', function(done) {
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
							last_name: 'Transaction'
						}, (error, result) => {
							if (error) return callback(error);
							return callback();
						});
					},
					function(callback){
						db.beginTransaction((error, transactionHandle)=>{
							if (error) return callback(error);
							return callback(null, transactionHandle);
						});
					},
					function(transactionHandle, callback){
						People.insert({
							_id: 'Mike001',
							first_name: 'Mike',
							last_name: 'Transaction'
						}, { transaction: transactionHandle }, (error, result) => {
							if (error) return callback(error);
							return callback(null, transactionHandle)
						});
					},
					function(transactionHandle, callback){
						People.insert({
							_id: 'Mike002',
							first_name: 'Rene',
							last_name: 'Transaction'
						}, { transaction: transactionHandle }, (error, result) => {
							if (error) return callback(error);
							return callback(null, transactionHandle)
						});
					},
					function(transactionHandle, callback){
						transactionHandle.commit((error, result) => {
							if (error) return callback(error);

							People.select({
								last_name: 'Transaction'
							}, (error, result) => {
								if (error) return callback(error);

								expect(result.rowCount).to.equal(2);

								return callback();
							});
						});
					}
				], (error)=>{
					db.end();
					return done(error);
				});
			});
		});

		describe('beginTransaction, rollback', function() {
			it('should async start a new transaction and rollback changes', function(done) {
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
							last_name: 'Transaction'
						}, (error, result) => {
							if (error) return callback(error);
							return callback();
						});
					},
					function(callback){
						db.beginTransaction((error, transactionHandle)=>{
							if (error) return callback(error);
							return callback(null, transactionHandle);
						});
					},
					function(transactionHandle, callback){
						People.insert({
							_id: 'Mike001',
							first_name: 'Mike',
							last_name: 'Transaction'
						}, { transaction: transactionHandle }, (error, result) => {
							if (error) return callback(error);
							return callback(null, transactionHandle)
						});
					},
					function(transactionHandle, callback){
						People.insert({
							_id: 'Mike002',
							first_name: 'Rene',
							last_name: 'Transaction'
						}, { transaction: transactionHandle }, (error, result) => {
							if (error) return callback(error);
							return callback(null, transactionHandle)
						});
					},
					function(transactionHandle, callback){
						transactionHandle.rollback((error, result) => {
							if (error) return callback(error);

							People.select({
								last_name: 'Transaction'
							}, (error, result) => {
								if (error) return callback(error);

								expect(result.rowCount).to.equal(0);
								return callback();
							});
						});
					}
				], (error)=>{
					db.end();
					return done(error);
				});
			});
		});

		describe('Reactivity during transactions', function() {
			it('should not wait for optimistic ui changes', function(done) {
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
				var peopleCount = 0;
				var reactivePeoples;

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
							last_name: 'Transaction'
						}, (error, result) => {
							if (error) return callback(error);
							return callback();
						});
					},
					function(callback){
						reactivePeoples = People.find({
							last_name: 'Transaction'
						}, {
							clientId: 'TestClient'
						});

						reactivePeoples.on('added', (id, doc) => {
							peopleCount++
						});

						reactivePeoples.run((error)=>{
							if (error) return callback(error);
							return callback();
						});
					},
					function(callback){
						db.beginTransaction((error, transactionHandle)=>{
							if (error) return callback(error);
							return callback(null, transactionHandle);
						});
					},
					function(transactionHandle, callback){
						People.insert({
							_id: 'Mike001',
							first_name: 'Mike',
							last_name: 'Transaction'
						}, { transaction: transactionHandle, clientId: 'TestClient' }, (error, result) => {
							if (error) return callback(error);
							return callback(null, transactionHandle)
						});
					},
					function(transactionHandle, callback){
						People.insert({
							_id: 'Mike002',
							first_name: 'Rene',
							last_name: 'Transaction'
						}, { transaction: transactionHandle, clientId: 'TestClient' }, (error, result) => {
							if (error) return callback(error);
							return callback(null, transactionHandle)
						});
					},
					function(transactionHandle, callback){
						transactionHandle.commit((error, result) => {
							if (error) return callback(error);

							People.select({
								last_name: 'Transaction'
							}, (error, result) => {
								if (error) return callback(error);

								expect(result.rowCount).to.equal(2);

								return callback();
							});
						});
					}
				], (error)=>{
					if (reactivePeoples){
						reactivePeoples.stop();
						reactivePeoples.destroy((error)=>{
							db.end();
							return done(error);
						});
					} else {
						db.end();
						return done(error);
					}
				});
			});
		});

	});
});
