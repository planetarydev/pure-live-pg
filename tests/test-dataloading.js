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
	describe('load incremental', function() {
		it('should return 10, then 10 more records from 1000', function(done) {
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
			var People,
				reactivePeople,
				loadNext = 0,
				peopleInfos,
				peopleCount = 0;

			async.series([
				function(callback){
					db.connect(callback);
				},
				function(callback){
					People = db.Table('people', callback);
				},
				function(callback){
					// remove people maybe inserted on the last test-run
					People.remove({
						_id: { $startsWith: 'dataLoading' }
					}, callback);
				},
				function(callback){
					reactivePeople = People.find({
						first_name: { $startsWith: 'A' }
					}, {
						loading: {
							type: 'incremental',
							limit: 10
						},
						clientId: 'testClient'
					});

					reactivePeople.on('added', (id, row) => {
						peopleCount++;
					});

					reactivePeople.on('removed', (id) => {
						peopleCount--;
					});

					reactivePeople.on('info', (infos) => {
						peopleInfos = infos;
						//console.log('onInfo:', infos);
						//console.log('StaledQueries:', db._staledQueries);
					});

					reactivePeople.on('ready', () => {
						//setTimeout(()=>{
							if (loadNext == 0){
								expect(peopleCount).to.equal(10);
								expect(peopleInfos.totalRowCount).to.equal(75);

								return callback(null);
							}
							else if (loadNext == 1){
								expect(peopleCount).to.equal(20);
								expect(peopleInfos.totalRowCount).to.equal(75);
							}
						//}, 1000);
					});

					reactivePeople.run();
				},
				function(callback){
					loadNext = 1;
					// should load 10 people more
					reactivePeople.loadNext(callback);
				},
				function(callback){
					loadNext = 2;
					// insert a new people should increase the totalRowCount + 1
					// but should'nt load more people than currently loaded
					People.insert({
						_id: 'dataLoading1',
						first_name: 'A-Data',
						last_name: 'Loading'
					}, {clientId: 'testClient'}, (error, result) => {
						expect(peopleInfos.totalRowCount).to.equal(76);
						expect(peopleCount).to.equal(20);

						return callback(error, result);
					});
				},
				function(callback){
					async.timesSeries(6, function(n, next){
						loadNext++;
						reactivePeople.loadNext(next);
					}, function(error, results){
						expect(peopleInfos.totalRowCount).to.equal(76);
						expect(peopleCount).to.equal(76);

						return callback(error);
					});
				},
				function(callback){
					reactivePeople.stop()
					reactivePeople.destroy(callback);
				}
			], (error)=>{
				db.end();
				return done(error);
			});
		});
	});

	describe('load paging', function() {
		it('should always have 10 records published from 1000', function(done) {
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
			var People,
				reactivePeople,
				step = 0,
				peopleInfos,
				peopleCount = 0;

			async.series([
				function(callback){
					db.connect(callback);
				},
				function(callback){
					People = db.Table('people', callback);
				},
				function(callback){
					// remove people maybe inserted on the last test-run
					People.remove({
						_id: { $startsWith: 'dataLoading' }
					}, callback);
				},
				function(callback){
					reactivePeople = People.find({
						first_name: { $startsWith: 'A' }
					}, {
						loading: {
							type: 'paging',
							limit: 10
						},
						clientId: 'testClient'
					});

					reactivePeople.on('added', (id, row) => {
						peopleCount++;
					});

					reactivePeople.on('removed', (id) => {
						peopleCount--;
					});

					reactivePeople.on('info', (infos) => {
						peopleInfos = infos;
					});

					reactivePeople.on('ready', () => {
						if (step == 0){
							expect(peopleCount).to.equal(10);
							expect(peopleInfos.totalRowCount).to.equal(75);
							expect(peopleInfos.totalPageCount).to.equal(8);

							return callback(null);
						}
						else if (step == 1){
							expect(peopleCount).to.equal(10);
							expect(peopleInfos.totalRowCount).to.equal(75);
							expect(peopleInfos.totalPageCount).to.equal(8);
						}
					});

					reactivePeople.run();
				},
				function(callback){
					step = 1;
					// should goto Page 5 and get people-records from 50 to 59
					reactivePeople.gotoPage(5, callback);
				},
				function(callback){
					step = 2;
					// insert 5 new people should increase the totalPages + 1
					// but should'nt load more people than currently loaded
					async.timesSeries(6, function(n, next){
						People.insert({
							_id: 'dataLoading' + n + 1, // +1 because dataLoading1 always exists
							first_name: 'A-Data',
							last_name: 'Loading'
						}, {
							clientId: 'testClient'
						}, next);
					}, function(error, results){
						expect(peopleInfos.totalPageCount).to.equal(9);
						expect(peopleInfos.totalRowCount).to.equal(81);
						expect(peopleCount).to.equal(10);

						return callback(error);
					});
				},
				function(callback){
					reactivePeople.stop()
					reactivePeople.destroy(callback);
				}
			], (error)=>{
				db.end();
				return done(error);
			});
		});

	});
});
