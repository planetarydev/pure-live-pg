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

describe('PGReactiveQuery', function() {
	describe('reactivity "none"', function() {
		it('should publish only first results', function(done) {
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
				peopleCount = 0,
				peopleChanged = 0;

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
						_id: { $startsWith: 'reactivity' }
					}, callback);
				},
				function(callback){
					reactivePeople = People.find({
						first_name: { $eq: 'Koo' }
					}, {
						clientId: 'testClient',
						reactivity: 'none'
					});

					reactivePeople.on('added', (id, row) => {
						peopleCount++;
					});

					reactivePeople.on('removed', (id) => {
						peopleCount--;
					});

					reactivePeople.on('changed', (id, row) => {
						peopleChanged++;
					});

					reactivePeople.on('info', (infos) => {
						peopleInfos = infos;
						//console.log('onInfo:', infos);
						//console.log('StaledQueries:', db._staledQueries);
					});

					reactivePeople.on('ready', () => {
						if (step == 0){
							expect(peopleCount).to.equal(2);

							return callback(null);
						}
						else if (step == 1){
							expect(peopleChanged).to.equal(2);
						}
					});

					reactivePeople.run();
				},
				function(callback){
					step = 1;
					// should update a people, but with out an reactive effect,
					// because the reactivity is set to "none"
					People.update({ first_name: 'Koo' }, {
						city: 'Berlin'
					}, (error, result)=>{
						expect(peopleChanged).to.equal(0);
						return callback();
					});
				},
				function(callback){
					step = 3;
					expect(peopleChanged).to.equal(0);
					return callback();
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

	describe('reactivity "own"', function() {
		it('should publish results and rerun when changes made by own client', function(done) {
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
				peopleCount = 0,
				peopleChanged = 0;

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
						_id: { $startsWith: 'reactivity' }
					}, callback);
				},
				function(callback){
					reactivePeople = People.find({
						first_name: { $eq: 'Koo' }
					}, {
						clientId: 'testClient',
						reactivity: 'own'
					});

					reactivePeople.on('added', (id, row) => {
						peopleCount++;
					});

					reactivePeople.on('removed', (id) => {
						peopleCount--;
					});

					reactivePeople.on('changed', (id, row) => {
						peopleChanged++;
					});

					reactivePeople.on('info', (infos) => {
						peopleInfos = infos;
						//console.log('onInfo:', infos);
						//console.log('StaledQueries:', db._staledQueries);
					});

					reactivePeople.on('ready', () => {
						if (step == 0){
							expect(peopleCount).to.equal(2);

							return callback(null);
						}
						else if (step == 1){
							expect(peopleCount).to.equal(2);
							expect(peopleChanged).to.equal(0);
						}
					});

					reactivePeople.run();
				},
				function(callback){
					step = 1;
					// should update a people, but with out an reactive effect,
					// because the reactivity is set to "own" and we dont specify the client-id
					People.update({ first_name: 'Koo' }, {
						city: 'Munich'
					}, (error, result)=>{
						expect(peopleCount).to.equal(2);
						expect(peopleChanged).to.equal(0);
						return callback();
					});
				},
				function(callback){
					step = 2;
					expect(peopleChanged).to.equal(0);
					// now make an update with the own-client id
					People.update({ first_name: 'Koo' }, {
						city: 'Hamburg'
					}, { clientId: 'testClient' }, (error, result)=>{
						expect(peopleCount).to.equal(2);
						expect(peopleChanged).to.equal(2);
						return callback();
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

	describe('reactivity "manually"', function() {
		it('should publish results and not rerun on changes, get an info that the query is staled.', function(done) {
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
				peopleCount = 0,
				peopleChanged = 0;

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
						_id: { $startsWith: 'reactivity' }
					}, callback);
				},
				function(callback){
					reactivePeople = People.find({
						first_name: { $eq: 'Koo' }
					}, {
						clientId: 'testClient',
						reactivity: 'manually'
					});

					reactivePeople.on('added', (id, row) => {
						peopleCount++;
					});

					reactivePeople.on('removed', (id) => {
						peopleCount--;
					});

					reactivePeople.on('changed', (id, row) => {
						peopleChanged++;
					});

					reactivePeople.on('info', (infos) => {
						peopleInfos = infos;
					});

					reactivePeople.on('ready', () => {
						if (step == 0){
							expect(peopleCount).to.equal(2);

							return callback(null);
						}
						else if (step == 1){
							expect(peopleCount).to.equal(2);
							expect(peopleChanged).to.equal(0);
							expect(peopleInfos.state).to.equal('staled');
							expect(peopleInfos.ready).to.equal(true);
						}
					});

					reactivePeople.run();
				},
				function(callback){
					step = 1;
					// should update a people, but with out an reactive effect,
					// because the reactivity is set to "own" and we dont specify the client-id
					People.update({ first_name: 'Koo' }, {
						city: 'Munich'
					}, (error, result)=>{
						expect(peopleCount).to.equal(2);
						expect(peopleChanged).to.equal(0);
						return callback();
					});
				},
				function(callback){
					step = 2;
					reactivePeople.reRun((error)=>{
						expect(peopleInfos.state).to.equal('ready');
						expect(peopleInfos.ready).to.equal(true);
						return callback();
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

	describe('reactivity "userdefined"', function() {
		it('should set to staled after waiting 3 seconds', function(done) {
			this.timeout(5000);

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
				peopleCount = 0,
				peopleChanged = 0;

			async.series([
				function(callback){
					db.connect(callback);
				},
				function(callback){
					People = db.Table('people', callback);
				},
				/*function(callback){
					// remove people maybe inserted on the last test-run
					People.remove({
						_id: { $startsWith: 'reactivity' }
					}, callback);
				},*/
				function(callback){
					reactivePeople = People.find({
						first_name: { $eq: 'Koo' }
					}, {
						clientId: 'testClient',
						reactivity: function(changesMadeByOwnQuery, data, callback){
							if (this.isStaled || this.isRunning) return callback();

							setTimeout(()=>{
								//console.log('SET staled in userdef func.');
								this.staled();
								return callback();
							}, 3000);
						}
					});

					reactivePeople.on('added', (id, row) => {
						peopleCount++;
					});

					reactivePeople.on('removed', (id) => {
						peopleCount--;
					});

					reactivePeople.on('changed', (id, row) => {
						peopleChanged++;
					});

					reactivePeople.on('info', (infos) => {
						peopleInfos = infos;
					});

					reactivePeople.on('ready', () => {
						if (step == 0){
							expect(peopleCount).to.equal(2);

							return callback();
						}
						else if (step == 1){
							expect(peopleCount).to.equal(2);
							expect(peopleChanged).to.equal(0);
							expect(peopleInfos.state).to.equal('ready');
							expect(peopleInfos.ready).to.equal(true);
						}
					});

					reactivePeople.run();
				},
				function(callback){
					step = 1;
					// should update a people, but with out an reactive effect,
					// because the reactivity is set to "own" and we dont specify the client-id
					People.update({ first_name: 'Koo' }, {
						city: 'Cologne'
					}, (error, result)=>{
						setTimeout(()=>{
							// after waiting 2,5 second there should no changes happend
							// because the query will be staled after 3 seconds
							expect(peopleCount).to.equal(2);
							expect(peopleChanged).to.equal(0);
							expect(peopleInfos.state).to.equal('ready');
							expect(peopleInfos.ready).to.equal(true);

							return callback();
						}, 2500);
					});
				},
				function(callback){
					step = 2;
					setTimeout(()=>{
						expect(peopleCount).to.equal(2);
						expect(peopleChanged).to.equal(2);
						expect(peopleInfos.state).to.equal('ready');
						expect(peopleInfos.ready).to.equal(true);

						return callback();
					}, 1000); // --> after this 1 sec. we are waiting 3,5 sec. since the update
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
