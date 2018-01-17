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
	describe('connect', function() {
		describe('with Pooling', function() {
			it('should async connect and disconnect to the database', function(done) {
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

				expect(db).to.be.instanceOf(PGDatabase);

				db.connect( (error) => {
					if (!error){
						db.end();
						return done();
					} else {
						throw new Error(error);
					}
				});
			});


			it('should get a new client connection from the pool and query directly', function(done) {
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
				db.connect( (error) => {
					if (error) return done(error);

					db.getConnection((error, connection)=>{
						if (error) return done(error);

						var result = connection.query('SELECT people.email AS email FROM people WHERE _id = \'6b0b584b-b036-11e7-b16a-bc307d530814\'', function(error, result){
							db.releaseConnection(connection);
							db.end();

							expect(result.rows[0].email).to.equal('jrysdaleke@vistaprint.com');
							done();
						});
					});
				});
			});
		});

		describe('single connection - without Pooling', function() {
			it('should async connect and disconnect to the database', function(done) {
				let db = new PGDatabase({
					enablePooling: false,
					connect: {
						user: DB_USER,
						host: DB_HOST,
						database: DB_DATABASE,
						password: DB_PASSWORD,
						port: DB_PORT,
						max: DB_MAX_CONNECTION
					}
				});

				expect(db).to.be.instanceOf(PGDatabase);

				db.connect( (error) => {
					if (!error){
						db.end();
						return done();
					} else {
						throw new Error(error);
					}
				});
			});


			it('should get a new client connection from the pool and query directly', function(done) {
				let db = new PGDatabase({
					enablePooling: false,
					connect: {
						user: DB_USER,
						host: DB_HOST,
						database: DB_DATABASE,
						password: DB_PASSWORD,
						port: DB_PORT,
						max: DB_MAX_CONNECTION
					}
				});

				db.connect( (error) => {
					if (!error){
						db.getConnection((error, connection) => {
							if (error) return done(error);

							var sql = 'SELECT people.email AS email FROM people WHERE _id = \'6b0b584b-b036-11e7-b16a-bc307d530814\'';
							var result = connection.query(sql, function(error, result){
								db.releaseConnection(connection);
								db.end();

								expect(result.rows[0].email).to.equal('jrysdaleke@vistaprint.com');
								done();
							});
						});
					} else {
						throw new Error(error);
					}
				});
			});
		});
	});


	describe('query-Preperators', function() {
		it('should define a new query preparator', function(done) {
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

			db.queryPreparations(function(connection, query){
				// change the query to SELECT 1 as test
				expect(query.sql).to.equal('SELECT NOW() as test');
				query.sql = 'SELECT 1 as test';
			});

			expect(db._queryPreparators.length).to.equal(1);

			db.connect( (error) => {
				if (error) return done(error);

				db.getConnection((error, connection) => {
					if (error) return done(error);

					db.wpQuery(connection, 'SELECT NOW() as test', [], (error, result) => {
						db.releaseConnection(connection);
						db.end();

						if (error) return done(error);

						expect(result.rows[0].test).to.equal(1);
						return done();
					});
				});
			});
		});
	});

	describe('new Table', function() {
		it('should return a table object', function(done) {
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

			db.connect((error) => {
				if (error) return done(error);

				var People = db.Table('people', (error) => {
					if (error) return done(error);

					expect(People).to.be.instanceOf(PGTable);
					db.end();
					done();
				});
			});
		});

		it('should select one record from people', function(done) {
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

			db.connect((error) => {
				var People = db.Table('people', (error) => {
					People.select({
						email: 'jrysdaleke@vistaprint.com'
					}, {
						$columns:['_id', 'email']
					}, (error, result) => {
						if (error) return done(error);

						expect(result.rowCount).to.equal(1);
						expect(result.rows[0].email).to.equal('jrysdaleke@vistaprint.com');

						db.end();
						done();
					});
				});
			});
		});

		it('should reactively get records from people', function(done) {
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

			db.connect((error) => {
				var People = db.Table('people', (error) => {

					var reactiveQuery = People.find({
						first_name: 'Koo'
					});

					var peopleCounter = 0;
					reactiveQuery.on('added', (id, row) => {
						peopleCounter++;
					});

					reactiveQuery.on('state', (currentState) => {
						if (currentState == 'ready'){
							expect(peopleCounter).to.equal(2);

							reactiveQuery.destroy(function(error){
								db.end();
								done();
							});
						}
					});

					reactiveQuery.run();
				});
			});
		});

		it('should delete one record from people', function(done) {
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

			db.connect((error) => {
				var People = db.Table('public.people', (error) => {
					if (error) return done(error);

					People.remove({
						$or: [
							{ _id: { $startsWith: 'abcdef' } },
							{ first_name: 'Tester' }
						]
					}, (error, result) => {
						if (error) return done(error);
						db.end();
						done();
					});
				});
			});
		});

		it('should insert one record into people', function(done) {
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

			db.connect((error) => {
				var People = db.Table('public.people', (error) => {
					if (error) return done(error);

					People.insert({
						_id: 'abcdef0',
						first_name: 'Tester',
						last_name: 'Test',
						email: 'tester0.test@test.com.de'
					}, (error, result) => {
						if (error) return done(error);
						db.end();
						done();
					});
				});
			});
		});

		it('should insert multiple records into people', function(done) {
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

			db.connect((error) => {
				var People = db.Table('public.people', (error) => {
					if (error) return done(error);

					People.insert([
						{
							_id: 'abcdef1',
							first_name: 'Tester',
							last_name: 'Test',
							email: 'tester1.test@test.com.de'
						}, {
							_id: 'abcdef2',
							first_name: 'Tester',
							last_name: 'Test',
							email: 'tester2.test@test.com.de'
						}
					], (error, result) => {
						if (error) return done(error);

						expect(result.rowCount).to.equal(2);

						db.end();
						done();
					});
				});
			});
		});

		it('should update records from people', function(done) {
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

			db.connect((error) => {
				var People = db.Table('public.people', (error) => {
					if (error) return done(error);

					People.update({
						last_name: 'Test'
					}, {
						last_name: 'Test updated'
					}, (error, result) => {
						if (error) return done(error);

						expect(result.rowCount).to.equal(3);
						db.end();
						done();
					});
				});
			});
		});

		it('should reactively update one record from people and get a change event', function(done) {
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

			db.connect((error) => {
				var People = db.Table('public.people', (error) => {
					if (error) return done(error);

					var reactiveQuery = People.find({
						first_name: 'Tester'
					});

					var changeCounter = 0;
					reactiveQuery.on('changed', (id, row) => {
						changeCounter++;

						expect(row.email).to.equal('newmailadr0@gmail.com');

						if (changeCounter == 3) {
							reactiveQuery.destroy( () => {
								db.end();
								done();
							});
						}
					});

					var firstTime = true;
					reactiveQuery.on('state', (currentState) => {
						if (currentState == 'ready'){
							if (!firstTime) return;
							firstTime = false;

							People.update({ first_name: 'Tester'}, {
								email: 'newmailadr0@gmail.com'
							}, function(error, result){
								expect(error).to.equal(null);
								expect(result.rowCount).to.equal(3);
							});
						}
					});

					reactiveQuery.run();
				});
			});
		});
	});

	describe('new View', function() {
		it('should reactively work like a table', function(done) {
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

			var counter= 0;
			db.connect((error) => {
				var Hobbies = db.Table('public.hobbies', (error) => {
					if (error) return done(error);

					async.series([
						(callback)=>{
							// to run test twice remove the old inserted hobbies
							Hobbies.remove({
								$or: [
									{ _id: 'hobby_id0003' },
									{ _id: 'hobby_id0004' },
									{ hobby: 'Performancetracking' },
								]
							}, callback);
						},
						(callback)=>{
							var HobbiesByPeople = db.View('hobbies_by_people', (error) => {
								if (error) {
									return callback(error);
								}

								// check view-dependecies
								expect(HobbiesByPeople._tableDependencies.length).to.be.equal(2);
								expect(HobbiesByPeople._tableDependencies).to.include('public.people');
								expect(HobbiesByPeople._tableDependencies).to.include('public.hobbies');

								var reactiveHobbies = HobbiesByPeople.find({
									hobby: 'Football'
								});

								var hobbyCounter = 0,
									readyCounter = 0;
								reactiveHobbies.on('added', (id, row) => {
									hobbyCounter++;
								});

								reactiveHobbies.on('state', (currentState) => {
									if (currentState == 'ready'){
										readyCounter++;
										if (readyCounter == 1) expect(hobbyCounter).to.be.equal(1);
										if (readyCounter == 2) {
											expect(hobbyCounter).to.be.equal(2);

											reactiveHobbies.destroy((error)=>{
												callback()
											});
										}
									}
								});

								reactiveHobbies.run();

								Hobbies.insert([
									{
										_id: 'hobby_id0003',
										people_id: '6b0b5907-b036-11e7-b16a-bc307d530814',
										hobby: 'Football'
									}, {
										_id: 'hobby_id0004',
										people_id: '6b0b584b-b036-11e7-b16a-bc307d530814',
										hobby: 'Motorbike'
									}
								], (error, result) => {
									//console.log('INSERT:', error, result);
								});

							});

							expect(HobbiesByPeople).to.be.instanceOf(PGView);
						}
					], (error)=>{
						db.end();
						return done(error);
					});
				});
			});
		});
	});


	describe('Test Workload', function() {
		let globalDb = new PGDatabase({
			enablePooling: true,
			connect: {
				user: DB_USER,
				host: DB_HOST,
				database: DB_DATABASE,
				password: DB_PASSWORD,
				port: DB_PORT,
				max: 90 //DB_MAX_CONNECTION
			}
		});
		let People, peopleQuery = [];
		let Hobbies, hobbyQuery = [];
		let HobbiesByPeople, hobbiesByPeopleQuery = [];
		let hobbyDeleteCounter = 0,
			hobbyByPeopleDeleteCounter = 0;
		let globalStatementCount = 0;

		it('should Setup 100 Tables, Views and queries of each Object', function(done) {
			this.timeout(15000);

			globalDb.connect(error => {
				if (error) return done(error);

				async.series([
					(callback)=>{
						Hobbies = globalDb.Table('hobbies', callback);
					},
					(callback)=>{
						HobbiesByPeople = globalDb.View('hobbies_by_people', callback);
					},
					(callback)=>{
						People = globalDb.Table('people', (error) => {
							if (error) return done(error);

							People.select({}, {
								$columns: ['first_name'],
								$groupBy: ['first_name'],
								$having: { $: { $count: 'first_name', $gt: 1 } }
							}, (error, result)=>{
								//console.log(error, result);
								var fnCnt = 0;
								var count = 0;
								async.whilst(
								    function() { return count < 100; },
								    function(callback) {
								        count++;

										// create 100 times a different select
										fnCnt++;
										if (fnCnt >= result.rowCount) fnCnt = 0;

										var cid = 'cid' + count; // dummy client/session id
										var q = People.find({
											first_name: result.rows[fnCnt].first_name // result.rows[(Math.random() * result.rowCount) | 0].first_name
										}, {
											clientId: cid
										});
										q.on('added', (id, row)=>{
											var hq = Hobbies.find({
												people_id: id
											}, {
												clientId: cid
											});
											hq.on('added', (id, row)=>{});
											hq.on('changed', (id, row)=>{});
											hq.on('removed', (id)=>{
												hobbyDeleteCounter++;
											});
											hq.on('state', (currentState)=>{});
											hq.run();
											hobbyQuery.push(hq);

											var hbp = HobbiesByPeople.find({
												people_id: id
											}, {
												clientId: cid
											});
											hbp.on('added', (id, row)=>{});
											hbp.on('changed', (id, row)=>{});
											hbp.on('removed', (id)=>{
												hobbyByPeopleDeleteCounter++
											});
											hbp.on('state', (currentState)=>{});
											hbp.run();
											hobbiesByPeopleQuery.push(hbp);
										});

										q.on('changed', (id, row)=>{});
										q.on('removed', (id)=>{});
										q.on('state', (currentState)=>{});
										q.run();

										peopleQuery.push(q);
										return callback(null, count);
								    },
								    function (err, n) {
										return callback();
								    }
								);
							});
						});
					}
				], (error) => {
					if (error) return done(error);
					// after init success
					return done();

					expect(peopleQuery.length).to.be.above(90);
					setTimeout(()=>{
						expect(hobbyQuery.length).to.be.above(200);
						expect(hobbiesByPeopleQuery.length).to.be.above(200);
						return done();
					}, 4000)
				});
			});
		});

		/*globalDb.getConnection((error, connection)=>{
			connection.query('SELECT count(*) FROM core_reactive', (error, result)=>{
				connection.release();

				console.log('CORE_REACT:', result);
			});
		})*/
		it('should delete Hobbies in a short time', function(done) {
			this.timeout(5000);

			/*
			console.log('Total registered Queries:', Object.keys(globalDb._queryCache).length);
			console.log('peopleQueries:', peopleQuery.length);
			console.log('hobbyQueries:', hobbyQuery.length);
			console.log('hobbiesByPeopleQueries:', hobbiesByPeopleQuery.length);
			*/

			Hobbies.remove({
				$or:[
					{hobby: 'Performancetracking'},
					{hobby: 'Performacetracking'}
				]

			}, {clientId: 'cid50'}, (error, result)=> {
				return done(error);
			});
		});

		it('should insert Marc Tester M. as new People in a short time', function(done) {
			this.timeout(5000);

			People.insert({
				_id: 'people0001M',
				first_name: 'Marc Tester',
				last_name: 'Performace-Test'
			}, {clientId: 'cid50'}, (error, result) => {
				return done(error);
			});
		});


		it('should delete a People in a short time', function(done) {
			this.timeout(5000);

			People.remove({
				first_name: 'Marc Tester'
			}, {clientId: 'cid50'}, (error, result)=> {
				return done(error);
			});
		});

		it('should insert 2 new Hobbies in a short time', function(done) {
			this.timeout(5000);

			Hobbies.insert([
				{
					_id: 'hobby_id0005',
					people_id: '6b0b5907-b036-11e7-b16a-bc307d530814',
					hobby: 'Performancetracking'
				}, {
					_id: 'hobby_id0006',
					people_id: '6b0b584b-b036-11e7-b16a-bc307d530814',
					hobby: 'Performancetracking'
				}
			], {clientId: 'cid50'}, (error, result) => {
				//console.log(globalDb._queryCount - cnt);
				return done(error);
			});
		});

		it('should insert Marc Tester as new People in a short time', function(done) {
			this.timeout(5000);

			People.insert({
				_id: 'people0001X',
				first_name: 'Marc Tester',
				last_name: 'Performace-Test'
			}, {clientId: 'cid50'}, (error, result) => {
				return done(error);
			});
		});

		it('should work on all other queries and in parallel get some new hobbies to insert', function(done) {
			this.timeout(9000);
			var cnt = globalDb._queryCount;
			//console.log('Start', cnt);
			setTimeout(()=>{
				Hobbies.insert([
					{
						_id: 'hobby_id0007',
						people_id: '6b0bdaa3-b036-11e7-b16a-bc307d530814',
						hobby: 'Performancetracking'
					}, {
						_id: 'hobby_id0008',
						people_id: '6b0bf2e3-b036-11e7-b16a-bc307d530814',
						hobby: 'Performancetracking'
					}
				], {clientId: 'cid50'}, (error, result) => {
					//console.log('DONE.', globalDb._queryCount, globalDb._queryCount - cnt);
					return done(error);
				});

				//console.log('After 500ms:', globalDb._queryCount, globalDb._queryCount - cnt);
				//return done();
			}, 500);
		});

		it('should wait till all work is done', function(done) {
			this.timeout(15000);

			var lastOpCnt = globalDb._queryCount;
			var intv = setInterval(()=>{
				if (lastOpCnt != globalDb._queryCount) {
					//console.log('check at:', globalDb._queryCount - lastOpCnt);
					lastOpCnt = globalDb._queryCount;
				} else {
					clearInterval(intv);
					//console.log('Staled queries:', globalDb._staledQueries.length);
					//console.log('Finished with:', globalDb._queryCount);

					done();
				}
			}, 500);
		});

		it('should stop all reactivity and clear all cached queries', function(done) {
			this.timeout(15000);

			//console.log(globalDb._queriesByTable);

			async.eachSeries(hobbiesByPeopleQuery, (q, callback)=>{
				q.destroy(callback);
			}, (error)=>{
				async.eachSeries(hobbyQuery, (q, callback)=>{
					q.destroy(callback);
				}, (error)=>{
					async.eachSeries(peopleQuery, (q, callback)=>{
						q.destroy(callback);
					}, (error)=>{
						if (error) return done(error);

						expect(Object.keys(globalDb._queryCache).length).to.be.equal(0);
						globalDb.end();
						return done();
					});
				});
			});
		});
	});
});
