'use strict';

const expect     		= require('chai').expect;
const { PGDatabase }	= require('../index');
const { PGTable }		= require('../lib/pg-table');
const { PGView }		= require('../lib/pg-view');

const DB_USER = 'postgres';
const DB_HOST = 'localhost';
const DB_DATABASE = 'pureworkx';
const DB_PASSWORD = 'testtest';
const DB_PORT = 5432;
const DB_MAX_CONNECTION = 100;

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
					if (!error){
						var connection = db.getConnectionSync();
						var result = connection.query('SELECT people.email AS email FROM people WHERE _id = \'6b0b584b-b036-11e7-b16a-bc307d530814\'', function(error, result){
							db.releaseConnection(connection);
							db.end();

							expect(result.rows[0].email).to.equal('jrysdaleke@vistaprint.com');
							done();
						});
					} else {
						throw new Error(error);
					}
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

		it('should reactively get records from core_users', function(done) {
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
						done();
					});
				});
			});
		});

		it('should reactively update one record from core_users and get a change event', function(done) {
			// remove the query preparators first
			db._queryPreparators = [];

			var Users = db.Table('core_users');
			// update the emailadresse before running the reactive query
			// to have a change on the data with the next update
			Users.update({_id: '5fde3570-8fb2-11e7-a232-bc307d530814'}, {
				emailaddress: 'XXX0-newmailadr@gmail.com'
			}, function(error, result){
				expect(error).to.equal(null);

				var reactiveQuery = Users.find({
					created_by: 'peter'
				});

				reactiveQuery.on('changed', (id, row) => {
					expect(id).to.equal('5fde3570-8fb2-11e7-a232-bc307d530814');
					expect(row.emailaddress).to.equal('newmailadr0@gmail.com');

					reactiveQuery.destroy( () => {
						done();
					});
				});

				var firstTime = true;
				reactiveQuery.on('state', (currentState) => {
					if (currentState == 'ready'){
						if (!firstTime) return;
						firstTime = false;

						Users.update({_id: '5fde3570-8fb2-11e7-a232-bc307d530814'}, {
							emailaddress: 'newmailadr0@gmail.com'
						}, function(error, results){
							expect(error).to.equal(null);

						});

					}
				});

				reactiveQuery.run();
			});
		});
	});

	describe('new View', function() {
		it('should return a View object', function() {
			var ViewUserroles = db.View('view_userroles');

			expect(ViewUserroles).to.be.instanceOf(PGView);
		});

		it('should reactively update one record from core_users and get a change event on View', function(done) {
			// remove the query preparators first
			db._queryPreparators = [];

			var Users = db.Table('core_users');
			var ViewUserroles = db.View('view_userroles');

			// update the emailadress before running the reactive query
			// to have a change on the data with the next update -- this is important to run the test-script twice
			Users.update({_id: '84990b26-8ede-11e7-a232-bc307d530814'}, {
				emailaddress: 'XXX1-newmailadr@gmail.com'
			}, function(error, result){
				expect(error).to.equal(null);

				var reactiveQuery = ViewUserroles.find({
					user_id: '84990b26-8ede-11e7-a232-bc307d530814'
				});
				var changeCounter = 0;

				reactiveQuery.on('added', (id, row) => {
					//console.log('added:', id, row);
				});

				reactiveQuery.on('changed', (id, row) => {
					//console.log('changed:', id, row);
					changeCounter++;

					//expect(id).to.equal('84990b26-8ede-11e7-a232-bc307d530814');
					//expect(row.emailaddress).to.equal('newmailadr1@gmail.com');
					if (changeCounter == 2) {
						reactiveQuery.destroy( () => {
							done();
						});
					}
				});

				var firstTime = true;
				reactiveQuery.on('state', (currentState) => {
					if (currentState == 'ready'){
						if (!firstTime) return;
						firstTime = false;

						Users.update({_id: '84990b26-8ede-11e7-a232-bc307d530814'}, {
							emailaddress: 'newmailadr1@gmail.com'
						}, function(error, results){
							expect(error).to.equal(null);
							//console.log(error, results);
						});

					}
				});

				reactiveQuery.run();
			});
		});
	});
});
