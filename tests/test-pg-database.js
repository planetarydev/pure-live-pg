'use strict';

const expect     		= require('chai').expect;
const { PGDatabase }	= require('../index');
const { PGTable }		= require('../lib/pg-table');
const { PGView }		= require('../lib/pg-view');

let db = new PGDatabase({
	enableConnectionPooling: true,
	connect: {
		user: 'postgres',
		host: 'localhost',
		database: 'pureworkx',
		password: 'jahim2001!2',
		port: 5432,
		connectionLimit: 100
	}
});

describe('PgDatabase', function() {
	describe('connect', function() {
		describe('with Pooling', function() {
			it('should async connect and release a connection', function(done) {
				expect(db).to.be.instanceOf(PGDatabase);

				db.connect( (error, client) => {
					if (!error){
						db.releaseConnection(client);
						return done();
					} else {
						throw new Error(error);
					}
				});

				/*expect(query).to.be.instanceOf(SQLQuery);
				expect(query.sql).to.equal('(SELECT `first_name`, `last_name` FROM `people` WHERE `id` = ?) UNION (SELECT `first_name`, `last_name` FROM `more_people` WHERE `id` = ?)');
				expect(query.values.length).to.equal(2);
				expect(query.values[0]).to.equal(1);
				expect(query.values[1]).to.equal(1);*/
			});

			it('should get a new client connection from the pool', function(done) {
				var connection = db.getConnectionSync();
				var result = connection.query('SELECT core_users.emailaddress AS email FROM core_users WHERE _id = \'1761a1be-8fb3-11e7-a232-bc307d530814\'', function(error, result){
					db.releaseConnection(connection);

					expect(result.rows[0].email).to.equal('ramon@gmail.com');
					done();
				});
			});

			it('should query with a new connection in a sync way', function() {
				var connection = db.getConnectionSync();
				var results = db.querySync(connection, 'SELECT 1 AS test');

				expect(results.rows[0].test).to.equal(1);
				db.releaseConnection(connection);
			});
		});
	});
	describe('query-Preperators', function() {
		it('should define a new query preparator', function() {
			db.queryPreparations(function(connection, query){
				// change the query to SELECT 1 as test
				query.sql = 'SELECT 1 as test';
			});

			expect(db._queryPreparators.length).to.equal(1);
		});

		it('should run the query preparator first by calling wpQuerySync()', function() {
			var connection = db.getConnectionSync();
			var result = db.wpQuerySync(connection, 'SELECT NOW() as test');
			// because the query-preparator changed the sql-command
			expect(result.rows[0].test).to.equal(1);
		});
	});

	describe('new Table', function() {
		it('should return a table object', function() {
			var Users = db.Table('core_users');

			expect(Users).to.be.instanceOf(PGTable);
		});

		it('should select one record from core_users', function() {
			// remove the query preparators first
			db._queryPreparators = [];

			var Users = db.Table('core_users');
			var results = Users.select({
				emailaddress: 'rene@gmail.com'
			}, {
				$columns:['_id', 'emailaddress']
			});

			expect(results.rowCount).to.equal(1);
			expect(results.rows[0].emailaddress).to.equal('rene@gmail.com');
		});

		it('should reactively get records from core_users', function(done) {
			// remove the query preparators first
			db._queryPreparators = [];

			var Users = db.Table('core_users');
			var reactiveQuery = Users.find({
				created_by: 'peter'
			});

			var addCounter = 0;
			reactiveQuery.on('added', (id, row) => {
				addCounter++;
			});

			reactiveQuery.on('state', (currentState) => {
				if (currentState == 'ready' && addCounter > 5){
					reactiveQuery.destroy(function(error){
						done();
					});
				}
			});

			reactiveQuery.run();
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
