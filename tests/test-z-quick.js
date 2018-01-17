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

describe('Quicktest', function() {
	describe('zzzz', function() {
		it('should do this quick test', function(done) {
			/*let db = new PGDatabase({
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

			var query = db.sqlBuilder.build({

			});

			expect(query.sql).to.equal('.');*/
			return done();
		});
	});
});
