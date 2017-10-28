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

describe('Connections', function() {
	// ONLY for Tests on creating an new issue for the pg-module
	/*
	describe('native node-pg', function() {
		it('should use all connections and wait if there is no more', function(done) {
			this.timeout(15000);

			const PGPool = require('pg').Pool;
			const PGClient = require('pg').Client;

			var client = new PGClient({
				user: DB_USER,
				host: DB_HOST,
				database: DB_DATABASE,
				password: DB_PASSWORD,
				port: DB_PORT
			});

			var pool = new PGPool({
				user: DB_USER,
				host: DB_HOST,
				database: DB_DATABASE,
				password: DB_PASSWORD,
				port: DB_PORT,
				max: 20
			});
			var connCount = 0;

			// use just a simple client connection outside of the pool
			client.connect((err) => {
				if (err) {
					console.error('connection error', err.stack)
				} else {
					console.log('Single Client connected.');
				}
			});

			// Testing the pool
			async.times(25, function(n, next){
				pool.connect((error, connection, release) => {
					// release the first client to pool after 1 second,
					// the second client after 2 seconds...
					setTimeout(()=>{
						release();
					}, 1000 * connCount);

					console.log('get client from pool:', connCount);
					if (error) return next(error);
					connCount++;
					next();
				});
			}, function(error, result){
				console.log('Finished with:', connCount);
				console.log(error)
				pool.end();
				done();
			});

		});
	});*/

	describe('PGDatabase', function() {
		it('should use all connections and wait if there is no more', function(done) {
			this.timeout(15000);

			let db = new PGDatabase({
				enablePooling: true,
				connect: {
					user: DB_USER,
					host: DB_HOST,
					database: DB_DATABASE,
					password: DB_PASSWORD,
					port: DB_PORT,
					max: 5
				}
			});
			var connCount = 0;

			db.connect( (error) => {
				if (error) return done(error);

				async.times(10, function(n, next){
					db.getConnection((error, connection)=>{
						setTimeout(()=>{
							connection.release();
						}, 100 * connCount);

						//console.log('get client', connCount);

						if (error) return next(error);
						connCount++;
						next();
					});
				}, function(error, result){
					db.end();
					done();
				});
			});
		});
	});
});
