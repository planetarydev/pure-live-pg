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

			var query = db.sqlBuilder.build({
				$create: {
					$view: { $cor: 'test' },
					$select: {
						$from: 'meteor.users', $joins: {
							'meteor.users_profiles': { $leftJoin: { 'meteor.users._id': { $eq: '~~meteor.users_profiles._id' } } }
						},
						$columns: {
							_id: { $column: 'meteor.users._id' },
							username: { $column: 'meteor.users.username' },
							createdAt: { $column: 'meteor.users.created_at' },

							profile: {
								$rowToJson: 'users_profiles'
							},

							emails: {
								$select: {
									$columns: {
										emails: {
											$jsonAgg: {
												$jsonBuildObject: {
													address: { $column: 'meteor.users_emails.address' },
													verified: { $column: 'meteor.users_emails.verified' }
												}
											}
										}
									},
									$from: 'meteor.users_emails',
									$where: {
										'meteor.users_emails.user_id': { $eq: '~~meteor.users._id' }
									}
								}
							},

							services: {
								$select: {
									services: { $jsonbObjectAgg: { '~~servicedata.key': '~~servicedata.value' } },
									$from: {
										data: {
											$select: {
												services: { $jsonbBuildObject: { '~~users_loginservices.service_id': '~~users_loginservices.data' } },
												$from: 'meteor.users_loginservices'
											}
										},
										servicedata: {
											$jsonbEach: '~~data.services'
										}
									}
								}
							}
						}
					}
				}
			});

			expect(query.sql).to.equal('.');
		});
	});
});
