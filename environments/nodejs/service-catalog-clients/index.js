'use strict';

const mysql = require('./mysql');
const redis = require('./redis');
const minio = require('./minio');
const rethinkdb = require('./rethinkdb');

const builderMap = {
    'mariadb': mysql,
    'redis': redis,
    'minio': minio,
    'rethinkdb': rethinkdb,
};

function toClientPool(instance) {
    const { name, serviceclass, credentials } = instance;
    if(!credentials) {
        return {
            name: name,
            get: (cb) => cb(new Error('Credentials are missing'), null),
            release: (conn) => {},
        };
    }
    const builder = builderMap[serviceclass];
    if(!builder) {
        return {
            name: name,
            get: (cb) => cb(new Error(`No builder for ${serviceclass}`), null),
            release: (conn) => {},
        };
    }
    builder.build(credentials);
    return { name: name, get: builder.get, release: builder.release };
}

module.exports = toClientPool;
