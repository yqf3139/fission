'use strict';

const mysql = require('mysql');
let pool = null;

function buildPool(credentails) {
    const config = credentails;
    config.user = config.username;
    return mysql.createPool(config);
}

module.exports = {
    build: (credentials) => {
        pool = buildPool(credentials);
    },
    get: (cb) => {
        cb(null, pool);
    },
    release: (client) => {

    },
    destory: () => {
        pool.end();
    },
};
