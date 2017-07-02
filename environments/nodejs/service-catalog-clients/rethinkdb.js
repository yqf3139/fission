'use strict';

const r = require('rethinkdb');
let client = null;
let config = null;

function buildConfig(credentials) {
    return {
        host: credentials['host'],
        port: credentials['ports.driver'],
        user: 'admin',
        password: credentials['password.admin'],
        db: 'test'
    };
}

module.exports = {
    build: (credentials) => {
        config = buildConfig(credentials);
    },
    get: (cb) => {
        if(!client) {
            r.connect(config).then(function(conn) {
                client = conn;
                cb(null, conn);
            }).error(function(err) {
                console.error(err.message);
                cb(new Error("Could not open a connection"), null);
            });
        } else {
            cb(null, client);
        }
    },
    release: (client) => {

    },
    destory: () => {
        client.close();
    },
};
