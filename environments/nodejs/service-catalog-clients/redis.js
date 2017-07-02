'use strict';

const redis = require('redis');
let client = null;

function buildClient(credentials) {
    const config = credentials;
    return redis.createClient(config);
}

module.exports = {
    build: (instance) => {
        client = buildClient(instance);
    },
    get: (cb) => {
        cb(null, client);
    },
    release: (conn) => {

    },
    destory: () => {
        client.quit();
    },
};
