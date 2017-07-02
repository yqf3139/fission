'use strict';

const minio = require('minio');
let client = null;

function buildClient(credentials) {
    const config = credentials;
    return new minio.Client({
        endPoint: config['host'],
        port: parseInt(config['port']),
        secure: false,
        accessKey: config['keys.access'],
        secretKey: config['keys.secret']
    });
}

module.exports = {
    build: (credentials) => {
        client = buildClient(credentials);
    },
    get: (cb) => {
        cb(null, client);
    },
    release: (client) => {

    },
    destory: () => {},
};
