#!/usr/bin/env node
// -*- mode: js -*-
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

var assert = require('assert-plus');
var bunyan = require('bunyan');
var carrier = require('carrier');
var crypto = require('crypto');
var exec = require('child_process').exec;
var fs = require('fs');
var getopt = require('posix-getopt');
var http = require('http');
var manta = require('manta');
var moray = require('moray');
var path = require('path');
var vasync = require('vasync');

var lib_rebalance = require('../bin/mako_rebalance');


///--- Globals

var LOG = bunyan.createLogger({
        level: (process.env.LOG_LEVEL || 'info'),
        name: 'shark_assign',
        stream: process.stdout
});

function checkForMantaObjects(_, cb) {
        lib_rebalance.mantaClient.ftw(_.rebalacePath, { type: 'o' },
            function (err, res) {
                if (err) {
                        cb(err);
                        return;
                }

                var objs = [];

                res.on('entry', function (o) {
                        objs.push(o);
                });

                res.once('end', function () {
                        if (objs.length < 1) {
                                cb(new Error('no manta objects'));
                                return;
                        }

                        LOG.info({ n: objs.length }, 'found manta objects');

                        if (_.mantaObjectsLimit < 0) {
                            _.rebalaceObjects = objs;
                        } else {
                            _.rebalaceObjects = objs.slice(0,
                                _.mantaObjectsLimit);
                        }

                        LOG.info({
                            limit: _.mantaObjectsLimit,
                            n: _.rebalaceObjects.length
                        }, 'working on manta objects');
                        cb();
                });
        });
}

function rebalance(_, objects, cb) {

        //Unwrap.  See above...
        objects = objects.objects;
        if (objects.length === 0) {
                cb();
                return;
        }

        LOG.info({
                nobjects: objects.length
        }, 'starting pipeline for objects');

        vasync.pipeline({
                funcs: [
                        lib_rebalance.setupObjectData,
                        function overrideRemoteHost(_, _cb) {
                                /*
                                 * The remoteHost from the existing tooling is
                                 * the oldShark (at least in the context of how
                                 * this tool runs), so we override it here.
                                 */
                                _.remoteHost =
                                    _.objects[0].newShark.manta_storage_id;
                                _cb();
                        },
                        lib_rebalance.pullMorayObjects,
                        pushObject,
                        lib_rebalance.updateMorayObjects,
                        function overrideRemoteHostAgain(_, _cb) {
                                /*
                                 * This is awful, but to make use of the
                                 * existing tombstone method we override the
                                 * remote host to be localhost so that we
                                 * tombstone the _local_ object.
                                 */
                                _.remoteHost = 'localhost';
                                _cb();
                        },
                        lib_rebalance.tombstoneOldObject
                ],
                arg: {
                        objects: objects,
                        pc: _
                }
        }, cb);
}

function pushObject(_, cb) {
        LOG.info({
            objects: _.objects
        }, 'moving objects');

        var error = null;
        var ox = {
            hostname: _.remoteHost,
            port: 80,
            path: _.remoteLocation,
            method: 'HEAD'
        };
        var req1 = http.request(ox, function (res) {
                if (res.statusCode === 200) {
                        /*
                         * Already done this one.
                         */
                        LOG.info('already exists');
                        res.removeAllListeners();
                        cb();
                        return;
                }
                res.once('error', function (headErr) {
                        res.removeAllListeners();
                        LOG.fatal({
                            err: err
                        }, 'head err');
                        cb(headErr)
                        return;
                });
                res.once('end', function () {
                        LOG.info({
                            h: res.statusCode
                        }, 'starting put');
                        var options = {
                            hostname: _.remoteHost,
                            port: 80,
                            path: _.remoteLocation,
                            method: 'PUT'
                        };

                        var req = http.request(options, function (res2) {
                                res2.once('error', function (putError) {
                                        LOG.fatal({
                                            err: putError
                                        }, 'put res err');
                                        cb(putError);
                                        return;
                                });
                                res2.once('end', function () {
                                        LOG.info('finished put');
                                        cb();
                                        return;
                                });
                        });
                        req.on('error', function (err) {
                                LOG.fatal({
                                    err: err
                                }, 'put req err');
                                cb(err);
                                return;
                        });

                        var readStream = fs.createReadStream(_.localFilename);

                        readStream.pipe(req);
                });
        });
        req1.on('error', function (err) {
                cb(err);
                return;
        });
        req1.end();
}


/*
 * Any positive integer supplied on the CLI is the limit of manta objects we
 * want to work with, but we take any negative integer as meaning "all".
 */
//var mantaObjectsLimit = process.argv[2];
var mantaObjectsLimit;
if (!mantaObjectsLimit || mantaObjectsLimit < 0) {
        mantaObjectsLimit = -1;
} else {
        mantaObjectsLimit = parseInt(mantaObjectsLimit);
}
assert.number(mantaObjectsLimit);

var opts = {
    'rebalanceFunc': rebalance,
    'rebalacePath': null,
    'mantaObjectsLimit': mantaObjectsLimit
};

vasync.pipeline({
        funcs: [
                lib_rebalance.readConfig,
                function setRebalaNcePath(_, cb) {
                        _.rebalacePath =
                            '/poseidon/stor/manta_shark_assign/do/' +
                            _.cfg.manta_storage_id;
                        cb();
                },
                checkForMantaObjects,
                lib_rebalance.initMorayClient,
                lib_rebalance.rebalanceMantaObjects,
                lib_rebalance.closeMorayClient
        ],
        arg: opts
}, function (err) {
        if (err && !err.ok) {
                LOG.fatal(err);
                process.exit(1);
        }
        lib_rebalance.mantaClient.close();
        LOG.debug('Done.');
});
