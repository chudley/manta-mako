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



///--- Globals

var LOG = bunyan.createLogger({
        level: (process.env.LOG_LEVEL || 'info'),
        name: 'shark_assign',
        stream: process.stdout
});
var REBALANCE_CONFIG = (process.env.REBALANCE_CONFIG ||
                        '/opt/smartdc/mako/etc/mako_rebalancer_config.json');
var MANTA = 'manta';
var MANTA_CLIENT = manta.createClientFromFileSync(REBALANCE_CONFIG, LOG);
var MANTA_USER = MANTA_CLIENT.user;
var REBALANCE_PATH_PREFIX = '/' + MANTA_USER +
        '/stor/manta_shark_assign/do';
var OK_ERROR = new Error('Not really an error');
OK_ERROR.ok = true;


///--- Pipeline

function readConfig(_, cb) {
        fs.readFile(REBALANCE_CONFIG, function (err, contents) {
                if (err) {
                        cb(err);
                        return;
                }
                try {
                        var cfg = JSON.parse(contents);
                } catch (e) {
                        cb(e, 'error parsing config');
                        return;
                }

                LOG.info(cfg, 'config');

                assert.object(cfg, 'cfg');
                assert.string(cfg.manta_storage_id, 'cfg.manta_storage_id');
                assert.object(cfg.moray, 'cfg.moray');
                assert.string(cfg.moray.host, 'cfg.moray.host');
                assert.number(cfg.moray.port, 'cfg.moray.port');
                assert.number(cfg.moray.connectTimeout,
                              'cfg.moray.connectTimeout');
                _.cfg = cfg;
                cb();
        });
}


function checkForMantaObjects(_, cb) {
        _.rebalacePath = REBALANCE_PATH_PREFIX + '/' + _.cfg.manta_storage_id;
        /*
         * XXX This orders the output as strings, but we encode a sequence
         * integer into this string.  We need to sort this list.
         */
        MANTA_CLIENT.ls(_.rebalacePath, {}, function (err, res) {
                if (err) {
                        cb(err);
                        return;
                }

                var objs = [];

                res.on('object', function (o) {
                        objs.push(o);
                });

                res.once('error', function (err2) {
                        cb(err2);
                });

                res.once('end', function () {
                        if (objs.length < 1) {
                                LOG.info('No objects in directory, returning');
                                cb(OK_ERROR);
                                return;
                        }

                        _.rebalaceObjects = [];

                        var seqs = [];
                        var objsBySeq = {};
                        objs.forEach(function (o) {
                            var s = o.name.split('-X-');
                            var seq = parseInt(s[0].split('-')[0]);
                            seqs.push(seq);
                            objsBySeq[seq] = o;
                        });

                        seqs.sort(function (a, b) {
                            return (a - b);
                        });

                        seqs.forEach(function (n) {
                            _.rebalaceObjects.push(objsBySeq[n]);
                        });

                        LOG.info({
                                objects: _.rebalaceObjects,
                                path: _.rebalacePath
                        }, 'found objects');

                        cb();
                });

        });
}


function initMorayClient(_, cb) {
        var cfg = {
                log: LOG,
                connectTimeout: _.cfg.moray.connectTimeout,
                host: _.cfg.moray.host,
                port: _.cfg.moray.port
        };

        var client = moray.createClient(cfg);
        client.on('connect', function () {
                _.morayClient = client;
                LOG.info('init');
                cb();
        });
}


function closeMorayClient(_, cb) {
        _.morayClient.close();
        cb();
}


function rebalanceMantaObjects(_, cb) {
        var i = 0;
        function rebalanceNext() {
                var mantaObject = _.rebalaceObjects[i];
                if (!mantaObject) {
                        cb();
                        return;
                }

                LOG.fatal({
                    i: i,
                    objectsToMove: _.objectsToMove
                }, 'sss');
                if (_.objectsToMove && i >= _.objectsToMove) {
                        LOG.fatal({
                            objectsToMove: _.objectsToMove,
                            i: i
                        });
                        cb();
                        return;
                }

                rebalanceMantaObject(_, mantaObject, function (err) {
                        if (err) {
                                LOG.error({
                                        err: err,
                                        mantaObject: mantaObject
                                }, 'error while processing manta object');
                                cb(err);
                                return;
                        }

                        var mop = _.rebalacePath + '/' + mantaObject.name;
                        /*
                         * XXX Perhaps worth keeping a running counter of etag
                         * mismatches, and only unlink if we see none?
                         *
                         * If this isn't done, we'd remove the progress file
                         * even in the face of a mismatch that could have come
                         * from an update to the metadata _other_ than sharks.
                         */
                        MANTA_CLIENT.unlink(mop, {}, function (err2) {
                                if (err2) {
                                        cb(err2);
                                        return;
                                }
                                LOG.info({ obj: mantaObject },
                                         'Done with mantaObject');

                                ++i;
                                rebalanceNext();
                        });
                });
        }
        rebalanceNext();
}


function rebalanceMantaObject(_, mantaObject, cb) {
        LOG.info({
                mantaObject: mantaObject
        }, 'rebalancing mantaObject');

        var toProcess = 0;
        var processed = 0;
        var endCalled = false;
        var queue = vasync.queue(function (objs, subcb) {
                rebalance(_, objs, function (err) {
                        if (err && !err.ok) {
                                LOG.error({
                                        err: err,
                                        stack: err.stack,
                                        object: objs
                                }, 'error with objects');
                        }
                        ++processed;
                        //Don't pass along the error, just keep going...
                        //TODO: Is ^^ the right call?
                        subcb();
                });
        }, 1); //Serialize, please, to keep load down.
        /*
         * XXX We serialise ^^ on each "mantaObject", which is a set of
         * instructions on where an object will need to be moved from/to.
         * However, these "mantaObjects" could contain thousands of records, so
         * while we serialise the first part, the actual work against each one
         * of those objects is done in parallel.
         */

        function tryEnd(err) {
                if (queue.npending === 0 && toProcess === processed &&
                    endCalled) {
                        cb();
                }
        }

        var mantaObjectPath = _.rebalacePath + '/' + mantaObject.name;
        MANTA_CLIENT.get(mantaObjectPath, {}, function (err, stream) {
                if (err) {
                        cb(err);
                        return;
                }

                var c = carrier.carry(stream);
                var prevObjectId;
                var objs = [];

                c.on('line', function (line) {
                        if (line === '') {
                                return;
                        }

                        try {
                                var dets = JSON.parse(line);
                        } catch (e) {
                                LOG.error({
                                        line: line,
                                        err: e
                                }, 'not parseable JSON');
                                return;
                        }

                        //Make batches for each object.
                        if (prevObjectId !== dets.objectId) {
                                if (objs.length > 0) {
                                        ++toProcess;
                                        //We have to wrap them, otherwise
                                        // vasync unrolls them.
                                        queue.push({ objects: objs }, tryEnd);
                                }
                                objs = [];
                                prevObjectId = dets.objectId;
                        }
                        objs.push(dets);
                });

                c.on('error', function (err2) {
                        LOG.error(err2, 'during carrier');
                });

                c.on('end', function () {
                        if (objs.length > 0) {
                                ++toProcess;
                                queue.push({ objects: objs }, tryEnd);
                        }
                        LOG.info({
                                mantaObjectPath: mantaObjectPath
                        }, 'Done reading manta object');
                        endCalled = true;
                        tryEnd();
                });

                stream.resume();
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
                        setupObjectData,
                        pullMorayObjects,
                        pushObject,
                        updateMorayObjects,
                        tombstoneOldObject
                ],
                arg: {
                        objects: objects,
                        pc: _
                }
        }, function (err) {
                cb(err);
        });
}


function setupObjectData(_, cb) {
        var o = _.objects[0];
        var today = (new Date()).toISOString().substring(0, 10);
        //For x-account links.
        var user = (o.creator || o.owner);
        _.objectId = o.objectId;
        _.md5 = o.md5;
        _.localDirectory = '/manta/' + user;
        _.localFilename = _.localDirectory + '/' + o.objectId;
        _.remotePath = '/' + user + '/' + o.objectId;
        _.remoteHost = o.newShark.manta_storage_id;
        _.remoteLocation = 'http://' + _.remoteHost + _.remotePath;
        _.remoteTomb = '/tombstone/' + today + '/' + o.objectId;
        cb();
}


function pullMorayObjects(_, cb) {
        var keys = _.objects.map(function (o) {
                return (o.key);
        });

        function getMorayObject(key, subcb) {
                _.pc.morayClient.getObject(MANTA, key, {}, function (err, obj) {
                        subcb(err, obj);
                });
        }

        vasync.forEachParallel({
                'func': getMorayObject,
                'inputs': keys
        }, function (err, results) {
                var morayObjects = [];
                //Pull out all the results into morayObjects
                var i;
                for (i = 0; i < results.operations.length; ++i) {
                        var obj = _.objects[i];
                        var key = keys[i];
                        var oper = results.operations[i];
                        morayObjects[i] = null;
                        if (oper.status === 'ok') {
                                var mobj = oper.result;
                                //If the etag is off, just ignore.  We don't
                                // want to accidentally overwrite data...

                                //TODO: Checking this here risks creating cruft
                                // on the remote node of the MOVE fails.
                                if (mobj._etag !== obj.morayEtag) {
                                        LOG.info({
                                                objectId: _.objectId,
                                                key: key,
                                                objEtag: mobj._etag,
                                                morayObjEtag: obj.morayEtag
                                        }, 'Moray etag mismatch.  Ignoring.');
                                } else {
                                        LOG.info({
                                                objectId: _.objectId,
                                                key: key
                                        }, 'got moray object for key');
                                        morayObjects[i] = oper.result;
                                }
                        } else {
                                err = oper.err;
                                if (err.name === 'ObjectNotFoundError') {
                                        LOG.info({
                                                objectId: _.objectId,
                                                key: key
                                        }, 'ObjectNotFoundError, ignoring');
                                } else {
                                        //Just break out of the whole thing.
                                        return (cb(err));
                                }
                        }
                }

                //Check that we actually have work to do.
                for (i = 0; i < morayObjects.length; ++i) {
                        if (morayObjects[i] !== null) {
                                _.morayObjects = morayObjects;
                                return (cb());
                        }
                }
                return (cb(OK_ERROR));
        });
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



function updateMorayObjects(_, cb) {

        function updateMorayObject(index, subcb) {
                var obj = _.objects[index];
                var mobj = _.morayObjects[index];

                //If there's no corresponding moray object, we don't need
                // to update anything...
                if (!mobj) {
                        subcb();
                        return;
                }

                var oldShark = obj.oldShark;
                var newShark = obj.newShark;

                var b = MANTA;
                var k = mobj.key;
                var v = mobj.value;
                var etag = mobj._etag;
                var op = { etag: etag };

                for (var i = 0; i < v.sharks.length; ++i) {
                        var s = v.sharks[i];
                        if (s.manta_storage_id === oldShark.manta_storage_id &&
                            s.datacenter === oldShark.datacenter) {
                                v.sharks[i] = newShark;
                        }
                }

                LOG.info({
                        objectId: _.objectId,
                        key: k,
                        sharks: v.sharks
                }, 'updating moray object');

                //TODO: Will the etag mismatch also catch deleted objects?  How
                // can we detect that and get rid of the object (since it would
                // be cruft at that point?)
                _.pc.morayClient.putObject(b, k, v, op, function (e) {
                        var ece = 'EtagConflictError';
                        if (e && e.name !== ece) {
                                subcb(e);
                                return;
                        }
                        if (e && e.name === ece) {
                                LOG.info({
                                        objectId: _.objectId,
                                        key: k
                                }, 'Etag conflict');
                        }
                        subcb();
                });
        }

        //Kinda lame...
        var seq = [];
        for (var n = 0; n < _.objects.length; ++n) {
                seq.push(n);
        }
        vasync.forEachParallel({
                'func': updateMorayObject,
                'inputs': seq
        }, function (err, results) {
                //And error passed through should be something bad...
                return (cb(err));
        });
}

function tombstoneOldObject(_, cb) {
        var opts = {
                'method': 'MOVE',
                'hostname': 'localhost',
                'path': _.remotePath,
                'headers': {
                        'Destination': _.remoteTomb
                }
        };

        LOG.info({
                opts: opts,
                objectId: _.objectId
        }, 'moving object');

        var req = http.request(opts, function (res) {
                if (res.statusCode !== 204 &&
                    res.statusCode !== 404) {
                        LOG.error({
                                res: res,
                                opts: opts
                        }, 'unexpected response while moving object');
                        cb(err);
                        return;
                }

                res.on('end', function () {
                        cb();
                });
        });

        req.on('error', function (err) {
                cb(err);
                return;
        });

        req.end();
}


///--- Main

var objectsToMove = process.argv[2];

console.log(objectsToMove);

var opts = {
    'objectsToMove': objectsToMove
};

console.log(opts);

vasync.pipeline({
        funcs: [
                readConfig,
                checkForMantaObjects,
                initMorayClient,
                rebalanceMantaObjects,
                closeMorayClient
        ],
        arg: opts
}, function (err) {
        if (err && !err.ok) {
                LOG.fatal(err);
                process.exit(1);
        }
        MANTA_CLIENT.close();
        LOG.debug('Done.');
});
