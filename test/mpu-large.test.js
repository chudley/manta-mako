/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

/*
 * Test MPU work that requires data that is larger than 4 GiB in size. To that
 * end what we do is create three streams of random data, one that's over 4 GiB,
 * and two that are some number of bytes. The reason for large file sizes is to
 * make sure that we can properly handle the 64-bit offsets in nginx (which it
 * should if built correctly).
 */

var mod_test = require('tape');
var test = mod_test.test;
var mod_extsprintf = require('extsprintf');
var sprintf = mod_extsprintf.sprintf;
var mod_http = require('http');
var mod_crypto = require('crypto');
var mod_vasync = require('vasync');
var mod_uuid = require('node-uuid');
var mod_jsprim = require('jsprim');

var mod_common = require('./common.js');

var LMPU_LARGE_SIZE = 4294967808;        /* 4 GiB + 512 bytes */
var LMPU_SMALL_SIZE = 42;
var LMPU_COMMIT = {};
var LMPU_MD5_OBJ;

function lmpu_generate_file(t, size)
{
        var req, opts, mpustream, name;

        name = mod_uuid.v4();
        LMPU_COMMIT.parts.push(name);
        opts = mod_common.mako_default_opts();
        opts.method = 'PUT';
        opts.path = sprintf('/%s/%s', LMPU_COMMIT['account'], name);
        console.log(sprintf('# putting file at %s', opts.path));
        mpustream = new mod_common.MPUSource({ length: size,
            md5: LMPU_MD5_OBJ });

        req = mod_http.request(opts, function (res) {
                t.ok(res.statusCode, 201);
                res.resume();
                t.end();
        });

        req.on('error', function (err) {
                t.fail(sprintf('received error: %r', err));
                t.end();
        });

        mpustream.pipe(req);
}

test('setup', function (t) {
        t.plan(0);
        mod_common.mpu_setup();

        LMPU_COMMIT['version'] = 1;
        LMPU_COMMIT['nbytes'] = LMPU_LARGE_SIZE + 2 * LMPU_SMALL_SIZE;
        LMPU_COMMIT['account'] = mod_uuid.v4();
        LMPU_COMMIT['objectId'] = mod_uuid.v4();
        LMPU_COMMIT['parts'] = [];
        console.log(sprintf('# account: %s, object: %s', LMPU_COMMIT['account'],
            LMPU_COMMIT['objectId']));
        LMPU_MD5_OBJ = mod_crypto.createHash('md5');
        t.end();
});

test('generating large file', function (t) {
        lmpu_generate_file(t, LMPU_LARGE_SIZE);
});

test('generating small file 1', function (t) {
        lmpu_generate_file(t, LMPU_SMALL_SIZE);
});

test('generating small file 2', function (t) {
        lmpu_generate_file(t, LMPU_SMALL_SIZE);
});

test('calculate md5', function (t) {
        LMPU_COMMIT.md5 = LMPU_MD5_OBJ.digest('base64');
        t.ok(LMPU_COMMIT.md5);
        t.end();
});

test('commit', function (t) {
        var req, opts;

        opts = mod_common.mpu_default_opts();
        req = mod_http.request(opts, function (res) {
                t.equal(res.statusCode, 204);
                t.equal(res.headers['x-joyent-computed-content-md5'],
                    LMPU_COMMIT.md5);
                res.resume();
                t.end();
        });

        req.on('error', function (err) {
                t.fail(sprintf('received error: %r', err));
                t.end();
        });

        req.write(JSON.stringify(LMPU_COMMIT));
        req.end();
});
