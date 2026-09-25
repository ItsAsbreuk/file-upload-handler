/*global describe, it, before, after */

'use strict';

// These tests run the handler inside a real hapi 21 server and send real http-requests: the
// payload-handling of hapi and node (streams, the `close` event of a finished request) is exactly
// what changed between hapi 16 and hapi 21, so a mocked `h` would prove nothing.

var expect = require('chai').expect,
    fs = require('fs'),
    os = require('os'),
    path = require('path'),
    http = require('http'),
    Hapi = require('@hapi/hapi'),
    getHapiFns = require('../lib/hapi-handler');

var MAX_FILE_SIZE = 1024,
    TRANSFER_HEADERS = 'x-filename, x-transid, x-clientid, x-partial, x-total-size, x-data';

/**
 * Sends one http-request to the test-server.
 *
 * @method send
 * @param port {Number}
 * @param options {Object} method, path, headers and body
 * @return {Promise} resolves with {status, headers, body}
 */
var send = function(port, options) {
    return new Promise(function(resolve, reject) {
        var req = http.request({
            port: port,
            method: options.method,
            path: options.path,
            headers: options.headers || {}
        }, function(res) {
            var chunks = [];
            res.on('data', function(chunk) {
                chunks.push(chunk);
            });
            res.on('end', function() {
                var body = Buffer.concat(chunks).toString('utf8'),
                    json;
                try {
                    json = JSON.parse(body);
                }
                catch (err) {
                    json = undefined;
                }
                resolve({status: res.statusCode, headers: res.headers, body: body, json: json});
            });
        });
        req.on('error', reject);
        if (options.body) {
            req.write(options.body);
        }
        req.end();
    });
};

/**
 * Builds a multipart/form-data body with fields and files.
 *
 * @method multipart
 * @param fields {Object} name -> value
 * @param files {Array} [{name, filename, content}]
 * @return {Object} {body, contentType}
 */
var multipart = function(fields, files) {
    var boundary = '----itsa-test-boundary',
        parts = [];
    Object.keys(fields).forEach(function(name) {
        parts.push('--' + boundary + '\r\nContent-Disposition: form-data; name="' + name + '"\r\n\r\n' + fields[name] + '\r\n');
    });
    files.forEach(function(file) {
        parts.push('--' + boundary + '\r\nContent-Disposition: form-data; name="' + file.name + '"; filename="' +
            file.filename + '"\r\nContent-Type: text/plain\r\n\r\n' + file.content + '\r\n');
    });
    parts.push('--' + boundary + '--\r\n');
    return {
        body: parts.join(''),
        contentType: 'multipart/form-data; boundary=' + boundary
    };
};

/**
 * Starts a hapi 21 server whose routes use the handler, and records what the callbacks receive.
 *
 * @method startServer
 * @param accessControlAllowOrigin {Boolean|String}
 * @return {Promise} resolves with {server, port, received, tmpDir}
 */
var startServer = function(accessControlAllowOrigin) {
    var tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'itsa-fileuploadhandler-')),
        fns = getHapiFns(tmpDir, MAX_FILE_SIZE, accessControlAllowOrigin),
        server = Hapi.server({port: 0, host: 'localhost'}),
        received = [],
        streamPayload = {output: 'stream', parse: false, maxBytes: 10 * MAX_FILE_SIZE},
        waitForCb = function(request) {
            return (request.query.wait === 'false') ? false : undefined;
        };

    server.route([
        {
            method: 'GET',
            path: '/clientid',
            handler: function(request, h) {
                return fns.generateClientId(request, h);
            }
        },
        {
            method: 'OPTIONS',
            path: '/upload',
            handler: function(request, h) {
                return fns.responseOptions(request, h);
            }
        },
        {
            method: 'PUT',
            path: '/upload',
            options: {payload: streamPayload},
            handler: function(request, h) {
                return fns.recieveFile(request, h, null, function(fullFilename, originalFilename) {
                    var entry = {
                        content: fs.readFileSync(fullFilename, 'utf8'),
                        originalFilename: originalFilename,
                        params: Object.assign({}, this.params)
                    };
                    received.push(entry);
                    return {processed: originalFilename};
                }, waitForCb(request));
            }
        },
        {
            method: 'POST',
            path: '/form',
            options: {payload: streamPayload},
            handler: function(request, h) {
                return fns.recieveFormFiles(request, h, null, function(files) {
                    var entry = {
                        files: files.map(function(file) {
                            return {
                                content: fs.readFileSync(file.fullFilename, 'utf8'),
                                originalFilename: file.originalFilename
                            };
                        }),
                        params: Object.assign({}, this.params)
                    };
                    received.push(entry);
                    return {count: files.length};
                }, waitForCb(request));
            }
        }
    ]);

    return server.start().then(function() {
        return {server: server, port: server.info.port, received: received, tmpDir: tmpDir};
    });
};

/**
 * Waits until `received` has `count` entries: the background-processing when `waitForCb` is false.
 *
 * @method waitForReceived
 * @param received {Array}
 * @param count {Number}
 * @return {Promise}
 */
var waitForReceived = function(received, count) {
    var started = Date.now();
    return new Promise(function(resolve, reject) {
        var check = function() {
            if (received.length >= count) {
                resolve();
            }
            else if (Date.now() - started > 2000) {
                reject(new Error('callback was not invoked'));
            }
            else {
                setTimeout(check, 10);
            }
        };
        check();
    });
};

/**
 * Sends a file in two chunks, the way the itsa-client transfers it.
 *
 * @method sendInTwoChunks
 * @param port {Number}
 * @param transId {String}
 * @param query {String} querystring for the upload-path
 * @return {Promise} resolves with [response chunk 1, response chunk 2]
 */
var sendInTwoChunks = function(port, transId, query) {
    var baseHeaders = {
            'content-type': 'application/octet-stream',
            'x-transid': transId,
            'x-clientid': 'client-1',
            'x-total-size': '11'
        },
        uploadPath = '/upload' + (query || '');
    return send(port, {
        method: 'PUT',
        path: uploadPath,
        headers: Object.assign({'x-partial': '1'}, baseHeaders),
        body: 'hello '
    }).then(function(first) {
        return send(port, {
            method: 'PUT',
            path: uploadPath,
            headers: Object.assign({
                'x-partial': '2',
                'x-filename': 'hello.txt',
                'x-data': JSON.stringify({projectid: '42'})
            }, baseHeaders),
            body: 'world'
        }).then(function(second) {
            return [first, second];
        });
    });
};

describe('hapi-handler on hapi 21', function() {
    var context;

    before(function() {
        return startServer(false).then(function(started) {
            context = started;
        });
    });

    after(function() {
        return context.server.stop();
    });

    describe('generateClientId', function() {
        it('answers a client id as text/plain', function() {
            return send(context.port, {method: 'GET', path: '/clientid'}).then(function(response) {
                expect(response.status).to.equal(200);
                expect(response.headers['content-type']).to.match(/^text\/plain/);
                expect(response.body).to.match(/^ITSA_CL_ID_/);
            });
        });
    });

    describe('responseOptions', function() {
        it('answers the preflight with 200, not the 204 hapi 21 gives an empty response', function() {
            return send(context.port, {
                method: 'OPTIONS',
                path: '/upload',
                headers: {'access-control-request-headers': TRANSFER_HEADERS}
            }).then(function(response) {
                expect(response.status).to.equal(200);
                expect(response.headers['access-control-allow-methods']).to.equal('PUT,GET,POST');
                expect(response.headers['access-control-allow-headers']).to.equal(TRANSFER_HEADERS);
                expect(response.headers['access-control-max-age']).to.equal('1728000');
            });
        });
    });

    describe('recieveFile', function() {
        it('answers BUSY for a chunk and OK with the callback its data for the last one', function() {
            context.received.length = 0;
            return sendInTwoChunks(context.port, 'trans-wait').then(function(responses) {
                expect(responses[0].status).to.equal(200);
                expect(responses[0].json).to.deep.equal({status: 'BUSY'});
                expect(responses[1].status).to.equal(200);
                expect(responses[1].json).to.deep.equal({status: 'OK', data: {processed: 'hello.txt'}});
            });
        });

        it('hands the rebuilt file and the x-data params to the callback', function() {
            context.received.length = 0;
            return sendInTwoChunks(context.port, 'trans-content').then(function() {
                expect(context.received).to.have.length(1);
                expect(context.received[0].content).to.equal('hello world');
                expect(context.received[0].originalFilename).to.equal('hello.txt');
                expect(context.received[0].params.projectid).to.equal('42');
            });
        });

        it('answers OK right away when waitForCb is false, and still runs the callback', function() {
            context.received.length = 0;
            return sendInTwoChunks(context.port, 'trans-nowait', '?wait=false').then(function(responses) {
                expect(responses[1].status).to.equal(200);
                expect(responses[1].json).to.deep.equal({status: 'OK'});
                return waitForReceived(context.received, 1);
            }).then(function() {
                expect(context.received[0].content).to.equal('hello world');
            });
        });

        it('answers 403 when the declared size exceeds the maximum', function() {
            return send(context.port, {
                method: 'PUT',
                path: '/upload',
                headers: {
                    'content-type': 'application/octet-stream',
                    'x-transid': 'trans-big',
                    'x-clientid': 'client-1',
                    'x-partial': '1',
                    'x-total-size': String(MAX_FILE_SIZE + 1)
                },
                body: 'too big'
            }).then(function(response) {
                expect(response.status).to.equal(403);
                expect(response.json).to.deep.equal({status: 'Error: max filesize exceeded'});
            });
        });
    });

    describe('recieveFormFiles', function() {
        var form = multipart({projectid: '7'}, [
            {name: 'uploadfiles', filename: 'a.txt', content: 'first file'},
            {name: 'uploadfiles', filename: 'b.txt', content: 'second file'}
        ]);

        it('hands the files and fields to the callback and answers OK with its data', function() {
            context.received.length = 0;
            return send(context.port, {
                method: 'POST',
                path: '/form',
                headers: {'content-type': form.contentType},
                body: form.body
            }).then(function(response) {
                expect(response.status).to.equal(200);
                expect(response.json).to.deep.equal({status: 'OK', data: {count: 2}});
                expect(context.received).to.have.length(1);
                expect(context.received[0].params.projectid).to.equal('7');
                expect(context.received[0].files).to.deep.equal([
                    {content: 'first file', originalFilename: 'a.txt'},
                    {content: 'second file', originalFilename: 'b.txt'}
                ]);
            });
        });

        it('answers ERROR without invoking the callback when a file exceeds the maximum', function() {
            var bigForm = multipart({}, [
                {name: 'uploadfiles', filename: 'big.txt', content: new Array(MAX_FILE_SIZE + 2).join('x')}
            ]);
            context.received.length = 0;
            return send(context.port, {
                method: 'POST',
                path: '/form',
                headers: {'content-type': bigForm.contentType},
                body: bigForm.body
            }).then(function(response) {
                expect(response.status).to.equal(200);
                expect(response.json).to.deep.equal({status: 'ERROR', message: 'max filesize exceeded'});
                expect(context.received).to.have.length(0);
            });
        });

        it('answers OK right away when waitForCb is false, and still runs the callback', function() {
            context.received.length = 0;
            return send(context.port, {
                method: 'POST',
                path: '/form?wait=false',
                headers: {'content-type': form.contentType},
                body: form.body
            }).then(function(response) {
                expect(response.status).to.equal(200);
                expect(response.json).to.deep.equal({status: 'OK'});
                return waitForReceived(context.received, 1);
            }).then(function() {
                expect(context.received[0].files).to.have.length(2);
            });
        });
    });
});

describe('hapi-handler with accessControlAllowOrigin', function() {
    var context;

    before(function() {
        return startServer(true).then(function(started) {
            context = started;
        });
    });

    after(function() {
        return context.server.stop();
    });

    it('sends the CORS header with the client id', function() {
        return send(context.port, {method: 'GET', path: '/clientid'}).then(function(response) {
            expect(response.headers['access-control-allow-origin']).to.equal('*');
        });
    });

    it('sends the CORS header with an upload response', function() {
        return sendInTwoChunks(context.port, 'trans-cors').then(function(responses) {
            expect(responses[0].headers['access-control-allow-origin']).to.equal('*');
            expect(responses[1].headers['access-control-allow-origin']).to.equal('*');
        });
    });
});
