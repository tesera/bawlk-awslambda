'use strict';
var sys = require('sys');
var events = require('events');
var stream = require('stream');
var Readable = stream.Readable;
var spawn = require('child_process').spawn;
var fs = require('fs');
var path = require('path');
var aws = require('aws-sdk');
var AdmZip = require('adm-zip');
var bawlk = require('bawlk');
var _ = require('lodash');
var reduce = require('stream-reduce');
var pg = require('pg');
var pgTransact = require('pg-transact');
var copyFrom = require('pg-copy-streams').from;
var es = require('event-stream');
var Q = require('q');
var mapSeries = require('promise-map-series');
var rimraf = require('rimraf');

var Upload = function(options) {
    var self = this;

    self.bucket = options.bucket;
    self.datapackageKey = options.key;
    self.logger = options.logger;
    self.pgUrl = options.pgUrl;

    self.s3 = new aws.S3({ params: { Bucket: self.bucket } });

    self.path = self.datapackageKey.replace('datapackage.zip', '');
    self.slugs = self.path.split('/').filter(function (s) { return s; });
    self.root = self.slugs.slice(0, -1).join('/');

    self.uploadId = self.slugs[4];
    self.action = self.slugs[5];

    events.EventEmitter.call(this);

    self.s3.getObject({Key: self.datapackageKey}, function (err, datapackageData) {

        self.zip = new AdmZip(datapackageData.Body);
        self.datapackage = JSON.parse(self.zip.getEntry('datapackage.json').getData().toString('utf8'));

        self.logger.log('looking if upload.json exists: ' + self.path+'upload.json');

        self.s3.getObject({Key: self.root+'/upload.json'}, function (err, uploadRes) {

            if(err && err.statusCode === 404) {
                self.logger.log('upload.json did not existed.');
                self.meta = {
                    id: self.slugs[4],
                    datetime: datapackageData.LastModified,
                    user: self.slugs[2],
                    company: self.slugs[3],
                    title: self.datapackage.title || 'no title',
                    description: self.datapackage.description || 'no desc',
                    bytes: datapackageData.ContentLength,
                    state: 'uploaded',
                    resources: {},
                    logs: []
                };
                return self.save().then(self.initialize.bind(self));
            } else {
                self.logger.log('upload.json existed.');
                self.meta = JSON.parse(uploadRes.Body);
                return self.initialize();
            }
        });

    });
};

sys.inherits(Upload, events.EventEmitter);

Upload.prototype.loadData = function () {
    var self = this;
    self.logger.log('loading data');

    self.meta.resources = self.meta.resources || {};

    self.datapackage.resources.forEach(function (resource) {
        self.logger.log('loading: ' + resource.path);
        var csvBuffer = self.zip.getEntry(resource.path).getData();
        var csv = csvBuffer.toString('utf8');
        var csvStream = new stream.Readable();
        csvStream.push(csv);
        csvStream.push(null);

        resource.data = csvStream;
        resource.raw = csv;
        self.meta.resources[resource.path] = {
            bytes: csvBuffer.length,
            count: csv.split('\n').length
        };
    });
};

Upload.prototype.save = function () {
    var self = this;
    var putUploadMeta = function () {
        return Q.nbind(self.s3.putObject, self.s3)({
            Key: self.root + '/upload.json',
            Body: JSON.stringify(self.meta),
            ContentType: 'application/json'
        });
    };

    self.meta.logs = self.meta.logs.concat(self.logger.flush());

    if (self.meta.validation && self.meta.validation.summary) {
        var lines = self.meta.validation.summary.split('\n').slice(0, -1);
        self.meta.validation.summary = lines.map(function (line) {
            return line.split(',');
        });
    }

    return putUploadMeta().then(function () {
        return self.logger.log('put upload.json to: ' + self.root + '/upload.json');
    });
};

Upload.prototype.checkForeignKeys = function () {
    var self = this;
    self.logger.log('checking foreign keys for upload: ' + self.uploadId);
    var uploadDataPath = path.join('/tmp', self.uploadId);

    var mkdir = function () {
        return Q.nbind(fs.mkdir, fs)(uploadDataPath);
    };

    var writeCsvs = function () {
        return Q.all(self.datapackage.resources.map(function (resource) {
            var resourcePath = path.join('/tmp', self.uploadId, resource.path);
            self.logger.log('writing csv file to ' + resourcePath);

            return Q.nbind(fs.writeFile, fs)(resourcePath, resource.raw);
        }));
    };

    return mkdir()
        .then(writeCsvs)
        .then(function () {
            var deferred = Q.defer();
            var checker = spawn('bash', [path.resolve(__dirname, '../fkeychecks.sh'), uploadDataPath]);
            var header = 'file_name,field_name,rule,message,violation_severity,violation_count\n';
            var violations = '';

            checker.stdout.on('end', function () {
                violations = violations ? header + violations : violations;
                deferred.resolve(violations);
            });

            checker.stdin.on('error', function (err) {
                self.logger.log('stdin error in fkey checks : ' + err);
                deferred.reject(err);
            });

            checker.stdout.on('error', function (err) {
                self.logger.log('stdout error in fkey checks : ' + err);
                deferred.reject(err);
            });

            checker.stdout.on('data', function (chunk) {
                violations += chunk;
            });

            return deferred.promise;
        })
        .then(function (violations) {
            self.meta.validation = {
                valid: !violations,
                count: violations ? violations.split('\n').length-1 : 0,
                summary: violations
            };
        })
        .catch(function (e) {
            self.meta.errors = true;
            self.logger.log('there was an error while checking fkeys : ' + e);
        })
        .finally(function () {
            rimraf(uploadDataPath, function (err) {
                if (err) self.logger.log('error removing temp upload directory');
                else self.logger.log('removed temp upload directory');
            });
            return self.save();
        });
};

Upload.prototype.initialize = function () {
    var self = this;

    function init (resource) {
        var deferred = Q.defer();
        self.logger.log('initializing ' + resource.path);
        var resourceStream = new Readable({ objectMode: true });
        resourceStream.push(resource);
        resourceStream.push(null);

        resourceStream
            .pipe(bawlk.getRuleset())
            .pipe(bawlk.getScript())
            .pipe(reduce(function (acc, chunk) {
                return acc + chunk;
            }, ''))
            .on('data', function (data) {
                resource.bawlk = {
                    script: data.toString('utf8')
                };
                deferred.resolve();
            })
            .on('error', function (err) {
                deferred.reject('error getting bawlk script for ' + resource.path + ': ' + err);
            });

        return deferred.promise;
    }

    var inits = self.datapackage.resources.map(function(r) { return init(r); });

    return Q.allSettled(inits).then(function() {
        self.logger.log('initialization complete');

        self.loadData();
        self.emit('ready');
    });
};

Upload.prototype.validateResource = function (resource) {
    var self = this;
    var deferred = Q.defer();
    var args = [
        '-F,',
        '-v','action=validate:summary',
        '-v', 'CSVFILENAME=' + resource.path,
        resource.bawlk.script
    ];
    var awk = spawn('awk', args);
    var violations = '';

    self.logger.log('validating: ' + resource.path);

    awk.stdin.on('error', function (err) {
        deferred.reject('error validating ' + resource.path + ': ' + err);
        this.emit('end');
    });

    awk.stdout.on('data', function (chunk) {
        if (chunk) {
            violations += chunk;
        }
    });

    awk.stdout.on('end', function () {
        var mem = process.memoryUsage();
        var used = ' used: ' + Math.floor(mem.heapUsed/1000000) + ' MB';
        var total = ' total: ' + Math.floor(mem.heapTotal/1000000) + ' MB';
        self.logger.log('validate finished for '  + resource.path);
        self.logger.log('-- memory usage ' + used + total);

        deferred.resolve(violations);
    });

    resource.data.pipe(awk.stdin);

    return deferred.promise;
};

Upload.prototype.validateResources = function (uploadId) {
    var self = this;
    self.logger.log('validating upload: ' + uploadId);
    var header = 'file_name,field_name,rule,message,violation_severity,violation_count\n';

    var validates = self.datapackage.resources.map(function (r) {
        return self.validateResource(r);
    });

    return Q.allSettled(validates).then(function (results) {
        self.logger.log('all violations complete');

        var violations = results.reduce(function (memo, result) {
            if (result.state === 'fulfilled' && result.value) {
                memo += result.value;
            }
            return memo;
        }, '');

        var count = _.reduce(violations.split('\n'), function (c, r, i) {
            var fields = r.split(',');
            if (i > 0 && fields.length === 6) {
                c += parseInt(fields[5]);
            }
            return c;
        }, 0);

        self.meta.validation = {
            valid: (count === 0),
            count: count,
            summary: count ? header + violations : ''
        };

        return self.save();
    });
};

Upload.prototype.importResource = function (client, resource) {
    var self = this;
    var deferred = Q.defer();
    var pgStream;

    self.logger.log('importing: ' + resource.path);

    var args = [
        '-v', 'action=insert',
        '-v', 'addfields=upload_id',
        '-v', 'addvals=' + self.uploadId,
        '-v', 'CSVFILENAME='+resource.path, resource.bawlk.script
    ];
    var awk = spawn('awk', args);
    awk.stdout.setEncoding('utf8');

    self.logger.log('pg importing ' + resource.path);

    awk.stdout
        .pipe(es.split())
        .pipe(es.through(function (line) {
            // first line will be the COPY command
            if (!pgStream) {

                pgStream = client.query(copyFrom(line));

                pgStream.on('error', function (err) {
                    return deferred.reject(err);
                });

                pgStream.on('end', function () {
                    return deferred.resolve();
                });

            } else if (line) { // this is actual data
                pgStream.write(new Buffer(line + '\n'));
            } else { // line is null means no more data
                if (pgStream) pgStream.end();
            }

        }));

    resource.data.pipe(awk.stdin);

    return deferred.promise;
};


Upload.prototype.importResources = function () {
    var self = this;
    var deferred = Q.defer();
    var client = new pg.Client(self.pgUrl);

    client.connect(function (err) {
        if(err) deferred.reject('error connecting to the db: ' + err);

        function transaction(client) {
            var query = Q.nbind(client.query, client);

            return query('SET search_path TO psp,staging,public')
                .then(function () {
                    var imports = mapSeries(self.datapackage.resources, function (resource) {
                        return self.importResource(client, resource);
                    }, self);

                    return Q.all(imports);
                });
        }

        var done = client.end.bind(client);

        pgTransact(client, transaction, done)
            .then(function () {
                self.meta.state = 'staged';
                self.logger.log('import successful; transaction committed');
                self.save();
                deferred.resolve();
            })
            .catch(function (err) {
                self.meta.errors = true;
                self.logger.error('error while staging upload: ' + err);
                self.logger.error('import failed; transaction rolled back');
                self.save();
                deferred.reject(err);
            });
    });

    return deferred.promise;
};

module.exports = Upload;
