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
var archiver = require('archiver');
var bawlk = require('bawlk');
var _ = require('lodash');
var reduce = require('stream-reduce');
var pg = require('pg');
var pgTransact = require('pg-transact');
var copyFrom = require('pg-copy-streams').from;
var es = require('event-stream');
var Q = require('q');
var mapSeries = require('promise-map-series');
var request = require('request');

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

    self.gawk = options.gawk || 'gawk';

    self.uploadId = self.slugs[4];
    self.action = self.slugs[5];

    self.localPath = '/tmp/' + self.uploadId;

    events.EventEmitter.call(this);

    self.s3.getObject({Key: self.datapackageKey}, function (err, datapackageData) {
        var zip = new AdmZip(datapackageData.Body);
        zip.extractAllTo(self.localPath);
        self.datapackage = fs.readFileSync(self.localPath + '/datapackage.json', {encoding: 'utf8'});
        self.datapackage = JSON.parse(self.datapackage);
        fs.mkdirSync(path.join(self.localPath, 'violations'));
        self.emit('ready');
    });
};

sys.inherits(Upload, events.EventEmitter);

Upload.prototype.checkForeignKeys = function () {
    var self = this;
    var deferred = Q.defer();
    var violationsPath = path.join(self.localPath, 'violations', 'fkeys.csv');

    self.logger.log('info', 'checking foreign keys for upload: ' + self.uploadId);

    var checker = spawn('bash', [path.resolve(__dirname, '../fkeychecks.sh'), self.localPath]);

    checker.stdout.on('end', function () {
        fs.readFile(violationsPath, {encoding: 'utf8'}, function (err, data) {
            var count = data.split('\n').length;
            var violation = {
                valid: count > 2,
                code: 'fkey',
                count: count - 2
            };

            self.logger.log('info', 'checked foreign keys results are: ', violation);
            deferred.resolve(violation);
        });
    });

    checker.stdout.on('error', function (err) {
        self.logger.log('error', 'checking foreign keys', err);
        deferred.reject();
    });

    checker.stdout.pipe(fs.createWriteStream(violationsPath));

    return deferred.promise;
};

Upload.prototype.validateResource = function (resource) {
    var self = this;
    var deferred = Q.defer();
    var resourceStream = fs.createReadStream(path.join(self.localPath, resource.path), {encoding: 'utf8'});
    var violations = fs.createWriteStream(path.join(self.localPath, 'violations', resource.path));

    self.logger.log('info', 'validating ' + resource.path);

    request(resource.validator, function (err, res, body) {
        var args = [
            '-F,',
            '-v','action=validate',
            '-v', 'CSVFILENAME=' + resource.path,
            body.toString()
        ];
        var awk = spawn(self.gawk, args);

        awk.stdin
            .on('error', function (err) {
                self.logger.log('error', 'validating ' + resource.path, err);
                deferred.reject();
            });

        awk.stdout
            .setEncoding('utf8')
            .on('end', deferred.resolve);

        resourceStream
            .pipe(es.child(awk))
            .pipe(violations);
    });

    return deferred.promise;
};

Upload.prototype.validateResources = function () {
    var self = this;
    self.logger.log('info', 'validating upload: ' + self.uploadId);

    var validates = self.datapackage.resources.map(function (r) {
        return self.validateResource(r);
    });

    return Q.allSettled(validates).then(function (results) {
        self.logger.log('info', 'all violations complete');
    });
};

Upload.prototype.importResource = function (client, resource) {
    var self = this;
    var deferred = Q.defer();
    var pgStream;

    self.logger.log('info', 'importing: ' + resource.path);

    var args = [
        '-v', 'action=insert',
        '-v', 'addfields=upload_id',
        '-v', 'addvals=' + self.uploadId,
        '-v', 'CSVFILENAME='+resource.path, resource.bawlk.script
    ];
    var awk = spawn('./gawk', args);
    awk.stdout.setEncoding('utf8');

    var csvStream = new stream.Readable();
    csvStream.push(resource.raw);
    csvStream.push(null);

    self.logger.log('info', 'pg importing ' + resource.path);

    awk.stdout
        .pipe(es.split())
        .pipe(es.through(function (line) {
            // first line will be the COPY command
            if (!pgStream) {

                pgStream = client.query(copyFrom(line));

                pgStream.on('error', function (err) {
                    console.error('error importing ' + resource.path);
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

    csvStream.pipe(awk.stdin);

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
                self.logger.log('info', 'import successful; transaction committed');
                self.save();
                deferred.resolve();
            })
            .catch(function (err) {
                self.meta.errors = true;
                console.error('error while staging upload: ' + err);
                console.error('import failed; transaction rolled back');
                self.save();
                deferred.reject(err);
            });
    });

    return deferred.promise;
};

module.exports = Upload;
