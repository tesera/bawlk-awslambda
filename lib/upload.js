var sys = require('sys');
var events = require('events');
var stream = require('stream');
var Readable = stream.Readable;
var spawn = require('child_process').spawn;
var util = require("util");
var aws = require('aws-sdk');
var AdmZip = require('adm-zip');
var bawlk = require('bawlk');
var reduce = require("stream-reduce");
var _ = require('lodash');
var pg = require('pg');
var pgTransact = require('pg-transact');
var copyFrom = require('pg-copy-streams').from;
var es = require('event-stream');
var Q = require('q');

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
        // if(err || !res.Body) return context.done(err, 'error fetching: ' + datapackageS3Uri);

        self.zip = new AdmZip(datapackageData.Body);
        self.datapackage = JSON.parse(self.zip.getEntry('datapackage.json').getData().toString('utf8'));

        self.logger.log('looking if upload.json exists: ' + self.path+'upload.json');
        self.s3.getObject({Key: self.root+'/upload.json'}, function (err, uploadRes) {
            // if(err && err.statusCode !== 404) this.emit('error')

            if(err && err.statusCode === 404) {
                self.logger.log('upload.json did not existed.')
                self.meta = {
                    id: self.slugs[4],
                    datetime: datapackageData.LastModified,
                    user: self.slugs[2],
                    company: self.slugs[3],
                    title: self.datapackage.title || 'no title',
                    description: self.datapackage.description || 'no desc',
                    bytes: datapackageData.ContentLength,
                    state: 'valiating',
                    resources: {}
                };

                self.putMeta();
            } else {
                self.logger.log('upload.json existed.')
                self.meta = JSON.parse(uploadRes.Body);
            }

            self.initialize();
        });

    });
};

sys.inherits(Upload, events.EventEmitter);

Upload.prototype.loadData = function () {
    var self = this;
    self.logger.log('loading data')

    self.meta.resources = self.meta.resources || {};

    self.datapackage.resources.forEach(function (resource) {
        self.logger.log('loading: ' + resource.path);
        var csvBuffer = self.zip.getEntry(resource.path).getData();
        var csv = csvBuffer.toString('utf8');
        var csvStream = new stream.Readable();
        csvStream.push(csv);
        csvStream.push(null);

        resource.data = csvStream;
        self.meta.resources[resource.path] = {
            bytes: csvBuffer.length,
            count: csv.split('\n').length
        };
    });
};

Upload.prototype.putMeta = function () {
    var self = this;
    var params = {
        Key: self.root + '/upload.json',
        Body: JSON.stringify(self.meta),
        ContentType: 'application/json'
    };
    var putUploadMeta = function () { return Q.nbind(self.s3.putObject, self.s3)(params) };

    return putUploadMeta()
        .then(function (err, data) {
            self.logger.log('put upload.json to: ' + params.Key);
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

                self.loadData();
                deferred.resolve();
            })
            .on('error', function (err) {
                deferred.reject('error getting bawlk script for ' + resource.path + ': ' + err);
            });

        return deferred.promise;
    }

    var inits = self.datapackage.resources.map(function(r) { return init(r) });

    return Q.allSettled(inits).then(function() {
        self.logger.log('initialization complete')
        self.emit('ready');
    });
}

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
        if (chunk) violations += chunk;
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
}

Upload.prototype.validateResources = function (uploadId, resource) {
    var self = this;
    self.logger.log('validating upload: ' + uploadId);

    var validates = self.datapackage.resources.map(function (r) { 
        return self.validateResource(r);
    });

    return Q.allSettled(validates).then(function (results) {
        self.logger.log('all violations complete')

        var violations = results.reduce(function (memo, result) {
            if (result.state === 'fulfilled' && result.value) {
                memo += result.value;
            }
            return memo;
        }, 'file_name,field_name,rule,message,violation_severity,violation_count\n');

        var count = _.reduce(violations.split('\n'), function (c, r, i) { 
            var fields = r.split(',');
            if(i > 0 && fields.length == 6) c += parseInt(fields[5]);
            return c;
        }, 0);

        self.meta.state = 'uploaded';
        self.meta.validation = {
            valid: (count === 0),
            count: count
        };

        var params = {
            Body: violations,
            Key: self.path + 'summary.csv',
            ContentEncoding: 'utf-8',
            ContentType: 'text/csv'
        };

        var putViolationSummary = function () { return Q.nbind(self.s3.putObject, self.s3)(params) };

        return self.putMeta()
            .then(putViolationSummary)
            .then(function () {
                self.logger.log('put violation summary to : ' + params.Key);
            });
    });
};

Upload.prototype.importResource = function (client, resource) {
    var self = this;
    var deferred = Q.defer();
    var pgStream;
    self.logger.log('importing: ' + resource.path);

    var args = [
        '-v','action=insert', 
        '-v','addfields=upload_id',
        '-v','addvals=' + self.uploadId,
        '-v', 'CSVFILENAME='+resource.path, resource.bawlk.script
    ];
    var awk = spawn('awk', args);
    awk.stdout.setEncoding('utf8')

    self.logger.log('pg importing ' + resource.path)

    awk.stdin.on('error', function (err) {
        this.emit('end');
        return deferred.reject('error importing from awk.stdin: ' + resource.path + ': ' + util.inspect(err));
    });

    awk.stdout.on('end', function () {
        // pgStream.end();
        // self.logger.log('success importing '+ resource.path);
        // return deferred.resolve('success importing '+ resource.path);
    });

    awk.stdout.on('error', function (err) {
        this.emit('end');
        return deferred.reject('error importing from awk.stdout: ' + resource.path + ': ' + util.inspect(err));
    });

    awk.stdout
        .pipe(es.split())
        .pipe(es.through(function (line) {
            var that = this;
            var isCopy = /^COPY/.test(line);
            var isRecord = !/^(SET|\\.)/.test(line);

            if (isCopy && !pgStream) {
                pgStream = client.query(copyFrom(line));
                pgStream.on('error', function (err) {
                    this.end();
                    that.emit('end');
                    pgStream=null
                    self.logger.log(err, true);
                    return deferred.reject({resource: resource.path, error: err.message });
                });
            } else if(pgStream && isRecord) {
                if (line) pgStream.write(Buffer(line+'\n'));
                else {
                    pgStream.end();
                    that.emit('end');
                    return deferred.resolve('success importing '+ resource.path);
                }
            } else that.emit('end');
        }));

    resource.data.pipe(awk.stdin);

    return deferred.promise;
}

// todo: could be optimized using https://github.com/vitaly-t/pg-promise
Upload.prototype.importResources = function () {
    var self = this;
    var deferred = Q.defer();

    this.logger.log('importing resources for: ' + self.uploadId);

    pg.connect(self.pgUrl, function(err, client, done){
        if(err) {
            self.logger.log('error connecting to the db: ' + err);
            done();
            return deferred.reject();
        }

        function mapSeries (arr, iterator) {
          // create a empty promise to start our series (so we can use `then`)
          var currentPromise = Q()
          var promises = arr.map(function (el) {
            return currentPromise = currentPromise.then(function () {
              // execute the next function after the previous has resolved successfully
              return iterator(el)
            })
          })
          // group the results and return the group promise
          return Q.all(promises);
        }

        var transaction = function (client, cb) {

            var imports = mapSeries(self.datapackage.resources, function (resource) {
                return self.importResource(client, resource);
            });

            return Q.all(imports).then(function (results) {
                return cb(null, results);
            }, function (err) { cb(err); });
        };

        pgTransact(client, transaction, done)
            .then(function () {
                self.logger.log('importing successful; transaction committed');
                self.meta.state = 'staged'
                self.putMeta()
                deferred.resolve();
            }, function(err) {
                self.logger.log('error importing... rolling back transaction.');
                deferred.reject(err);
            });

    });

    return deferred.promise;
};

module.exports = Upload;