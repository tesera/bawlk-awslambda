'use strict';
var fs = require('fs');
var path = require('path');
var spawn = require('child_process').spawn;
var aws = require('aws-sdk');
var Q = require('q');
var request = require('request');
var winston = require('winston');
var AdmZip = require('adm-zip');
var mapSeries = require('promise-map-series');

exports.handler = function(event, context) {
    var gawk = './gawk';
    var source = {
        bucket: event.Records[0].s3.bucket.name,
        key: decodeURIComponent(event.Records[0].s3.object.key)
    };
    var uploadRoot = source.key.replace('datapackage.zip', '');
    var uploadPath = 's3://' + source.bucket + '/' + uploadRoot;
    var uploadId = source.key.split('/').slice(-2)[0];
    var wd = path.join('/tmp', uploadId);

    if (!/datapackage.zip$/.test(source.key)) {
        return context.done(null);
    }

    var logger = new (winston.Logger)({
        transports: [
            new (winston.transports.Console)(),
            new (winston.transports.File)({ filename: path.join(wd, 'logs.ldj') })
        ]
    });

    var s3 = new aws.S3({ params: { Bucket: source.bucket } });

    function checkFkeys() {
        var deferred = Q.defer();
        var violationsStream = fs.createWriteStream(path.join(wd, 'violations.csv'), {flags: 'a'});

        logger.log('info', 'checking foreign keys for upload');

        var checker = spawn('bash', [path.resolve(__dirname, './fkeychecks.sh'), wd]);

        checker.stdout.on('end', function () {
            logger.log('info', 'checked foreign keys finished');
            deferred.resolve();
        });

        checker.stdout.on('error', function (err) {
            logger.log('error', 'check foreign keys error', err);
            deferred.reject();
        });

        checker.stdout.pipe(violationsStream);

        return deferred.promise;
    }

    function invoke (args, outputStream) {
        var deferred = Q.defer();

        logger.log('info', 'invoking', args);

        var awk = spawn(gawk, args);

        awk.stdout.setEncoding('utf8');
        awk.stdout.on('end', deferred.resolve);
        awk.stdout.on('error', function (err) {
            logger.log('error', 'invoke error', args, err);
            deferred.reject();
        });

        awk.stdout.pipe(outputStream);

        return deferred.promise;
    }

    function validate () {
        logger.log('info', 'validating resources');

        var validates = mapSeries(datapackage.resources, function (resource) {
            var violationsStream = fs.createWriteStream(path.join(wd, 'violations.csv'), {flags: 'a'});
            var args = [
                '-v', 'action=validate',
                '-v', 'CSVFILENAME='+resource.path.split('/')[3],
                '-f', resource.validator,
                resource.path
            ];
            return invoke(args, violationsStream);
        });

        return Q.all(validates).then(function () {
            logger.log('info', 'all violations complete');
        });
    }

    // function insert () {
    //     logger.log('info', 'validating resources');

    //     var inserts = mapSeries(datapackage.resources, function (resource) {
    //         var insertStream = fs.createWriteStream(path.join(wd, 'insert.sql'), {flags: 'a'});

    //         var args = [
    //             '-v', 'action=insert',
    //             '-v', 'addfields=upload_id',
    //             '-v', 'addvals=' + uploadId,
    //             '-v', 'CSVFILENAME='+resource.path.split('/')[3],
    //             '-v', 'schema=psp.',
    //             '-f', resource.validator,
    //             resource.path
    //         ];
    //         return invoke(args, insertStream);
    //     });

    //     return Q.all(inserts).then(function () {
    //         logger.log('info', 'all inserts complete');
    //     });
    // }

    var datapackage;
    s3.getObject({Key: source.key}, function (err, datapackageData) {
        var zip = new AdmZip(datapackageData.Body);
        zip.extractAllTo(wd);
        datapackage = fs.readFileSync(path.join(wd, '/datapackage.json'), {encoding: 'utf8'});
        datapackage = JSON.parse(datapackage);

        var inits = datapackage.resources.map(function (resource) {
            var url = resource.validator;
            var deferred = Q.defer();

            logger.log('info', 'initializing', resource.path);

            resource.path = path.join(wd, resource.path);
            resource.validator = resource.path.replace('.csv', '.awk');

            request(url)
                .pipe(fs.createWriteStream(resource.validator))
                .on('close', function () {
                    logger.log('info', 'initialized', resource.path);
                    deferred.resolve();
                });

            return deferred.promise;
        });

        Q.allSettled(inits)
            .then(checkFkeys)
            .then(validate)
            // .then(insert)
            .finally(function() {
                var readdir = Q.denodeify(fs.readdir);

                return readdir(wd)
                    .then(function (files) {
                        var puts = files.map(function (file) {
                            var deferred = Q.defer();
                            var params = {
                                Key: path.join(uploadRoot, file),
                                Body: fs.createReadStream(path.join(wd, file))
                            };

                            logger.log('info', 'putting', params.Key);

                            s3.upload(params).send(function () {
                                logger.log('info', 'put', params.Key);
                                deferred.resolve();
                            });

                            return deferred.promise;
                        });
                        return Q.allSettled(puts);
                    }).then(function () {
                        logger.log('info', 'sync complete to:', uploadPath);
                        require('rimraf')(wd, function () {
                            logger.log('info', 'all done');
                        });
                    });
            })
            .done();
    });
};
