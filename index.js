'use strict';
var stream = require('stream');
var spawn = require('child_process').spawn;
var aws = require('aws-sdk');
var s3 = new aws.S3();
var _ = require('lodash');
var AdmZip = require('adm-zip');
var Q = require('q');
var bawlk = require('bawlk');


function invokeAwk(dataStream, awkArgs) {
    var deferred = Q.defer();
    var awk = spawn('awk', awkArgs);
    var result = '';

    awk.stdout.on('data', function (chunk) {
        result+=chunk.toString('utf8');
    });

    awk.stdout.on('end', function () {
        console.log() // todo: fix, if remove get EPIPE err!! https://github.com/joyent/node/issues/3211
        return deferred.resolve(result);
    });

    awk.stdout.on('error', function (err) {
        return deferred.reject(err);
    });

    dataStream.pipe(awk.stdin);

    return deferred.promise;
}

function getBawlkScript(schema) {
    var rules = bawlk.getRulesetsFromSchema(schema);
    return invokeAwk(rules, ['-F', ',', '-f', './node_modules/bawlk/bin/bawlk.awk']);
}

function invokeBawlk(csvStream, filename, action, bawlkScript){
    return invokeAwk(csvStream, ['-F', ',', '-v', 'FILENAME='+filename , '-v', 'action='+action, bawlkScript]);
}

function validateCsv(csvStream, filename, bawlkScript) {
    return invokeBawlk(csvStream, filename, 'validate', bawlkScript);
}

function importCsv(csvStream, filename, bawlkScript) {
    return invokeBawlk(csvStream, filename, 'insert', bawlkScript);
}

exports.handler = function(event, context) {
    var params = {
        Bucket: event.Records[0].s3.bucket.name,
        Key: event.Records[0].s3.object.key
    };

    if (event.Records[0].eventName == 'ObjectCreated:Put' && event.Records[0].s3.object.key.indexOf('datapackage.zip') != -1) {
        s3.getObject(params, function (err, res) {
            var zip = new AdmZip(res.Body);
            var datapackage = JSON.parse(zip.getEntry('datapackage.json').getData().toString('utf8'));

            var initResources = _.map(datapackage.resources, function (resource) {
                var csv = zip.getEntry(resource.path).getData().toString('utf8');

                var csvStream = new stream.Readable();
                csvStream.push(csv);
                csvStream.push(null);

                return getBawlkScript(resource.schema).then(function (script) {
                    return {
                        filename: resource.path,
                        csvStream: csvStream,
                        bawlkScript: script
                    };
                });
            });

            Q.allSettled(initResources)
                .then(function (results) {
                    var validates = results.map(function (result) {
                        if (result.state == 'fulfilled') {
                            return validateCsv(result.value.csvStream, result.value.filename, result.value.bawlkScript);
                        } else {
                            return Q.reject('error valdiating');
                        }
                    });

                    return Q.allSettled(validates);
                })
                .then(function (results) {
                    var violations = results.reduce(function (memo, result) {
                        memo+=result.value;
                        return memo;
                    }, '');

                    params.Body = violations;
                    params.Key = params.Key.replace('datapackage.zip', 'violations.csv');
                    params.ContentEncoding = 'utf-8';
                    params.ContentType = 'text/csv';

                    s3.putObject(params, function (err, data) {
                        if (err) {
                            console.log(err, err.stack);
                        }
                        else {
                            context.done(null,'');
                            console.log(data);
                        }
                    });
                });
        });
    } else {
        context.done(null,'');
    }
};