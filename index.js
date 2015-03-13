'use strict';
var stream = require('stream');
var spawn = require('child_process').spawn;
var aws = require('aws-sdk');
var s3 = new aws.S3();
var AdmZip = require('adm-zip');
var bawlk = require('bawlk');
var es = require('event-stream');
var Readable = require('stream').Readable;
var reduce = require("stream-reduce");

exports.handler = function(event, context) {
    var params = {
        Bucket: event.Records[0].s3.bucket.name,
        Key: event.Records[0].s3.object.key
    };

    if (event.Records[0].eventName == 'ObjectCreated:Put' && event.Records[0].s3.object.key.indexOf('datapackage.zip') != -1) {
        s3.getObject(params, function (err, res) {
            var zip = new AdmZip(res.Body);
            var datapackage = JSON.parse(zip.getEntry('datapackage.json').getData().toString('utf8'));

            var streams = datapackage.resources.map(function (resource) {
                var resourceStream = new Readable({ objectMode: true });
                resourceStream.push(resource);
                resourceStream.push(null);

                var csv = zip.getEntry(resource.path).getData().toString('utf8');
                var csvStream = new stream.Readable();
                csvStream.push(csv);
                csvStream.push(null);

                return resourceStream
                    .pipe(bawlk.getRuleset())
                    .pipe(bawlk.getScript())
                    .pipe(reduce(function (acc, chunk) {
                        return acc + chunk;
                    }, ''))
                    .pipe(es.through(function (script) {
                        var self = this;
                        var args = ['-v', 'FILENAME='+resource.path, script.toString('utf8')];
                        var awk = spawn('awk', args);

                        awk.stdout.setEncoding('utf8');

                        awk.stdin.on('error', function () {
                            self.emit('end');
                        });

                        awk.stdout.on('data', function (data) {
                            self.emit('data', data);
                        });

                        awk.stdout.on('end', function (data) {
                            self.emit('end');
                        });

                        csvStream.pipe(awk.stdin);

                        self.pause();
                    }));
            });

            var violations = '';
            var out = es.merge(streams);

            out.on('data', function (chunk) {
                violations += chunk;
            });

            out.on('end', function () {
                params.Body = violations;
                params.Key = params.Key.replace('datapackage.zip', 'violations.csv');
                params.ContentEncoding = 'utf-8';
                params.ContentType = 'text/csv';

                s3.putObject(params, function (err, data) {
                    if (err) {
                        console.log(err, err.stack);
                    } else {
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