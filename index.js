'use strict';
var stream = require('stream');
var spawn = require('child_process').spawn;
var aws = require('aws-sdk');
var s3 = new aws.S3();
var _ = require('lodash');
var AdmZip = require('adm-zip');
var Q = require('q');
var bawlk = require('bawlk');

function validate(csv, schema) {
    var deferred = Q.defer();
    var rules = bawlk.getRulesetsFromSchema(schema);
    var awk = spawn('awk', ['-F', ',', '-f', './node_modules/bawlk/bin/bawlk.awk']);
    var validator;

    var csvStream = new stream.Readable();
    csvStream.push(csv);
    csvStream.push(null);

    rules.pipe(awk.stdin);

    awk.stdout.on('data', function (chunk) {
        validator+=chunk.toString('utf8');
    });

    awk.stdout.on('end', function () {
        var validator = spawn('awk', ['-F', ',', '-v', 'action=validate', validator]);

        csvStream.pipe(validator.stdin);
        validator.stdout.pipe(process.stdout)
    });
}

exports.handler = function(event, context) {
    var params = {
        Bucket: event.Records[0].s3.bucket.name,
        Key: event.Records[0].s3.object.key
    };

    s3.getObject(params, function (err, res) {
        var zip = new AdmZip(res.Body);
        var datapackage = JSON.parse(zip.getEntry('datapackage.json').getData().toString('utf8'));

        var validates = _.map(datapackage.resources, function (r) {
            var schema = r.schema;
            var csv = zip.getEntry(r.path).getData().toString('utf8');

            return validate(csv, schema); 
        });
    });
};