'use strict';
var fs = require('fs');
var path = require('path');
var aws = require('aws-sdk');
var Q = require('q');
var request = require('request');
var winston = require('winston');
var aws = require('aws-sdk');
var spawn = require('child_process').spawn

var Upload = require('./lib/upload');

require('node-env-file')('.env');

exports.handler = function(event, context) {
    // exit if event not from a datapackage.zip put or copy
    if (!/datapackage.zip$/.test(event.Records[0].s3.object.key)) {
      return context.done(null);
    }

    var source = {
        bucket: event.Records[0].s3.bucket.name,
        key: decodeURIComponent(event.Records[0].s3.object.key)
    };
    var slugs = source.key.split('/');
    var outcome = 'success';

    var uploadOptions = {
        bucket: source.bucket,
        key: source.key,
        pgUrl: process.env.PGURL,
        gawk: event.local ? 'gawk' : './gawk'
    };

    // parse out env from bucket namespace i.e. qa.afgo.pgyi = qa
    var env = slugs[0].split('.')[0];
    uploadOptions.pgUrl = uploadOptions.pgUrl.replace('_dev', '_' + env);

    var s3 = new aws.S3({params: {Bucket: source.bucket}});
    var upload = new Upload(uploadOptions);

    var logger = new (winston.Logger)({
        transports: [
            new (winston.transports.Console)(),
            new (winston.transports.File)({ filename: path.join(upload.localPath, 'logs.ldj') })
        ]
    });

    upload.logger = logger;

    upload.on('ready', function() {
        logger.log('info', 'triggered by requestId ' + event.awsRequestId + ' with PUT of: ' + source.key);
        logger.log('info', 'upload ready and calling: ' + upload.action);
        var actionHandler = actionHandlers[upload.action];

        actionHandler(upload)
            .catch(function (err) {
                if (err) logger.error(err);
                outcome = 'failed';
                return false;
            })
            .finally(function() {
                var deferred = Q.defer();
                var args = ['s3', 'sync', upload.localPath, 's3://tesera.datathemes/' + upload.path];
                var sync = spawn('aws', args);
                sync.stdout.on('end', function () {
                    require('rimraf')(upload.localPath, function () {
                        deferred.resolve();
                    });
                });
                return deferred.promise;
            })
            .done();
    });

    var actionHandlers = {
        validate: function (upload) {
            logger.log('info', 'validate invoked for :' + source.key);
            return upload.checkForeignKeys()
                .then(function () {
                    return upload.validateResources();
                });
        },
        stage: function (upload) {
            logger.log('info', 'import invoked for :' + source.key);
            var rds = new aws.RDS({
                region: process.env.AWS_RDS_REGION || 'us-east-1',
                params: {
                    DBSecurityGroupName: process.env.AWS_RDS_SECURITY_GROUP || 'default'
                }
            });
            var lambdaCIDRIP;

            function getLambdaCIDRIP() {
                return Q.nfcall(request, process.env.IP_SVC)
                    .then(function (args) {
                        var info = JSON.parse(args[1]);
                        lambdaCIDRIP = info.ip + '/32';
                        return logger.log('info', 'lambda CIDRIP is ' + lambdaCIDRIP);
                    });
            }

            function authorizeCIDRIP() {
                logger.log('info', 'authorizingSecurityGroupIngress for : ' + lambdaCIDRIP);
                var auth = Q.nbind(rds.authorizeDBSecurityGroupIngress, rds);
                return auth({ CIDRIP: lambdaCIDRIP })
                    .fail(function (err) {
                        if(err.code !== 'AuthorizationAlreadyExists') {
                          throw new Error(err);
                        } else {
                          return logger.log('info', 'AuthorizationAlreadyExists for CIDRIP ' + lambdaCIDRIP);
                        }
                    });
            }

            function revokeCIDRIP() {
                logger.log('info', 'revokeSecurityGroupIngress for CIDRIP ' + lambdaCIDRIP);
                var revoke = Q.nbind(rds.revokeDBSecurityGroupIngress, rds);
                return revoke({ CIDRIP: lambdaCIDRIP });
            }

            var importResources = upload.importResources.bind(upload);

            return getLambdaCIDRIP()
                .then(authorizeCIDRIP)
                .then(importResources)
                .finally(revokeCIDRIP);
        }
    };
};
