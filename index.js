'use strict';
var aws = require('aws-sdk');
var Q = require('q');
var request = require('request');

var Upload = require('./lib/upload');
var Logger = require('./lib/logger');

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
    var uploadPath = slugs.slice(0, -1).join('/');
    var outcome = 'success';

    var loggerOptions = {
        bucket: source.bucket,
        key: uploadPath + '/logs.json',
        debug: true
    };
    var logger = new Logger(loggerOptions);

    var uploadOptions = {
        bucket: source.bucket,
        key: source.key,
        logger: logger,
        pgUrl: process.env.PGURL
    };

    logger.log('triggered by requestId ' + event.awsRequestId + ' with PUT of: ' + source.key);

    if(slugs[0] === 'qa.afgo.pgyi') {
        uploadOptions.pgUrl = uploadOptions.pgUrl.replace('afgo_dev', 'afgo_qa');
    }

    var upload = new Upload(uploadOptions);

    upload.on('ready', function() {
        logger.log('upload ready and calling: ' + upload.action);
        var actionHandler = actionHandlers[upload.action];

        actionHandler(upload)
            .catch(function (err) {
                if (err) logger.error(err);
                outcome = 'failed';
                return false;
            })
            .finally(function() {
                logger.log(upload.action + ' ' + outcome + ' for :' + source.key);
                logger.log('la fin');

                logger.save()
                    .then(function () {
                        return context.done(null);
                    });
            })
            .done();
    });

    var actionHandlers = {
        validate: function (upload) {
            logger.log('validate invoked for :' + source.key);
            return upload.checkForeignKeys().then(function () {
                if(!upload.errors && upload.meta.validation.valid) {
                    return upload.validateResources();
                }
            });
        },
        stage: function (upload) {
            logger.log('import invoked for :' + source.key);
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
                        return logger.log('lambda CIDRIP is ' + lambdaCIDRIP);
                    });
            }

            function authorizeCIDRIP() {
                logger.log('authorizingSecurityGroupIngress for : ' + lambdaCIDRIP);
                var auth = Q.nbind(rds.authorizeDBSecurityGroupIngress, rds);
                return auth({ CIDRIP: lambdaCIDRIP })
                    .fail(function (err) {
                        if(err.code !== 'AuthorizationAlreadyExists') {
                          throw new Error(err);
                        } else {
                          return logger.log('AuthorizationAlreadyExists for CIDRIP ' + lambdaCIDRIP);
                        }
                    });
            }

            function revokeCIDRIP() {
                logger.log('revokeSecurityGroupIngress for CIDRIP ' + lambdaCIDRIP);
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
