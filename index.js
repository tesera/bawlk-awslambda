var sys = require('sys');
var fs = require('fs');
var aws = require('aws-sdk');
var _ = require('lodash');
var Q = require('q');
var request = require('request');

var Upload = require('./lib/upload');
var Logger = require('./lib/logger');

exports.handler = function(event, context, debug) {
    // exit if event not from a datapackage.zip put or copy
    if (!/datapackage.zip$/.test(event.Records[0].s3.object.key)) return context(null);

    var env = {
        awsDefaultRegion: "us-east-1",
        rdsSecurityGroup: "default",
        pgUrl: "postgres://localhost/afgo_dev"
    };
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
        debug: debug || false
    };
    var logger = new Logger(loggerOptions);

    function loadEnv() {
        var envFile = 'env.json';
        var read = Q.denodeify(fs.readFile);

        return read(envFile).then(function (data) {
            env = JSON.parse(data.toString('utf8'));
            return logger.log('loaded env file');
        }, function () {
            return logger.log('no env file; using defauls');
        });
    }

    loadEnv()
        .then(function () {
            var uploadOptions = {
                bucket: source.bucket,
                key: source.key,
                logger: logger,
                pgUrl: env.pgUrl
            };
            var upload = new Upload(uploadOptions);
            logger.log('triggered by put with: ' + source.key);

            upload.on('ready', function() {
                logger.log('upload ready and calling: ' + upload.action);
                var actionHandler = actionHandlers[upload.action];

                actionHandler(upload)
                    .fail(function () {
                        return outcome = 'failed';
                    })
                    .fin(function() {
                        logger.log(upload.action + ' ' + outcome + ' for :' + source.key);
                        logger.log('la fin');

                        logger.save()
                            .then(function () {
                                return context.done(null);
                            });
                    })
                    .done();
            });
        });

    var actionHandlers = {
        validate: function (upload) {
            logger.log('validate invoked for :' + source.key);
            return upload.validateResources();
        },
        import: function (upload) {
            logger.log('import invoked for :' + source.key);
            var rds = new aws.RDS({
                region: env.awsDefaultRegion,
                params: {
                    DBSecurityGroupName: env.rdsSecurityGroup
                }
            });
            var lambdaCIDRIP;

            function getLambdaCIDRIP() {
                return Q.nfcall(request, 'http://ipinfo.io')
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
                        if(err.code !== 'AuthorizationAlreadyExists') throw new Error(err);
                        else return logger.log('AuthorizationAlreadyExists for CIDRIP ' + lambdaCIDRIP);
                    });
            };

            function revokeCIDRIP() {
                logger.log('revokeSecurityGroupIngress for CIDRIP ' + lambdaCIDRIP);
                var revoke = Q.nbind(rds.revokeDBSecurityGroupIngress, rds);
                return revoke({ CIDRIP: lambdaCIDRIP });
            }

            var importResources = upload.importResources.bind(upload);

            return getLambdaCIDRIP()
                .then(authorizeCIDRIP)
                .then(importResources)
                .fin(revokeCIDRIP);
        }
    }
};