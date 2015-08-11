#!/usr/bin/env node

//usage: ./invoke.js s3://tesera.dataprofiles/dev.afgo.pgyi/users/yves.richard@tesera.com/upload/datapackage.zip

'use strict';
var lambda = require('./index.js');
var s3Url = require('url').parse(process.argv[2]);

var evt = {
    Records:[
        {
            eventName: 'ObjectCreated:Put',
            s3: {
                bucket: {
                    name: s3Url.hostname
                },
                object: {
                    key: s3Url.path.substring(1, s3Url.path.length)
                }
            }
        }
    ],
    local: true
};

var context = {
    done: function(err) {
        if(err) {
            console.log('lambda exited with errors: %s', err);
        } else {
            console.log('lambda exited without errors.');
        }
    }
};
console.log('invoking with ', JSON.stringify(evt));

lambda.handler(evt, context, true);
