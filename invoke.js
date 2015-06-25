#!/usr/bin/env node

//usage: ./invoke.js s3://tesera.datathemes/dev.afgo.pgyi/uploads/yves.richard@tesera.com/blue/3c3b1317-bb2e-4dc8-a11c-dde03233a671/stage/datapackage.zip

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
    ]
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

lambda.handler(evt, context, true);
