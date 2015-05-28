'use strict';
var lambda = require('./index.js');

var evt = {
    Records:[
        {
            eventName: 'ObjectCreated:Put',
            s3: {
                bucket: {
                    name: 'tesera.datathemes'
                },
                object: {
                    key: 'afgo.pgyi/uploads/yves.richard@tesera.com/tsi/a52a3673-7ad4-4fee-9966-ad7454b7f15e/stage/datapackage.zip'
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
