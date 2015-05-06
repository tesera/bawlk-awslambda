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
                    key: 'afgo.pgyi/uploads/yves.richard@tesera.com/tsi/36471b2d-dea0-49d8-8f0b-1ead5d07a162/stage/datapackage.zip'
                }
            }
        }
    ]
};

var context = {
    done: function(err, data) {
        if(err) {
            console.log('lambda exited with errors: %s', err);
        } else {
            console.log('lambda exited without errors: %s', data);
        }
    }
};

lambda.handler(evt, context, true);
