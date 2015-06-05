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
                    key: 'dev.afgo.pgyi/uploads/yves.richard@tesera.com/tsi/bc9da3a5-9dcc-401b-846d-9f060fc43ce9/validate/datapackage.zip'
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
