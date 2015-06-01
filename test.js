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
                    key: 'dev.afgo.pgyi/uploads/yves.richard@tesera.com/tsi/75eb99ab-47a1-4bf0-b085-570aaefe3173/validate/datapackage.zip'
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
