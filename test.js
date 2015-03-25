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
                    key: 'afgo.pgyi/uploads/yves.richard@tesera.com/tsi/ef9fd62e-d015-4d0b-8821-b73dc9721ba7/validate/datapackage.zip'
                }
            }
        }
    ]
};

var context = {
    done: function(err, data) {
        if(err) console.log('lambda exited with errors')
        else console.log('lambda exited without errors')
    }
}

lambda.handler(evt, context, true);