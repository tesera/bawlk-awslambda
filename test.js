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
                    key: 'afgo.pgyi/uploads/yves.richard@tesera.com/tsi/bd11e4c7-5cd8-4b97-8a68-5249db8f3ea8/validate/datapackage.zip'
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