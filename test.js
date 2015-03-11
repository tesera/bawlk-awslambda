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
                    key: 'afgo.pgyi/uploads/60cceca7-651e-4edd-afb6-01c5b097a137/datapackage.zip'
                }
            }
        }
    ]
};

var context = {
    done: function(err, data) {
        console.log(data);
    }
}

lambda.handler(evt, context);