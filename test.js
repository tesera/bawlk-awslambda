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
                    key: 'afgo.pgyi/uploads/22770144-ec75-4842-ae27-b3ca61545b87/datapackage.zip'
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