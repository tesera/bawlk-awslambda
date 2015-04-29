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
                    key: 'afgo.pgyi/uploads/yves.richard@tesera.com/tsi/6137581c-9517-42e3-a23d-0dfee58fe623/stage/datapackage.zip'
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