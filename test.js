var lambda = require('./index.js');

var evt = {
    Records:[ 
        {
            s3: {
                bucket: {
                    name: 'tesera.datathemes'
                },
                object: {
                    key: 'datapackage.zip'
                }
            }
        }
    ]
}

lambda.handler(evt);