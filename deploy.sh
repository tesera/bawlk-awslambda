#!/usr/bin/env bash

# desc:   	zips up lambda function resources and uploads to aws
# usage:  	./scripts/deploy.sh
# docs:   	http://docs.aws.amazon.com/cli/latest/reference/lambda/upload-function.html

zip -r function.zip package.json node_modules/* lib/* .env  fkeychecks.sh index.js

aws lambda update-function-code \
    --function-name bawlk-s3-importer \
    --zip-file fileb://function.zip

rm function.zip
