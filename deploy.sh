#!/usr/bin/env bash

# desc:   	zips up lambda function resources and uploads to aws
# usage:  	./scripts/deploy.sh
# docs:   	http://docs.aws.amazon.com/cli/latest/reference/lambda/upload-function.html

zip -r function.zip package.json node_modules/* fkeychecks.sh index.js gawk

aws lambda update-function-code \
    --function-name pg-import \
    --zip-file fileb://function.zip

rm function.zip
