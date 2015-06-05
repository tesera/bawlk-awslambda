#!/usr/bin/env bash

# desc:   zips up lambda function resources and uploads to aws
# usage:  ./scripts/deploy.sh
# docs:   http://docs.aws.amazon.com/cli/latest/reference/lambda/upload-function.html

zip -r function.zip package.json node_modules/* lib/* .env  fkeychecks.sh index.js

aws lambda upload-function \
  --function-name bawlk-s3-importer \
  --function-zip function.zip  \
  --runtime nodejs  \
  --role arn:aws:iam::674223647607:role/lambda_exec_role \
  --handler index.handler  \
  --mode event  \
  --timeout 60  \
  --memory-size 1024

rm function.zip
