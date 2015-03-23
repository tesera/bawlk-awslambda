#!/usr/bin/env bash

# zips up lambda function resources and uploads to aws
# usage: LAMBDA_EXEC_ROLE_ARN=arn:aws:iam::{iam_id}:role/{role_name} ./scripts/deploy.sh
# aws.docs: http://docs.aws.amazon.com/cli/latest/reference/lambda/upload-function.html

zip -r function.zip package.json node_modules/* lib/* env.json index.js

aws lambda upload-function \
--function-name bawlk-s3-importer \
--function-zip function.zip  \
--runtime nodejs  \
--role $LAMBDA_EXEC_ROLE_ARN \
--handler index.handler  \
--mode event  \
--timeout 60  \
--memory-size 1024