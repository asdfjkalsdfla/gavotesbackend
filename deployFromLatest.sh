#!/bin/bash
set -e
set -x

aws s3 sync s3://georgiavotesvisual-latest/static s3://georgiavotesvisual/static --cache-control "max-age=3600"
aws cloudfront create-invalidation --distribution-id EE4GWZ043VGRF --paths "/*"
