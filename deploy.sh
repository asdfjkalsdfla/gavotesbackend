#!/bin/bash
set -e
set -x

aws s3 sync data/frontend/. s3://georgiavotesvisual-latest/static/
aws cloudfront create-invalidation --distribution-id E3PTAV7LNU8JS2 --paths "/static/*"