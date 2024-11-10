aws s3 sync s3://georgiavotesvisual-latest/static s3://georgiavotesvisual/static
aws cloudfront create-invalidation --distribution-id EE4GWZ043VGRF --paths "/*"