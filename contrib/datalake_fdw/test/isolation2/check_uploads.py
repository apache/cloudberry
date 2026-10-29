"""Inspect only this test's unique object key; never infer cleanup from ListObjects."""
import os
import sys

import boto3
from botocore.config import Config

client = boto3.client('s3', endpoint_url=os.environ['PR2044_S3_ENDPOINT'],
                      region_name='us-east-1', config=Config(s3={'addressing_style': 'path'}))
bucket = os.environ['PR2044_S3_BUCKET']
key = os.environ['PR2044_S3_KEY']
uploads = [upload for page in client.get_paginator('list_multipart_uploads').paginate(
    Bucket=bucket, Prefix=key) for upload in page.get('Uploads', []) if upload['Key'] == key]
mode = sys.argv[1]
if mode == 'cleanup':
    for upload in uploads:
        client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload['UploadId'])
else:
    expected = 1 if mode == 'before' else 0
    print('multipart uploads {} abort: {}'.format(mode, len(uploads)))
    if len(uploads) != expected:
        sys.exit(1)
    print('PR2044_S3_{}_PASS'.format(mode.upper()))
