import os
import boto3
from boto3.exceptions import Boto3Error
from botocore.exceptions import ClientError

import logging
logger = logging.getLogger(__name__)


def get_s3_client(access_key, secret, endpoint=None, region=None):
    return boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret,
        endpoint_url=endpoint,
        region_name=region,
        # config=boto3.session.Config(signature_version='s3v4'),
    )


def get_s3_resource(access_key, secret, endpoint=None, region=None):
    return boto3.resource(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret,
        endpoint_url=endpoint,
        region_name=region,
        # config=boto3.session.Config(signature_version='s3v4'),
    )


class S3Exception(Exception):
    pass


class S3Bucket(object):
    def __init__(self, bucket_name=None, access_key=None, secret=None, endpoint=None, region=None):
        self.bucket_name = bucket_name or os.environ.get('S3_BUCKET_NAME')
        self.client = get_s3_client(
            access_key,
            secret,
            endpoint=endpoint,
            region=region
        )
        self.resource = get_s3_resource(
            access_key,
            secret,
            endpoint=endpoint,
            region=region
        )

    def exists(self, s3_key):
        is_existed = False
        try:
            result = self.list_files(s3_key)
            is_existed = bool(result)
        except Exception as ex:
            logger.info('check_if_key_exist_in_s3 false. key = %s, msg = %s', s3_key, ex)
        return is_existed

    def upload_file(self, localpath, s3_key=None):
        if not os.path.exists(localpath):
            return False
        s3_key = s3_key or os.path.basename(localpath)
        self.client.upload_file(localpath, self.bucket_name, s3_key)
        return True

    def upload_file_blob(self, file_blob, s3_key):
        self.client.put_object(Bucket=self.bucket_name, Key=s3_key, Body=file_blob)
        return True

    def download_file_blob(self, s3_key):
        obj = self.resource.Object(self.bucket_name, s3_key)
        body = obj.get()
        blob = body['Body'].read()
        return blob

    def get_signed_url(self, s3_key, expiredin=86400, httpmethod=None):
        try:
            url = self.client.generate_presigned_url(
                ClientMethod='get_object',
                Params={
                    'Bucket': self.bucket_name,
                    'Key': s3_key,
                },
                ExpiresIn=expiredin,
                HttpMethod=httpmethod,
            )
        except Exception as e:
            logger.error(e)
            raise S3Exception('FAILED TO GET SIGNED S3 LINK, PLEASE TRY AGAIN')
        return url

    def get_file_meta_info(self, s3_key, meta_info_name):
        try:
            meta_info = self.client.head_object(Bucket=self.bucket_name, Key=s3_key)
            return meta_info.get(meta_info_name)
        except (ClientError, Boto3Error, Exception) as ex:
            raise S3Exception(ex)

    def create_bucket_if_not_exists(self):
        existing = [b['Name'] for b in self.client.list_buckets()['Buckets']]
        if self.bucket_name not in existing:
            self.resource.create_bucket(Bucket=self.bucket_name)
            print('Created bucket:', str(self.bucket_name))
        return self.resource.Bucket(self.bucket_name).creation_date

    def delete_bucket(self):
        """
        Only used in testing environment
        """
        existing = [b['Name'] for b in self.client.list_buckets()['Buckets']]
        if self.bucket_name in existing:
            bucket = self.resource.Bucket(self.bucket_name)
            _ = [key.delete() for key in bucket.objects.all()]
            bucket.delete()

    def list_files(self, path_prefix=None):
        if path_prefix:
            keys = [obj.key for obj in self.resource.Bucket(self.bucket_name).objects.filter(Prefix=path_prefix)]
        else:
            keys = [obj.key for obj in self.resource.Bucket(self.bucket_name).objects.all()]
        return keys


def main():
    s3 = S3Bucket(bucket_name='mybucket')
    s3.list_files('path/to/my/folder/')


if __name__ == '__main__':
    main()
