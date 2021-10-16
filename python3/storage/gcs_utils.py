import json
import googleapiclient.discovery
from oauth2client import service_account

import logging
logger = logging.getLogger(__name__)


class GcpStorage:
    """ Google Cloud Storage operations through [ Service Account ] as authentication """

    def __init__(self, bucket: str, cred_dict: dict, **kwargs):
        self.bucket = bucket
        self.scopes = kwargs.get('scopes') or ['https://www.googleapis.com/auth/devstorage.read_only']
        cred_obj = service_account.ServiceAccountCredentials.from_json_keyfile_dict(cred_dict, scopes=self.scopes)
        self.storage = googleapiclient.discovery.build('storage', 'v1', credentials=cred_obj)
        logger.info('[ GCP STORAGE ] SUCCESSFULLY INITIALIZED...')

    def verify_bucket_access(self):
        is_valid = False
        try:
            path_list = self.list_files('stats/installs')
            is_valid = bool(path_list)
        except Exception as e:
            logger.warning('[ VALIDATE BUCKET ] INVALID CREDENTAIL, VALIDATION FAILED: {}'.format(e))
        return is_valid

    def list_files(self, prefix=''):
        data = self.storage.objects().list(
            bucket=self.bucket, prefix=prefix, fields='items(name, timeCreated, updated)',
        ).execute()
        items = data['items'] if data.get('items') else []
        path_list = [x['name'] for x in items]
        logger.info('[ GET LIST ] DETECTED {} FILES IN BUCKET'.format(len(path_list)))
        return path_list

    def download_headers(self, cloud_path: str):
        headers = self.storage.objects().get(bucket=self.bucket, object=cloud_path).execute()
        return headers

    def download_blob(self, cloud_path: str):
        blob = self.storage.objects().get_media(bucket=self.bucket, object=cloud_path).execute()
        return blob


def main():
    cred_dict = json.loads(open('/path/to/cred.json').read())
    storage = GcpStorage('mybucket', cred_dict)
    assert storage.verify_bucket_access() is True, '[ VERIFY ACCESS ] FAILED ACCESS BUCKET'


if __name__ == '__main__':
    main()
