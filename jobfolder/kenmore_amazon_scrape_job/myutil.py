from google.cloud import storage
import pandas as pd
import io
import time
import google.cloud
import sys


def read_csv_from_sto(fullpath, projname, retry=5):
    try_cnt = 0
    rule_df = None
    while try_cnt < retry:
        try:
            sto_client = storage.Client(projname)
            bucketname = fullpath.split('/')[0]
            blobpath = '/'.join(fullpath.split('/')[1:])
            bucket = sto_client.bucket(bucketname)
            blob = bucket.blob(blobpath)
            rule_data = blob.download_as_string()
            rule_df = pd.read_csv(io.BytesIO(rule_data), encoding='utf8')
            break
        except:
            if sys.exc_info()[0] == google.cloud.exceptions.NotFound:
                break
            try_cnt += 1
            time.sleep(1)
            continue
    return rule_df


def save_csv_to_sto(fullpath, projname, df, retry=5):
    try_cnt = 0
    while try_cnt < retry:
        try:
            sto_client = storage.Client(projname)
            bucketname = fullpath.split('/')[0]
            blobpath = '/'.join(fullpath.split('/')[1:])
            bucket = sto_client.bucket(bucketname)
            blob = bucket.blob(blobpath)
            blob.upload_from_string(df.to_csv(index=False), content_type='text/csv')
            break
        except:
            print(sys.exc_info())
            try_cnt += 1
            time.sleep(1)
            continue
    print('Done')
