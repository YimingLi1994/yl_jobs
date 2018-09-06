import pandas as pd
import requests
from lxml import etree
from io import StringIO, BytesIO
import pandas as pd
import json
import re
from typing import List
import myutil
import uuid

import time

def job_prep():

    sears_querystr = '''\
    select sears_pid  pid   from
    [yl3573-214601:scraping.kenmore_amazon_match]
    group by 1
    '''

    sears_df = pd.read_gbq(sears_querystr, 'yl3573-214601')
    name = str(uuid.uuid4())[-5:]

    df_all = pd.DataFrame()

    df_all['PID'] = sears_df['pid']
    df_all['SKU'] = sears_df['pid']
    df_all = df_all.dropna()
    df_all = df_all.drop_duplicates(subset=['PID'])

    ### Wrap up

    df_all['WEBSITE'] = 'Sears'  # < ------ 'Website name change
    df_all['URL'] = None
    ### Only ["Kenmore", "Craftsman","DieHard"]
    df_job = df_all[['WEBSITE', 'PID', 'SKU', 'URL']]

    xlst = json.loads(df_job.to_json(orient='records'))
    _ = [x.update({'JOB_TAG': json.dumps({'proxy': False, 'webdriver': True}), 'TAG': 'kenmore'}) for x in xlst]

    json_df = pd.DataFrame(data=[json.dumps(x) for x in xlst], columns=['payload'])

    df_shuffled = json_df.sample(frac=1)

    myutil.save_csv_to_sto('yl-crawl/crawl_job_site_map_walk_{}.csv'.format(name), 'yl3573-214601', df_shuffled)


    def get_url(sitemap_link):
        print(sitemap_link)
        urllst = []
        btcontent = requests.get(sitemap_link).content
        tree = etree.parse(BytesIO(btcontent))
        for eachnode in tree.xpath("//*[local-name() = 'loc']"):
            urllst.append(eachnode.text)
        df = pd.DataFrame(data=urllst)
        # df.to_gbq()
        return df
    shoretry= 5
    while True:
        try:
            df_all = get_url("http://www.searshometownstores.com/sitemap-products.xml")
            break
        except:
            if shoretry == 0:
                raise
            shoretry -= 1
            time.sleep(1)

    df_all.columns = ['URL']
    df_all['URL'] = [x + '?preview=3085' for x in df_all['URL'].values]
    ### --- Fetch the PID of each url

    def findPid(x):
        reret = re.findall('product\/(.*)$', x)
        if len(reret) > 0:
            return reret[0]
        else:
            return None

    df_all['PID'] = df_all['URL'].apply(findPid)
    df_all = df_all.dropna()
    df_all = df_all.drop_duplicates(subset=['PID'])

    ### Wrap up
    df_all['SKU'] = None
    df_all['WEBSITE'] = 'SearsHometownStores'  # < ------ 'Website name change

    ### Only ["Kenmore", "Craftsman","DieHard"]
    df_all['brand'] = [x[0] for x in df_all.loc[:, "PID"].str.split("-")]
    df_all = df_all[df_all['brand'].isin(["Kenmore", "Craftsman", "DieHard"])]
    df_job = df_all[['WEBSITE', 'PID', 'SKU', 'URL']]

    import numpy as np
    xlst = json.loads(df_job.to_json(orient='records'))
    _ = [x.update({'JOB_TAG': json.dumps({'proxy': False, 'webdriver': True}), 'TAG': 'kenmore'}) for x in xlst]

    json_df = pd.DataFrame(data=[json.dumps(x) for x in xlst], columns=['payload'])

    df_shuffled = json_df.sample(frac=1)

    df_shuffled.reset_index(drop=True, inplace=True)


    name = str(uuid.uuid4())[-5:]
    myutil.save_csv_to_sto('yl-crawl/crawl_job_{}_{}.csv'.format('scraper_general', name),
                           'yl3573-214601', df_shuffled)


if __name__ == '__main__':
    job_prep()