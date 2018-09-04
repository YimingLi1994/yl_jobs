import json
import string
import traceback
from io import StringIO
import re
import lxml.html as lh
import requests
import pandas as pd
import datetime as dt
import pytz
from multiprocessing.pool import ThreadPool as Pool
# from jsonpath_ng import
from jsonpath_ng.ext import parse
from tqdm import tqdm

def get_xpath(xpthstr, elem_tree):
    elemlst = elem_tree.xpath(xpthstr)
    if len(elemlst) > 0:
        return elemlst[0]
    else:
        return None


def get_content(URL):
    proxy = {"http": "http://10099:M9GUfa@hn4.nohodo.com:10099",
             "https": "http://10099:M9GUfa@hn4.nohodo.com:10099"}
    headers = {'Connection': 'keep-alive',
               'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
               'Accept-Encoding': 'gzip, deflate, br',
               'accept-language': 'en-US,en;q=0.8,zh-CN;q=0.6,zh;q=0.4',
               'upgrade-insecure-requests': '1',
               'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'}

    r = requests.get(URL, headers=headers, timeout=5, proxies=proxy)
    ready_str = r.text
    printable = set(string.printable)
    ready_str = ''.join(filter(lambda x: x in printable, ready_str))
    # with open('Amzcheck.html','w') as f:
    #     f.write(ready_str)
    elem_tree = lh.parse(StringIO(ready_str))
    return elem_tree


def fill_template(job_template):
    elem_tree = get_content(job_template['URL'])
    retdict = {}
    for key, value in job_template['JOB'].items():
        retdict[key] = None
        thisvalue = value
        if type(thisvalue) != list:
            thisvalue = [thisvalue]
        for each in thisvalue:
            if each is None:
                continue
            ret = elem_tree.xpath(each)
            if len(ret) > 0:
                target = ret[0]
                try:
                    retdict[key] = target.text_content()
                except:
                    retdict[key] = str((target))
                break
            else:
                retdict[key] = None
        if retdict[key] is not None:
            retdict[key] = re.sub(' +', ' ', retdict[key].strip().replace('\n', ' ')).strip()

    return retdict


def do_the_job(jobdict, driver=None):
    job_template = {
        'JOB': {
            'SKU': None,
            'UPC': None,
            'availability': ['//*[@id="availability"]',
                             '//*[@id="sns-availability"]/div/span[contains(@class,\'size-medium\')]',
                             '//*[@id="outOfStock"]',
                             '//*[@id="fast-track"]',
                             "//img[contains(@src,'title') and contains(@src,'error') and contains(@alt, 'orry')]/@alt"],
            'brand': ['//div[@id="product-details-grid_feature_div"]//tr[contains(.,"Brand") or contains(.,"brand") ]',
                      '//*[@id="bylineInfo"]'],
            'channel': ['//*[@id="merchant-info"]',
                        '//*[@id="sns-availability"]/div/span[contains(@class,\'size-base\')]'],
            'meta': None,
            'model': ['//div[@id="product-details-grid_feature_div"]//tr[contains(.,"Model") or contains(.,"model")]',
                      '//*[@id="detail-bullets"]//li[contains(.,\'model number\')]',
                      '//*[@id="productDetailsTable"]//th[contains(.,\'model number\')]/parent::tr'],
            'page_path': ['//div[@id="wayfinding-breadcrumbs_feature_div"]/ul[contains(@class, "list")]',
                          '//*[@id="nav-subnav"]/a[1]'],
            'price': [
                '//div[@id="price"]//span[@id="priceblock_ourprice"]|//div[@id="price"]//span[@id="priceblock_dealprice"]',
            ],
            'price_type': ['//*[@id="onetimeOption"]/label/span',
                           '//*[@id="addon"]//i[contains(@class,\'addon\')]',
                           '//*[@id="burjActionPanelAddOnBadge"]'],
            'product_title': ['//span[@id="productTitle"]',
                              "//img[contains(@src,'title') and contains(@src,'error') and contains(@alt, 'orry')]/@alt"],
            'rating': '//span[@id="acrPopover"]/@title',
            'reviews': [
                '//div[@id="averageCustomerReviews_feature_div"]//a[contains(@id,"CustomerReviewLink")]/span[@id]',
                '//*[@id="averageCustomerReviews"]'],
            'shipping': ['//*[@id="ourprice_shippingmessage"]'],
            'style': ['//*[@id="variation_color_name"]',
                      '//*[@id="shelf-label-color_name"]'],
            'style2': ["//div[@id and contains(@class,'variation-dropdown')]//option[@id and @selected]",
                       '//div[@id="variation_size_name"]//div[@class]',
                       '//*[@id="shelf-label-size_name"]']},
        'URL': 'http://www.amazon.com/gp/product/{}?psc=1'.format(jobdict['PID']) if jobdict['URL'] is None else
        jobdict['URL'],
        'WEBSITE': 'Amazon'}

    try:
        retdict = fill_template(job_template)
        retwrapper = {
            'STATUS': 'SUCCESS',
            'PAYLOAD': json.dumps(retdict),
        }
    except:
        retwrapper = {
            'STATUS': 'FAIL',
            'PAYLOAD': json.dumps({'Reason': traceback.format_exc()}),
        }
        raise
    return retwrapper

import time
def scraping_job(row):
    testdict = {"WEBSITE": "Amazon",
                "PID": row,
                "SKU": None,
                "URL": None}
    column_lst = ['product_title', 'price', 'availability']
    retry = 10
    try:
        retlst = []
        retlst.append('Amazon')
        retlst.append(testdict['PID'])
        while retry > 0:
            try:
                ret_json = do_the_job(testdict)
                ret_dict = json.loads(ret_json['PAYLOAD'])
                for eachcol in column_lst:
                    retlst.append(ret_dict[eachcol])
                break
            except:
                time.sleep(1)
                if retry == 0:
                    raise

    except Exception as e:
        for eachcol in column_lst:
            retlst.append(None)
        print(row,e)
    finally:
        retlst.append('km_amz')
        #print(retlst)
        return retlst


if __name__ == '__main__':
    lst_mapping_tbl = pd.read_gbq('''SELECT table_id FROM [shc-pricing-dev:jx_crawl.__TABLES__] 
             WHERE REGEXP_MATCH(table_id, r'^amazon_kenmore_scrape_mapping_[0-9]+')
             ORDER BY creation_time DESC LIMIT 1''', 'shc-pricing-dev').values[0, 0]
    amz_querystr = '''select amazon_pid pid   from
[shc-pricing-dev:jx_crawl.{}]
group by 1 '''.format(lst_mapping_tbl)
    time_now = dt.datetime.now(pytz.timezone('America/Chicago'))
    datestr = time_now.strftime('%Y-%m-%d %H:%M:%S')
    amz_lst = pd.read_gbq(amz_querystr, 'shc-pricing-dev')
    wraplst = []
    with Pool(processes= 16 ) as p:
        max_ = len(amz_lst['pid'].values)
        with tqdm(total = max_) as pbar:
            for i, res in tqdm(enumerate(p.imap_unordered(scraping_job, amz_lst['pid'].values))):
                wraplst.append(res)
                pbar.update()

    #wraplst = [scraping_job(x) for x in tqdm.tqdm(amz_lst['pid'].values)]

    # wraplst = amz_lst.apply(scraping_job, axis=1)
    dfupload = pd.DataFrame(data=wraplst, columns=['WEBSITE', 'PID', 'product_title', 'price', 'availability', 'tag'])
    dfupload['LAST_CRAWL'] = datestr
    print(dfupload.shape)
    #dfupload.to_csv('check.csv',index=False)
    dfupload.to_gbq('jx_crawl.succ_crawl_{}'.format(time_now.strftime('%Y%m%d')), 'shc-pricing-dev', if_exists='append')
