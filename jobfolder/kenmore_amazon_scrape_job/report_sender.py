import datetime as dt
import pytz
import re
import json
import re
import emailsender
import numpy as np
import pandas as pd
import premailer


def srs_money_to_float(row):
    if row['Sears_Price'] is None:
        return np.nan
    else:
        return float(row['Sears_Price'].replace('$', '').replace(',', '').replace(' ', ''))


def amz_money_to_float(row):
    if row['Amazon_Price'] is None:
        return np.nan
    else:
        try:
            retprice = float(row['Amazon_Price'].replace('$', '').replace(',', '').replace(' ', ''))
        except:
            retprice = np.nan
        return retprice


def amazon_below_map(row):
    if row['Amazon_Price'] - row['MAP_price'] <= -0.0099:
        return 'True'
    else:
        return None


def shs_below_map(row):
    if row['SrsHmetwn_Price'] - row['MAP_price'] <= -0.0099:
        return 'True'
    else:
        return None


def sears_below_map(row):
    if row['Sears_Price'] - row['MAP_price'] <= -0.0099:
        return 'True'
    else:
        return None


def shs_prc_match(row):
    if np.fabs(row['Sears_Price'] - row['SrsHmetwn_Price']) <= 0.0099:
        return 'Matched'
    else:
        return None


def amz_prc_match(row):
    if np.fabs(row['Sears_Price'] - row['Amazon_Price']) <= 0.0099:
        return 'Matched'
    else:
        return None


def avail(row):
    if row['availability'] is None:
        return None
    patternlst = re.findall(string=row['availability'], pattern=r'.*(\(.*\))')
    if len(patternlst) == 0:
        return row['availability'].replace('.', '').strip()
    return row['availability'].replace(patternlst[0], '').replace('.', '').strip()


def highlight_in_cart(s):
    is_min = s == np.nanmin(s)
    is_max = s != np.nanmax(s)
    is_ture = [is_min[x] and is_max[x] for x in range(s.size)]
    is_nan = np.isnan(s)
    return ['background-color: #ecacf9' if is_ture[x] else \
                    '' for x in range(is_nan.size)]

def highlight_min(s):
    '''
    highlight the maximum in a Series yellow.
    '''
    is_min = s == np.nanmin(s)
    is_max = s != np.nanmax(s)
    is_ture = [is_min[x] and is_max[x] for x in range(s.size)]
    is_nan = np.isnan(s)
    return ['background-color: green' if is_ture[x] else \
                'background-color: #cccccc' if is_nan[x] else \
                    '' for x in range(is_nan.size)]


def highlight_cell(s):
    hilight = s == 'Matched'
    return ['background-color: #ff9900' if v else '' for v in hilight]


def isdanger(s):
    isdanger = s == 'True'
    return ['background-color: red' if v else '' for v in isdanger]


def makereport():
    lst_succ_crawl = pd.read_gbq('''SELECT table_id FROM [shc-pricing-dev:jx_crawl.__TABLES__] 
     WHERE REGEXP_MATCH(table_id, r'^succ_crawl_[0-9]+')
     ORDER BY creation_time DESC LIMIT 1''', 'shc-pricing-dev').values[0, 0]

    succ_crawl_l2 = pd.read_gbq('''SELECT table_id FROM [shc-pricing-dev:jx_crawl.__TABLES__] 
     WHERE REGEXP_MATCH(table_id, r'^succ_crawl_[0-9]+')
     ORDER BY creation_time DESC LIMIT 2''', 'shc-pricing-dev')['table_id'].values

    lst_static_tbl = pd.read_gbq('''SELECT table_id FROM [shc-pricing-prod:static_tables.__TABLES__] 
     WHERE REGEXP_MATCH(table_id, r'^static__table_[0-9]+')
     ORDER BY creation_time DESC LIMIT 1''', 'shc-pricing-prod').values[0, 0]
    lst_scrape_mapping = pd.read_gbq('''SELECT table_id FROM [shc-pricing-dev:jx_crawl.__TABLES__] 
     WHERE REGEXP_MATCH(table_id, r'^amazon_kenmore_scrape_mapping_[0-9]+')
     ORDER BY creation_time DESC LIMIT 1''', 'shc-pricing-dev').values[0, 0]

    datestr = dt.datetime.now(pytz.timezone('America/Chicago')).strftime('%Y%m%d')

    querystr = '''select sears_pid Sears_PID, amazon_pid ASIN, c.price Sears_Price, c.meta meta, b.price Amazon_Price, 
    d.price SrsHmetwn_Price,
    b.availability amz_availability, 
    b.LAST_CRAWL AMAZON_LAST_CRAWL, 
    d.LAST_CRAWL SrsHmetwn_LAST_CRAWL,
    c.LAST_CRAWL SEARS_LAST_CRAWL
    from
    (select * from [shc-pricing-dev:jx_crawl.{lst_scrape_mapping}]
    group by 1,2) a
    left join
    (select PID, first(price) price,
    first(availability ) availability,
    first(LAST_CRAWL ) LAST_CRAWL FROM ( SELECT PID, price, availability, LAST_CRAWL FROM [shc-pricing-dev:jx_crawl.{lst_succ_crawl}],[shc-pricing-dev:jx_crawl.{lst_succ_crawl1}] where WEBSITE ='Amazon' ORDER BY LAST_CRAWL DESC ) group by 1) b
    on a.amazon_pid = b.PID
    left join
    (select PID, first(price) price, first(meta) meta,
    first(LAST_CRAWL ) LAST_CRAWL FROM ( SELECT PID, price, meta, availability, LAST_CRAWL FROM [shc-pricing-dev:jx_crawl.{lst_succ_crawl}],[shc-pricing-dev:jx_crawl.{lst_succ_crawl1}] where WEBSITE ='Sears' ORDER BY LAST_CRAWL DESC ) group by 1) c
    on a.sears_pid = c.PID
    left join
    (select 
    concat( 
    regexp_extract(SKU, r'(\d{{3}})\d{{1}}\d{{5}}\d{{3}}') ,
    regexp_extract(SKU, r'\d{{3}}\d{{1}}(\d{{5}})\d{{3}}'),
    regexp_extract(SKU, r'\d{{3}}\d{{1}}\d{{5}}(\d{{3}})'),
    'P') PID,LAST_CRAWL,
    float(price) price,  from
    (SELECT *, row_number() over(partition by SKU order by LAST_CRAWL desc) _rn FROM [shc-pricing-dev:jx_crawl.{lst_succ_crawl}],[shc-pricing-dev:jx_crawl.{lst_succ_crawl1}] 
    where WEBSITE = 'SearsHometownStores' and price is not null)
    where _rn =1) d
    on a.sears_pid = d.PID
    '''.format(lst_succ_crawl=succ_crawl_l2[0], lst_succ_crawl1=succ_crawl_l2[1], lst_scrape_mapping=lst_scrape_mapping)

    df = pd.read_gbq(querystr, 'shc-pricing-dev')


    def get_cart_prc(row):
        try:
            metadict = json.loads(row['meta'])
            for key, item in metadict.items():
                tempstr = ''.join(re.findall(r'[\d\.]', str(item)))
                metadict[key] = float(tempstr) if len(tempstr) > 0 else np.nan
            return np.nanmin([row['Sears_Price'], metadict['sales'], metadict['regular']])
        except Exception as e:
            # print(e,row['meta'])
            return np.nan

    df.columns = ['Sears_PID', 'ASIN', 'Sears_Price', 'meta', 'Amazon_Price', 'SrsHmetwn_Price', 'amz_availability',
                  'Amazon_LAST_CRAWL', 'SrsHmetwn_LAST_CRAWL', 'Sears_LAST_CRAWL']

    map_prc_str = '''\
    select b.sears_pid Sears_PID, round(MAP_price,2) MAP_price from
    (SELECT div_no , itm_no , MAP_price  
    FROM [shc-pricing-prod:static_tables.{lst_static_tbl}] 
     ) a
    join
    (select integer(substr(sears_pid , 1,3)) div_no, integer(substr(sears_pid, 4,5)) itm_no, sears_pid 
    from [shc-pricing-dev:jx_crawl.{lst_scrape_mapping}] 
    group by 1,2,3
    ) b
    on a.div_no = b.div_no and a.itm_no = b.itm_no
    '''.format(lst_static_tbl=lst_static_tbl, lst_scrape_mapping=lst_scrape_mapping)
    map_prc_df = pd.read_gbq(map_prc_str, 'shc-pricing-dev')

    map_df = pd.merge(df, map_prc_df, on=['Sears_PID'], how='left')

    bu_str = '''
    select a.sears_pid Sears_PID, b.BU BU  from
    (select integer(substr(sears_pid , 1,3)) div_no, integer(substr(sears_pid, 4,5)) itm_no, sears_pid 
    from [shc-pricing-dev:jx_crawl.amazon_kenmore_scrape_mapping_20180406] 
    group by 1,2,3
    ) a
    left join
    (SELECT Division, group_concat(BUSINESS_UNIT ,'-') BU FROM 
    (select Division,BUSINESS_UNIT from [shc-pricing-dev:Yiming.sears_BU] group by 1,2)
    group by 1) b
    on a.div_no = b.Division
    '''
    df_bu = pd.read_gbq(bu_str, 'shc-pricing-dev')

    def name_trans(x):
        return {'HOME': 'HOME', 'HOME APPLIANCES': 'HA', 'AUTOMOTIVE': 'AUTO'}[x]

    def name_rank(x):
        return {'HOME': 2, 'HOME APPLIANCES': 1, 'AUTOMOTIVE': 3}[x]

    df_bu['BU_name'] = df_bu['BU'].apply(name_trans)

    df_bu['BU_rank'] = df_bu['BU'].apply(name_rank)

    map_df = pd.merge(map_df, df_bu, on=['Sears_PID'], how='left')

    map_df = map_df.sort_values(by=['BU_rank', 'Sears_PID'])[
        ['BU_name', 'Sears_PID', 'ASIN', 'Sears_Price', 'meta', 'Amazon_Price', 'SrsHmetwn_Price',
         'amz_availability', 'Amazon_LAST_CRAWL', 'SrsHmetwn_LAST_CRAWL',
         'Sears_LAST_CRAWL', 'MAP_price']]

    def string_sub(x):
        if x is None:
            return None
        resub_dict = {
            r'''(?=only *(\d+) * left).*(?=in stock).*''': r'''In stock: \1 left''',
            r'''^ *in stock\. *$''': r'''In stock''',
            r'''^.*(?=currently unavailable).*''': r'''Currently Unavailable''',
            r'''.*(?<!temporarily )(?=out of stock).*''': r'''Out of Stock''',
            r'''^.*(?=we couldn.?t find).*(?=amazon.s home page).*''': r'''Not Found''',
            r'''^.*(?=temporarily).*(?=out of stock).*''': r'''Temporarily out of Stock''',
            r'''.*available from these sellers.*''': r'''Available in Marketplace''',
            r'''.*(?=usually ships).*(?=within).*(?=(\d+) *to *(\d+) *(days?|months?|weeks?|years?|hours?|minutes?)).*''': r'''Ship in \1 - \2 \3''',
        }
        for key, item in resub_dict.items():
            restr, flag = re.subn(key, item, x, flags=re.IGNORECASE)
            if flag > 0:
                return restr
        return x

    map_df['amz_availability'] = map_df['amz_availability'].apply(string_sub)

    # df_work = df[['WEBSITE', 'price', 'PID', 'LAST_CRAWL', 'availability']]
    # df_work['price'] = df_work.apply(money_to_float, axis=1)
    # df_amazon = df_work[df_work['WEBSITE'] == 'Amazon'][['price', 'PID', 'availability', 'LAST_CRAWL']]
    # df_amazon.columns = ['Amazon_Price', 'ASIN', 'availability', 'Amazon_LAST_CRAWL']
    # df_sears = df_work[df_work['WEBSITE'] == 'Sears'][['price', 'PID', 'LAST_CRAWL']]
    # df_sears.columns = ['Sears_Price', 'Sears_PID', 'Sears_LAST_CRAWL']


    df_work1 = map_df.copy()
    df_work1['Amazon_Price'] = df_work1.apply(amz_money_to_float, axis=1)
    df_work1['Sears_Price'] = df_work1.apply(srs_money_to_float, axis=1)

    df_work1['Sears_Cart_Prc'] = df_work1.apply(get_cart_prc, axis=1)

    df_work1['Amazon_Violated_MAP'] = df_work1.apply(amazon_below_map, axis=1)
    df_work1['Sears_Violated_MAP'] = df_work1.apply(sears_below_map, axis=1)
    df_work1['SrsHmetwn_Violated_MAP'] = df_work1.apply(shs_below_map, axis=1)
    df_work1['Amazon_Sears_Price_match'] = df_work1.apply(amz_prc_match, axis=1)
    df_work1['SrsHmetwn_Sears_Price_match'] = df_work1.apply(shs_prc_match, axis=1)
    # df_work1['availability'] = df_work1.apply(avail, axis=1)

    #df_work1 = df_work1.drop(columns=['meta'])

    df_send = df_work1[['BU_name', 'Sears_PID', 'ASIN', 'MAP_price', 'Sears_Price', 'Sears_Cart_Prc', 'Amazon_Price',
                        'SrsHmetwn_Price',
                        'Sears_Violated_MAP', 'Amazon_Violated_MAP', 'SrsHmetwn_Violated_MAP',
                        'Amazon_Sears_Price_match', 'SrsHmetwn_Sears_Price_match',
                        'amz_availability', 'Sears_LAST_CRAWL', 'Amazon_LAST_CRAWL', 'SrsHmetwn_LAST_CRAWL']]
    df_send.index = np.arange(1, len(df_send) + 1)
    column_consider = ['Sears_PID', 'ASIN', 'MAP_price', 'Sears_Price', 'Sears_Cart_Prc', 'Amazon_Price',
                       'SrsHmetwn_Price',
                       'Sears_Violated_MAP', 'Amazon_Violated_MAP', 'SrsHmetwn_Violated_MAP',
                       'Amazon_Sears_Price_match', 'SrsHmetwn_Sears_Price_match',
                       'amz_availability', 'Sears_LAST_CRAWL', 'Amazon_LAST_CRAWL', 'SrsHmetwn_LAST_CRAWL']

    df_HA = df_send[df_send['BU_name'] == 'HA'][column_consider]
    df_HA.index = np.arange(1, len(df_HA) + 1)
    df_Home = df_send[df_send['BU_name'] == 'HOME'][column_consider]
    df_Home.index = np.arange(1, len(df_Home) + 1)
    df_Auto = df_send[df_send['BU_name'] == 'AUTO'][column_consider]
    df_Auto.index = np.arange(1, len(df_Auto) + 1)

    html_HA = df_HA.style \
        .apply(highlight_min, subset=['Sears_Price', 'Amazon_Price', 'SrsHmetwn_Price'], axis=1) \
        .apply(highlight_cell, subset=['Amazon_Sears_Price_match']) \
        .apply(highlight_cell, subset=['SrsHmetwn_Sears_Price_match']) \
        .apply(highlight_in_cart, subset=['Sears_Price', 'Sears_Cart_Prc'], axis=1) \
        .apply(isdanger, subset=['Sears_Violated_MAP', 'Amazon_Violated_MAP', 'SrsHmetwn_Violated_MAP']) \
        .render()

    html_Home = df_Home.style \
        .apply(highlight_min, subset=['Sears_Price', 'Amazon_Price', 'SrsHmetwn_Price'], axis=1) \
        .apply(highlight_cell, subset=['Amazon_Sears_Price_match']) \
        .apply(highlight_cell, subset=['SrsHmetwn_Sears_Price_match']) \
        .apply(highlight_in_cart, subset=['Sears_Price', 'Sears_Cart_Prc'], axis=1) \
        .apply(isdanger, subset=['Sears_Violated_MAP', 'Amazon_Violated_MAP', 'SrsHmetwn_Violated_MAP']) \
        .render()
    html_Auto = df_Auto.style \
        .apply(highlight_min, subset=['Sears_Price', 'Amazon_Price', 'SrsHmetwn_Price'], axis=1) \
        .apply(highlight_cell, subset=['Amazon_Sears_Price_match']) \
        .apply(highlight_cell, subset=['SrsHmetwn_Sears_Price_match']) \
        .apply(highlight_in_cart, subset=['Sears_Price', 'Sears_Cart_Prc'], axis=1) \
        .apply(isdanger, subset=['Sears_Violated_MAP', 'Amazon_Violated_MAP', 'SrsHmetwn_Violated_MAP']) \
        .render()

    body_str = '<p>Generated by the <b>Harlem-125th Platform</b> </p>' + \
               '<style>table, tr, td, th {border: 1px solid black; margin: 0px;}</style>' + \
               '<h2>BU: HA</h2>' + \
               html_HA + \
               '<h2>BU: Home</h2>' + \
               html_Home + \
               '<h2>BU: Auto</h2>' + \
               html_Auto

    new_str = premailer.transform(body_str)

    emailsender.sendemail(titlestr='Kenmore Web Intelligence',
                          msgstr=new_str,
                          bcclst=[
                                'jianwei.xiao2@searshc.com',
                                'yiming.li2@searshc.com',
                                'Sashi.Marella@searshc.com',
                                'wenxue.zhang@searshc.com','Pritam.Baxi@searshc.com',
                                'Rishi.Potdar@searshc.com',
                                'Jeremy.Person@kcdbrands.com',
                                'KenmoreScrapeReporting@searshc.com',
                                'harsha.chandar2@searshc.com',
                                'harlem125_platform@searshc.com'
                          ])

if __name__ == '__main__':
    makereport()
