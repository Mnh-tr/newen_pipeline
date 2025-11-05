from datetime import date,timedelta
from configs.constant import SEARCH_HASHTAG_INSTA_BANO_PJT
import utils.gcs_bq_uploader as gcp
import random
import time

import src.tiktok_contents.keyword_scraper as tiktok_contents_keyword

today_ = date.today() #- timedelta(days = 1)
today = today_.strftime("%Y%m%d")

bucket_name = "3_mk_abcinsight"
since = '1693624417'
keywords = SEARCH_HASHTAG_INSTA_BANO_PJT 
region = 'VN'
max_page = 5
break_keyword_no = 5
break_all = False

j,k = 1,0

for keyword in keywords:
    for page in range(0,max_page):
        prefix = f'influencer/keyword_search/tiktok/{keyword}/{region}/keyword_search_{keyword}_{region}_{page}_{today}.json'        
        lists = gcp.gcs_list(bucket_name,prefix)
        if lists != []:
            print(keyword + str(page) + ' : existing, pass')
            k += 1            
        else:
            print(keyword + str(page) + ' : get list and upload to gcs')
            tiktok_contents_keyword.get_list_to_gcs(since,keyword,region,today,page)
            
            if j == break_keyword_no:      
                break_all = True          
                break
            else:
                j += 1 
                
    if break_all:
        break
        
if j==1:
    print('done all scrape and gcs upload')
else:
    print(f'{k+j-1} scrape were done, but {len(keywords * max_page) - k-j+1} are remained.')