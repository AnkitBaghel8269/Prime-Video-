import hashlib
import html
import json
import os
import colorama
import pymysql
import requests
import scrapy
from parsel import Selector
from scrapy.cmdline import execute
from new_prime import db_config as db

from urllib.parse import urlparse, parse_qs, urlencode
class DataSpider(scrapy.Spider):
    name = "page_save_movies"
    # allowed_domains = ["www.amazon.com"]
    # start_urls = ["https://www.google.com/"]
    start_urls = ["https://books.toscrape.com/"]
    # allowed_domains = []
    page_save = r"F:/Ankit_Live/OTT_PAGE_SAVE/amazon_movies_prime/US/"

    def __init__(self, start, end, name=None, **kwargs):
        super().__init__(name, **kwargs)
        # DATABASE CONNECTION
        self.con = pymysql.connect(host=db.db_host, user=db.db_user, password=db.db_password, db=db.db_name)
        self.cursor = self.con.cursor()

        self.start = start
        self.end = end

        if not os.path.exists(self.page_save):
            os.makedirs(self.page_save)

    def start_requests(self):
        query = f"select id,platform_url FROM {db.movies_link_us} where id between {self.start} and {self.end} and status ='new'"
        # query = f"select id,platform_url FROM {db.movies_link_us} where id between {self.start} and {self.end} and platform_url ='https://www.amazon.com/gp/video/detail/B075RSTP1Z/ref=atv_dp_share_cu_r'"
        #
        self.cursor.execute(query)
        query_results = self.cursor.fetchall()
        self.logger.info(f"\n\n\nTotal Results ...{len(query_results)}\n\n\n", )

        for query_result in query_results:
            hash_id = int(hashlib.md5(bytes(query_result[1], 'utf-8')).hexdigest(), 16) % (10 ** 8)

            meta = {
                "Id": query_result[0],
                "url": query_result[1],
                # "menu": query_result[2]
            }
            file_name = f'{self.page_save}/{hash_id}.html'
            if os.path.exists(file_name):
                # os.remove(file_name)
                print('page already saved')
                continue
            else:


                cookies = {
                    'av-timezone': 'Asia/Calcutta',
                    'csm-sid': '889-4740813-3068074',
                    'session-id': '147-0293896-6906173',
                    'session-id-time': '2082787201l',
                    'i18n-prefs': 'USD',
                    'ubid-main': '132-6757241-6000835',
                    'x-amz-captcha-1': '1709542483300380',
                    'x-amz-captcha-2': '975j/70jikRkkTJGxNYViQ==',
                    'session-token': 'iqZmxhwHeAS7fdbXIzUGfVkif3AHoQkr9kYc1sRw/V5XyVcJ/UlsjhAjcHheeZ1qivqvqpMleTraNMXkqWlS2xPwJ7p72+e3IPbcVDr4e3OhjfOW8BQWlizZYoOinucsv2EFNBaOV39HNqF7qJ7KasvpOqyYA+uOsk/qa50purYHgdKdGAnCroafvy9sWxHOFta1V/UQEMfMW6g7J+hgNtcNvsbOUluBb6zbskShx0vYqJlYuGi0nnhKpGc4kDUngoGTZ4VEpBjQ4GIMKny6gTFMpmEDWsL9pv48WcWR5hFDrMfOfzmbZYxp2+aISs2oHy5+8VQLMgKBP4CQ9WelpDsZjbmjGfN4',
                    'csm-hit': 'tb:s-A6XZS05F3913DF7QJJG4|1709535297578&t:1709535298046&adb:adblk_no',
                    'JSESSIONID': 'B115100DFA2F6066755E535EB73A0C4B',
                }

                headers = {
                    'authority': 'www.amazon.com',
                    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                    'accept-language': 'en-US,en;q=0.9',
                    'cache-control': 'max-age=0',
                    # 'cookie': 'av-timezone=Asia/Calcutta; csm-sid=889-4740813-3068074; session-id=147-0293896-6906173; session-id-time=2082787201l; i18n-prefs=USD; ubid-main=132-6757241-6000835; x-amz-captcha-1=1709542483300380; x-amz-captcha-2=975j/70jikRkkTJGxNYViQ==; session-token=iqZmxhwHeAS7fdbXIzUGfVkif3AHoQkr9kYc1sRw/V5XyVcJ/UlsjhAjcHheeZ1qivqvqpMleTraNMXkqWlS2xPwJ7p72+e3IPbcVDr4e3OhjfOW8BQWlizZYoOinucsv2EFNBaOV39HNqF7qJ7KasvpOqyYA+uOsk/qa50purYHgdKdGAnCroafvy9sWxHOFta1V/UQEMfMW6g7J+hgNtcNvsbOUluBb6zbskShx0vYqJlYuGi0nnhKpGc4kDUngoGTZ4VEpBjQ4GIMKny6gTFMpmEDWsL9pv48WcWR5hFDrMfOfzmbZYxp2+aISs2oHy5+8VQLMgKBP4CQ9WelpDsZjbmjGfN4; csm-hit=tb:s-A6XZS05F3913DF7QJJG4|1709535297578&t:1709535298046&adb:adblk_no; JSESSIONID=B115100DFA2F6066755E535EB73A0C4B',
                    'device-memory': '8',
                    'downlink': '1.5',
                    'dpr': '1',
                    'ect': '3g',
                    'rtt': '350',
                    'sec-ch-device-memory': '8',
                    'sec-ch-dpr': '1',
                    'sec-ch-ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"Windows"',
                    'sec-ch-ua-platform-version': '"10.0.0"',
                    'sec-ch-viewport-width': '826',
                    'sec-fetch-dest': 'document',
                    'sec-fetch-mode': 'navigate',
                    'sec-fetch-site': 'none',
                    'sec-fetch-user': '?1',
                    'upgrade-insecure-requests': '1',
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
                    'viewport-width': '826',
                }

                yield scrapy.Request(
                    # url=redirected_url,
                    url=f"{query_result[1]}",
                    headers=headers,
                    cookies=cookies,
                    dont_filter=True,
                    meta=meta,
                )

    def parse(self, response, **kwargs):
        url11 = response.meta.get('url')
        Id = response.meta.get('Id')

        try:
            if 'Continue shopping' not in response.text and 'edirecting you to the Prime Video website' not in response.text:
                if response.status == 200:
                    id = Id
                    hash_id = int(hashlib.md5(bytes(url11, 'utf-8')).hexdigest(), 16) % (10 ** 8)
                    with open(f'{self.page_save}{hash_id}.html', 'w',encoding='utf=8') as file:
                        file.write(response.text)
                    update_query = f"""UPDATE {db.movies_link_us} SET status='Page Save',hash_key = '{hash_id}'  WHERE id={Id}"""
                    self.cursor.execute(update_query)
                    self.con.commit()
                    print('Page Save Completed',hash_id)

        except Exception as e:
            print('page save error', e)
            pass



if __name__ == '__main__':
    execute("scrapy crawl page_save_movies -a start=1 -a end=295880".split())
