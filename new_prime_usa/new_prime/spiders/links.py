import pymysql
import scrapy
import datetime
import hashlib
from scrapy.cmdline import execute
import scrapy
from new_prime import db_config as db
from new_prime.items import OTTPlatformsLinksItem


class LinkSpider(scrapy.Spider):
    name = "link"
    # allowed_domains = ["www.amazon.com"]
    # start_urls = ["https://www.amazon.com"]
    allowed_domains = []
    handle_httpstatus_list = [503, 502, 501, 500]

    def __init__(self):
        self.con = pymysql.connect(host=db.db_host, user=db.db_user, password=db.db_password, db=db.db_name)
        self.cursor = self.con.cursor()

    def start_requests(self):
        url_list = ['https://www.amazon.com/s?k=Action+%26+Adventure&i=instant-video&crid=DAMBI4OB65E1&sprefix=musical%2Cinstant-video%2C394&ref=nb_sb_noss_1','https://www.amazon.com/s?k=Animation&i=instant-video&crid=Z7SHHH1A6Y6I&sprefix=action+%26+adventure%2Cinstant-video%2C397&ref=nb_sb_noss_1','https://www.amazon.com/s?k=Anime&i=instant-video&crid=2GGVWAX2AJX12&sprefix=animation%2Cinstant-video%2C390&ref=nb_sb_noss_1','https://www.amazon.com/s?k=Arts%2C+Culture+%26+Entertainment&i=instant-video&crid=2A9HWSTKWWUFE&sprefix=arts%2C+culture+%26+entertainment%2Cinstant-video%2C392&ref=nb_sb_noss_2','https://www.amazon.com/s?k=Faith+%26+Spirituality&i=instant-video&crid=2XLVOKZ22IUU7&sprefix=faith+%26+spirituality%2Cinstant-video%2C402&ref=nb_sb_noss_1','https://www.amazon.com/s?k=Fitness&i=instant-video&crid=AVIC4I0K9S1C&sprefix=faith+%26+spirituality%2Cinstant-video%2C397&ref=nb_sb_noss_1','https://www.amazon.com/s?k=LGBTQ%2B&i=instant-video&ref=nb_sb_noss','https://www.amazon.com/s?k=Music+Videos+%26+Concerts&i=instant-video&crid=1WGUFCSPAF5T5&sprefix=lgbtq%2B%2Cinstant-video%2C394&ref=nb_sb_noss_2','https://www.amazon.com/s?k=Talk+Shows+%26+Variety&i=instant-video&crid=1H3GJXO84V7ZN&sprefix=music+videos+%26+concerts%2Cinstant-video%2C406&ref=nb_sb_noss_2','https://www.amazon.com/s?k=Unscripted&i=instant-video&crid=1D5UI5MX2FD5W&sprefix=talk+shows+%26+variety%2Cinstant-video%2C377&ref=nb_sb_noss_2','https://www.amazon.com/s?k=Young+Adult&i=instant-video&crid=14BI4NPE60EFP&sprefix=unscripted%2Cinstant-video%2C396&ref=nb_sb_noss_1','https://www.amazon.com/s?k=Westerns&i=instant-video&crid=2W1G12VD1UOUY&sprefix=young+adult%2Cinstant-video%2C379&ref=nb_sb_noss','https://www.amazon.com/s?k=Military+%26+War&i=instant-video&crid=1C1817QF027VI&sprefix=westerns%2Cinstant-video%2C396&ref=nb_sb_noss_1','https://www.amazon.com/s?k=Science+Fiction&i=instant-video&crid=3AN7U8QBQKUWW&sprefix=military+%26+war%2Cinstant-video%2C409&ref=nb_sb_noss_1']
        for url in url_list:
            # url = "https://www.amazon.com/s?k=Web+Series&i=instant-video&crid=A5HN3KPZU8J5&sprefix=web+series%2Cinstant-video%2C323&ref=nb_sb_noss_1"
            # url = "https://www.amazon.com/s?k=series"
            payload = {}
            headers = {
                'authority': 'www.amazon.com',
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'accept-language': 'en-US,en;q=0.9',
                'cache-control': 'max-age=0',
                'cookie': 'session-id=130-6822226-5031258; i18n-prefs=USD; ubid-main=133-4608375-9306239; av-timezone=Asia/Calcutta; lc-main=en_US; session-id-time=2082787201l; x-amz-captcha-1=1702626315907996; x-amz-captcha-2=+EMLcguobuo29DsngKLyww==; AMCV_7742037254C95E840A4C98A6%40AdobeOrg=1585540135%7CMCIDTS%7C19721%7CMCMID%7C78254545272030972930637115177183630150%7CMCAAMLH-1704450860%7C12%7CMCAAMB-1704450860%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1703853260s%7CNONE%7CMCAID%7CNONE%7CvVersion%7C4.4.0; regStatus=pre-register; aws-target-data=%7B%22support%22%3A%221%22%7D; aws-target-visitor-id=1703846060588-424799.41_0; skin=noskin; session-token=tlUgCOYd/KOTYYxIJsOj6J9ZuryhJjpK5VvvSrVCl2aGFHOJVbv93+brsYIf2OxVsXYolgmVK9P06nWG526Hc8GXEN9VEqHM44mT3r9qYP4yDJ+U23FEjXLNHzgSj/P0zaYgWEdaMvVIQLt6ggrEG5c5u9UuwWSc20Hvu/iOk2j7LU504zGk1hgTrkKKsSbSsH7NpBYcCy4nt2ukarFAIVJ49Y9FKS0HUYTq+l7rpzKswKcZL9istPyyLOfnn5hzWVoaraBoMclavdbhYgJB4iYBt6BTG1ejuDjK6ZShYQ2CLtjVAwok8OwHfxBmOShyYNgNdIGdfBohyGXTsZmOLhTFXnWZgzd6; csm-hit=tb:VBZP8ZCCGK99X0ZH6NA7+s-9QDJES0C12EN5HWM0WAW|1704452792264&t:1704452792264&adb:adblk_no; JSESSIONID=86290450EC7BE93F01623C2F599500F8; session-token=VE//rRAxWA4kUpLfLddsnESXa1cQIVCtxv/apWiH0vbiC+zJsEQh5Mw/OnaEZaM8KpBMpMPP4epqmTzeH4JvwXBhw6rgPLMsMv8GzNZjleMlCeKICVl3I7HjdksEIMZNFwbRM22yF5UuRqv/sp6ERIjGFguICrmPQwcHmUSVtdpKpE/4Ajd+M4+IzQXFWO3r7IgMqTfj4zKt0Y/LzuVW94tmSNbwwviWmapBIndw58OTCTJwY5Y1cfLeG4M1gWosjra6PeXsNO4F9ZoJ6lwOoMFuPIvuYbWb4AdS/VlT3gcY1yZk4lrkqW0SFq/DfQUfjmHb58QL/nk7LvZ4Lj1BiQKbWdF3MWKh',
                'device-memory': '8',
                'downlink': '2.2',
                'dpr': '1',
                'ect': '4g',
                'referer': 'https://www.amazon.com/Amazon-Video/b?ie=UTF8&node=2858778011',
                'rtt': '200',
                'sec-ch-device-memory': '8',
                'sec-ch-dpr': '1',
                'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
                'sec-ch-ua-platform-version': '"10.0.0"',
                'sec-ch-viewport-width': '1920',
                'sec-fetch-dest': 'document',
                'sec-fetch-mode': 'navigate',
                'sec-fetch-site': 'same-origin',
                'sec-fetch-user': '?1',
                'upgrade-insecure-requests': '1',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'viewport-width': '1920'
            }

            yield scrapy.Request(
                url=url,
                headers=headers,
            meta = {'paginaiton': 1}
            )

    def parse(self, response, **kwargs):
        paginaiton = response.meta.get('paginaiton')
        item = OTTPlatformsLinksItem()
        for ii in response.xpath('//div[contains(@class,"puis-list-col-right")]//a[contains(@class,"a-link-normal s-underline-text s-underline-link-text s-link-style a-text-normal")]'):
            url  =  ii.xpath('.//@href').get('')
            # print(url)
            link = 'https://www.amazon.com' + url.split('/ref')[0]
            item['hash_id'] = int(hashlib.md5(bytes(link, "utf8")).hexdigest(), 16) % (10 ** 8)
            item['url'] = link
            item['type'] = "Series"
            yield item

        if response.xpath('//span[@class="s-pagination-strip"]'):
            next_url =  response.xpath('//span[@class="s-pagination-strip"]//span[contains(@aria-label,"Current page")]//following-sibling::a[1]//@href').get('')
            final_next = 'https://www.amazon.com' + next_url
            print(final_next)
            headers = {
                'authority': 'www.amazon.com',
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'accept-language': 'en-US,en;q=0.9',
                'cache-control': 'max-age=0',
                'cookie': 'session-id=130-6822226-5031258; i18n-prefs=USD; ubid-main=133-4608375-9306239; av-timezone=Asia/Calcutta; lc-main=en_US; session-id-time=2082787201l; x-amz-captcha-1=1702626315907996; x-amz-captcha-2=+EMLcguobuo29DsngKLyww==; AMCV_7742037254C95E840A4C98A6%40AdobeOrg=1585540135%7CMCIDTS%7C19721%7CMCMID%7C78254545272030972930637115177183630150%7CMCAAMLH-1704450860%7C12%7CMCAAMB-1704450860%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1703853260s%7CNONE%7CMCAID%7CNONE%7CvVersion%7C4.4.0; regStatus=pre-register; aws-target-data=%7B%22support%22%3A%221%22%7D; aws-target-visitor-id=1703846060588-424799.41_0; skin=noskin; session-token=kDL1pKXt7XzikamHGuAxJRniwz3REtUj50RmP5i1sj167gErk5s1SlkA8NmXv6M4O0fdtX3cXjlzVobnBFPv6V6U1C4e58w4/65T/pWrFTYCmyE8nIZ3eMa9iq8PweuBKmPY83GP8Ac66YpTvqHHWEcAYaio07eXNdKZpX+eg6O0CjS4hpOjG9rwwyudT4stIWiqCZ6BvwsbjdtjFdsj+45/cBj/7ev9n+tmmdhyrSJa/c3CtxjTaYF0vh4wkjMKOBmSzlyGho2MGmVh1WSI+v7yxpBjy5vke8F7s+ZnjlBmo5j+sBSOUAf0sZXSTcuY5rAHLRa2wDPEbBTi1NWHu+xsVkUQOWF1; csm-hit=tb:s-XW8YNT0ZMTHWQXCK666C|1704454314151&t:1704454314653&adb:adblk_no; JSESSIONID=C4CB0C8D652128E4FB5E6B8708ECB649; session-token=VE//rRAxWA4kUpLfLddsnESXa1cQIVCtxv/apWiH0vbiC+zJsEQh5Mw/OnaEZaM8KpBMpMPP4epqmTzeH4JvwXBhw6rgPLMsMv8GzNZjleMlCeKICVl3I7HjdksEIMZNFwbRM22yF5UuRqv/sp6ERIjGFguICrmPQwcHmUSVtdpKpE/4Ajd+M4+IzQXFWO3r7IgMqTfj4zKt0Y/LzuVW94tmSNbwwviWmapBIndw58OTCTJwY5Y1cfLeG4M1gWosjra6PeXsNO4F9ZoJ6lwOoMFuPIvuYbWb4AdS/VlT3gcY1yZk4lrkqW0SFq/DfQUfjmHb58QL/nk7LvZ4Lj1BiQKbWdF3MWKh',
                # 'device-memory': '8',
                # 'downlink': '10',
                # 'dpr': '1',
                # 'ect': '4g',
                # 'rtt': '200',
                # 'sec-ch-device-memory': '8',
                # 'sec-ch-dpr': '1',
                # 'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
                # 'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
                'sec-ch-ua-platform-version': '"10.0.0"',
                # 'sec-ch-viewport-width': '1920',
                # 'sec-fetch-dest': 'document',
                # 'sec-fetch-mode': 'navigate',
                # 'sec-fetch-site': 'same-origin',
                # 'sec-fetch-user': '?1',
                # 'upgrade-insecure-requests': '1',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'viewport-width': '1920'
            }
            print(paginaiton)
            paginaiton += 1
            yield scrapy.Request(
                url=final_next,
                headers=headers,
                callback=self.parse,
                meta = {'paginaiton':paginaiton}
            )


if __name__ == '__main__':

    execute('scrapy crawl link'.split())