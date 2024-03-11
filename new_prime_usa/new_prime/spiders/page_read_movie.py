import hashlib
import html
import json
import os
import re

import colorama
import pymysql
import requests
import scrapy
from parsel import Selector
from scrapy.cmdline import execute
from new_prime import db_config as db
from new_prime.items import OTTMoviedataItem


class DataSpider(scrapy.Spider):
    name = "page_read_movie"
    # allowed_domains = ["www.amazon.com"]
    # start_urls = ["https://www.google.com/"]
    start_urls = ["https://books.toscrape.com/"]
    # allowed_domains = []
    page_save = r"F:/Ankit_Live/OTT_PAGE_SAVE/amazon_movies_prime/US"

    def __init__(self, start, end, name=None, **kwargs):
        super().__init__(name, **kwargs)
        # DATABASE CONNECTION
        self.con = pymysql.connect(host=db.db_host, user=db.db_user, password=db.db_password, db=db.db_name)
        self.cursor = self.con.cursor()

        self.start = start
        self.end = end



    def start_requests(self):
        query = f"select id,platform_url FROM {db.movies_link_us} where id between {self.start} and {self.end} and status ='Page Save'"
        # query = f"select id,url FROM {db.movies_link_us} where id between {self.start} and {self.end} and url ='https://www.amazon.co.uk/gp/video/detail/amzn1.dv.gti.0e6c488a-2214-47a5-9bde-9a7d4b1dbe4d'"
        #
        self.cursor.execute(query)
        query_results = self.cursor.fetchall()
        self.logger.info(f"\n\n\nTotal Results ...{len(query_results)}\n\n\n", )

        for query_result in query_results:
            hash_id = int(hashlib.md5(bytes(query_result[1], 'utf-8')).hexdigest(), 16) % (10 ** 8)
            # hash_id = query_result[1].split('/')[-1]
            meta = {
                "Id": query_result[0],
                "url": query_result[1].replace('app.',''),
                "table_name": db.movie_data_us,
                "data_fetch_table_name": db.movies_link_us,
            }

            file_name = f'{self.page_save}/{hash_id}.html'
            # file_name = "Y:/Page_save/amzonprime/B07M5B9VM8.html"
            if os.path.exists(file_name):

                yield scrapy.FormRequest(url=f'file:///{file_name}', callback=self.parse, meta=meta, dont_filter=True)


    def parse(self, response, **kwargs):
            url1 = response.meta.get('url')
            Id = response.meta.get('Id')



            id = Id
            if 'Continue shopping' not in response.text:
                hash_id = int(hashlib.md5(bytes(url1, 'utf-8')).hexdigest(), 16) % (10 ** 8)


                unavlaible = response.xpath('//div[@class="dv-node-dp-action-box"]//*[contains(text(),"currently unavailable")]')
                if unavlaible:
                    update_query = f"""UPDATE {db.movies_link_us} SET status='currently unavailable' WHERE id={id}"""
                    self.cursor.execute(update_query)
                    self.con.commit()
                    print('its a currently unavailable',id)
                else:
                    try:
                        data_dict1 = json.loads(response.xpath('//script[@type="text/template"]//text()').getall()[-1])
                    except:
                        data_dict1 = ''
                        print('PAGE IS NOT GOOD',id)
                        # continue

                    data_dict =data_dict1.get('props').get('body')[0].get('props').get('atf').get('state')

                    movies = OTTMoviedataItem()
                    page_title_id = data_dict['pageTitleId']
                    # props.body[0].props.atf.state.detail.headerDetail.B08M3955ZZ.entityType

                    # entityType = html.unescape(data_dict['detail']['headerDetail'][f'{page_title_id}']['headerDetail']['entityType'])
                    entityType = html.unescape(data_dict['detail']['headerDetail'][f'{page_title_id}']['entityType'])
                    if entityType.lower() == 'movie':
                        name = html.unescape(data_dict['detail']['headerDetail'][f'{page_title_id}']['title'])
                        description = html.unescape(data_dict['detail']['headerDetail'][f'{page_title_id}']['synopsis'])
                        timing = data_dict['detail']['headerDetail'][f'{page_title_id}']['runtime']
                        try:
                            release_year = data_dict['detail']['headerDetail'][f'{page_title_id}']['releaseYear']
                        except:
                            release_year = ''
                        # category = "Movies>" + ">".join([data['text'] for data in data_dict['detail']['headerDetail'][f'{page_title_id}']['genres'][:2]])
                        category = "Movies>" + ">".join([data for data in response.xpath('//div[contains(@class,"-genres")]//span[@data-testid="genre-texts"]//a//text()').getall()])

                        images = data_dict['detail']['headerDetail'][f'{page_title_id}']['images'].get('heroshot')
                        try:
                            Rating = data_dict['detail']['headerDetail'][f'{page_title_id}']['amazonRating']['countFormatted']
                        except Exception as e:
                            try:
                                Rating = data_dict['reviews'][f'{page_title_id}']['reviewsAnalysisModel']['reviewRatingInfo']['totalReviewCount']
                            except:

                                Rating = ''

                        starring = json.dumps([name['name'] for name in data_dict['detail']['headerDetail'][f'{page_title_id}']['contributors']['starringActors']])
                        try:
                            dir_list = []
                            director_list = data_dict['detail']['headerDetail'][f'{page_title_id}']['contributors']['directors']
                            for dir in director_list:
                                dir_name = dir.get('name')
                                dir_list.append(dir_name)
                            if dir_list:
                                director = ', '.join(dir_list)
                            else:
                                director = ''
                        except Exception as e:
                            director = ''
                        country = 'CA'
                        # movies = dict()
                        # movies['url'] = url1
                        movies['url'] = data_dict.get('bottomBar').get('fullDetailUrl')

                        movies['category'] = category
                        movies['name'] = name
                        movies['Image_URL'] = images
                        movies['Video_URL'] = ''
                        movies['Trailer_URL'] = ''
                        try:
                            movies['genre'] = [data['text'] for data in data_dict['detail']['headerDetail'][f'{page_title_id}']['genres']][0]
                        except Exception as e:
                            movies['genre'] = ''
                        movies['Description'] = description
                        movies['Rating'] = Rating

                        # movies['rating_value'] = data_dict['detail']['headerDetail'][f'{page_title_id}']['ratingBadge']['displayText'].strip()
                        movies['rating_value'] = data_dict['metadata'][f'{page_title_id}']['maturityRating']['displayText'].strip()
                        try:
                            # "props.body[0].props.btf.state.reviews"
                            # "props.body[0].props.btf.state.reviews.B014LNAYDO.reviewsAnalysisModel.reviewRatingInfo.totalReviewCount"
                            # props.body[0].props.btf.state.reviews
                            for_revies_state = data_dict1.get('props').get('body')[0].get('props').get('btf').get('state')
                            movies['review'] = for_revies_state['reviews'][f'{page_title_id}']['reviewsAnalysisModel']['reviewRatingInfo'].get('averageRatingLabel').split('out')[0].strip()
                        except:
                            movies['review'] = ''
                        movies['`Audio languages`'] = ','.join(data_dict['detail']['headerDetail'][f'{page_title_id}'].get('audioTracks'))

                        try:
                            # "props.body[0].props.btf.state.metadata.B09NVV3JXJ.contentDescriptors"
                            for_Content_advisory = data_dict1.get('props').get('body')[0].get('props').get('btf').get('state')
                            movies['`Content advisory`'] = ','.join(for_Content_advisory['metadata'][f'{page_title_id}'].get('contentDescriptors'))
                        except:
                            movies['`Content advisory`'] = ''
                        movies['Subtitles'] = ','.join([ii.get('text') for ii in data_dict['detail']['headerDetail'][f'{page_title_id}'].get('enhancedSubtitles')])
                        movies['Producers'] = ','.join([ii.get('name') for ii in data_dict['detail']['headerDetail'][f'{page_title_id}'].get('contributors').get('producers')])
                        movies['Studio'] = ','.join([ii for ii in data_dict['detail']['headerDetail'][f'{page_title_id}'].get('studios')])
                        if response.xpath('//div[@id="dv-action-box"]//button[@name="more-purchase-options"]'):
                            try:
                                buy_rent1 = json.loads(response.xpath('//script[@type="text/template"]//text()').getall()[-2])
                                buy_rent = buy_rent1.get('props').get('state')
                            except:
                                buy_rent1 = json.loads(response.xpath('//script[@type="text/template"]//text()').getall()[-1])
                                # props.body[0].props.atf.state
                                buy_rent = buy_rent1.get('props').get('body')[0].get('props').get('atf').get('state')
                            # buy_rent = json.loads(response.xpath('//script[@type="text/template"]//text()').getall()[-2])
                            sub_list = []

                            movies['Rent'] = ''
                            movies['Buy'] = ''
                            # if len(buy_rent.get('props').get('state').get("action").get("atf").get(f'{page_title_id}').get("acquisitionActions").get("moreWaysToWatch").get('children')) >1:
                            for by in buy_rent.get("action").get("atf").get(f'{page_title_id}').get("acquisitionActions").get("moreWaysToWatch").get('children'):
                                rent_list = []
                                buy_list = []
                                for jj in by.get('children'):
                                    if jj.get('purchaseData'):
                                        purchaseData = jj.get('purchaseData')
                                        purchase = jj.get('purchaseData').get('text')
                                        offerType = jj.get('purchaseData').get('offerType')
                                        if 'rent' in offerType.lower():
                                            rent_list.append(purchase.split('ovie')[1].strip())
                                            # movies['Rent'] = purchase
                                        else:
                                            try:
                                                buy_list.append(purchase.split('ovie')[1].strip())
                                            except:
                                                con = pymysql.connect(host=db.db_host, user=db.db_user,password=db.db_password, db=db.db_name)
                                                cursor = self.con.cursor()
                                                update_query = f"""UPDATE link SET Movie_season='Series' WHERE id={id}"""
                                                cursor.execute(update_query)
                                                con.commit()
                                                print('IT IS SERIES****')
                                    if jj.get('message'):
                                        sub_list.append(jj.get('message'))

                                if rent_list:
                                    movies['Rent'] = ','.join(rent_list)

                                if buy_list:
                                    movies['Buy'] = ','.join(buy_list)

                            if movies['Rent'] and movies['Buy']:
                                movies['`Option`'] = 'Stream/Rent/Buy'
                            elif movies['Rent']:
                                movies['`Option`'] = 'Rent'
                            elif movies['Buy']:
                                movies['`Option`'] = 'Buy'
                            else:
                                movies['`Option`'] = 'Stream'

                            # sub_list = []
                            # for iii in response.xpath('//div[@data-testid="focus-trap"]//span[contains(@data-testid,"entit")]'):
                            #     sub = iii.xpath('.//following-sibling::*//text()').get('').strip()
                            #     sub_list.append(sub)
                            if sub_list:
                                movies['subcription'] = " | ".join(sub_list)
                            else:
                                subcription = response.xpath('//div[@class="dv-node-dp-action-box"]//span[contains(@data-testid,"entit")]//following-sibling::*//text()').get('').strip()
                                if not subcription:
                                    subcription = response.xpath('//div[@class="dv-node-dp-action-box"]//*[@class="_1sIAAm"]//following-sibling::*//text()').get('').strip()
                                movies['subcription'] = subcription
                        else:
                            # movies['Rent'] = ''.join([ii for ii in response.xpath('//div[@id="dv-action-box"]//span[contains(text(),"Rent")]//text()').getall() if '$' in ii]).strip()
                            # movies['Buy'] = ''.join([ii for ii in response.xpath('//div[@id="dv-action-box"]//span[contains(text(),"Buy")]//text()').getall() if '$' in ii]).strip()
                            if response.xpath('//div[@id="dv-action-box"]//span[contains(text(),"Rent")]'):
                                movies['Rent'] = re.sub(' +',' ',' '.join(response.xpath('//div[@id="dv-action-box"]//span[contains(text(),"Rent")]//text()').getall()).split('Rent')[1].strip())
                            else:
                                movies['Rent'] = ''
                            if response.xpath('//div[@id="dv-action-box"]//span[contains(text(),"Buy")]'):
                                movies['Buy'] = re.sub(' +',' ',' '.join(response.xpath('//div[@id="dv-action-box"]//span[contains(text(),"Buy")]//text()').getall()).split('Buy')[1].strip())
                            else:
                                movies['Buy'] = ''

                            if movies['Rent'] and movies['Buy']:
                                movies['`Option`'] = 'Stream/Rent/Buy'
                            elif movies['Rent']:
                                movies['`Option`'] = 'Rent'
                            elif movies['Buy']:
                                movies['`Option`'] = 'Buy'
                            else:
                                movies['`Option`'] = 'Stream'

                            subcription = response.xpath('//div[@class="dv-node-dp-action-box"]//span[contains(@data-testid,"entit")]//following-sibling::*//text()').get('').strip()
                            if not subcription:
                                subcription = response.xpath('//div[@class="dv-node-dp-action-box"]//*[@class="_1sIAAm"]//following-sibling::*//text()').get('').strip()
                            movies['subcription'] = subcription
                        movies['Release_date'] = release_year
                        movies['Hour_Duration'] = timing
                        movies['Starring'] = starring
                        movies['Director'] = director
                        movies['id'] = id
                        movies['table_name'] = response.meta['table_name']
                        movies['data_fetch_table_name'] = response.meta['data_fetch_table_name']
                        yield movies
                        # coloumns = ",".join(movies.keys())
                        # print(movies)

                        # print(self.insert_table(table_name=db.movide_data, columns=coloumns, values=tuple(movies.values())))
                        # print(self.update_table(table_name=db.movies_link_us, url_id=id))

                    else:
                        update_query = f"""UPDATE {db.movies_link_us} SET status='{entityType}' WHERE id={id}"""
                        self.cursor.execute(update_query)
                        self.con.commit()
                        print('its not a movie', entityType,id)
            #






    # def update_table(self, table_name, url_id):
    #     with self.con.cursor() as cursor:
    #         update_query = f"""UPDATE {table_name} SET status='Done' WHERE id={url_id}"""
    #         cursor.execute(update_query)
    #     self.con.commit()
    #     return colorama.Fore.BLUE + f"Status Updated at {url_id}" + colorama.Style.RESET_ALL
    #
    # def insert_table(self, table_name, columns, values):
    #     try:
    #         with self.con.cursor() as cursor:
    #             insert_query = f"""INSERT IGNORE INTO {table_name} ({columns}) VALUES {values}"""
    #             cursor.execute(insert_query)
    #         self.con.commit()
    #     except Exception as e:
    #         print(e)
    #     return colorama.Fore.GREEN + f"Movie Data Inserted" + colorama.Style.RESET_ALL



if __name__ == '__main__':
    execute("scrapy crawl page_read_movie -a start=1 -a end=10270000".split())
