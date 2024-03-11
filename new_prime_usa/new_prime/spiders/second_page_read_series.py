import hashlib,html,json,os,colorama,pymysql,requests,scrapy

from parsel import Selector
from scrapy.cmdline import execute
from new_prime import db_config as db
# from new_prime.items import OTTSereisdataItem
from new_prime.for_headers import *
class DataSpider(scrapy.Spider):
    name = "second_page_read_series"
    # allowed_domains = ["www.amazon.com"]
    # start_urls = ["https://www.google.com/"]
    start_urls = ["https://books.toscrape.com/"]
    # allowed_domains = []
    page_save = r"F:/Ankit_Live/OTT_PAGE_SAVE/amazon_series/US"

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
        query = f"select id,platform_url FROM {db.series_link_us} where id between {self.start} and {self.end} and status ='Done'"
        # query = f"select id,platform_url FROM {db.series_link_us} where id between {self.start} and {self.end} and platform_url = 'https://www.amazon.com/gp/video/detail/amzn1.dv.gti.3cbc1474-3584-82fd-c957-b2adcd32e2a6'"
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
                # continue
                yield scrapy.FormRequest(url=f'file:///{file_name}', callback=self.parse, meta=meta, dont_filter=True)
            else:
                yield scrapy.Request(
                    url=query_result[1],
                    headers=headers,
                    dont_filter=True,
                    meta=meta,
                )

    def parse(self, response, **kwargs):
        try:
            link_id = response.meta.get('Id')
            url = response.meta.get('url')


            data_dict = json.loads(response.xpath('//script[@type="text/template"]//text()').getall()[-1])
            try:
                page_title_id = data_dict['props']['state']['pageTitleId']
                final_data = data_dict['props']['state']
            except:
                page_title_id = data_dict['props']['body'][0]['props']['btf']['state']['pageTitleId']
                final_data = data_dict['props']['body'][0]['props']['btf']['state']
            # page_title_id = data_dict['props']['state']['pageTitleId']
            entityType = html.unescape(final_data['detail']['btfMoreDetails'][f'{page_title_id}']['entityType'])
            if entityType.lower() == 'tv show':
                hash_id = int(hashlib.md5(bytes(url, 'utf-8')).hexdigest(), 16) % (10 ** 8)
                with open(f'{self.page_save}/{hash_id}.html', 'w',encoding='utf=8') as file:
                    file.write(response.text)
                print(f'main PAGE SAVE DONE', url)
                update_id = link_id
                season = final_data['detail']['btfMoreDetails'][f'{page_title_id}']['seasonNumber']
                name = html.unescape(final_data['detail']['btfMoreDetails'][f'{page_title_id}']['parentTitle'])
                if response.xpath('//div[@class="dv-node-dp-seasons"]//li'):
                    for ii in response.xpath('//div[@class="dv-node-dp-seasons"]//li//a'):
                        new_s = ii.xpath('.//@href').get('')
                        seson = ii.xpath('.//span//text()').get('').replace(' ', '')
                        if new_s.startswith('http'):
                            new_url = new_s
                        else:
                            new_url = 'https://www.amazon.com' + new_s

                        hash_id = int(hashlib.md5(bytes(new_url, 'utf-8')).hexdigest(), 16) % (10 ** 8)

                        if not os.path.exists(f'{self.page_save}/{hash_id}.html'):
                            Response = requests.get(url=new_url,headers=headers_season)
                            if 'Continue shopping' not in Response.text:
                                if Response.status_code == 200:
                                    filen = 'Series'
                                    hash_id = int(hashlib.md5(bytes(new_url, 'utf-8')).hexdigest(), 16) % (10 ** 8)
                                    with open(f'{self.page_save}/{hash_id}.html','w',encoding='utf=8') as file:
                                        file.write(Response.text)
                                    print(f'{seson} PAGE SAVE DONE', new_url)
                                    response = Selector(text=Response.text)
                            else:
                                continue
                        else:
                            hash_id = int(hashlib.md5(bytes(new_url, 'utf-8')).hexdigest(), 16) % (10 ** 8)
                            file_data = open(f'{self.page_save}/{hash_id}.html', 'r',encoding='utf-8')
                            response = Selector(text=file_data.read())
                            if response.xpath('//h4[contains(text(),"Enter the characters you see below")]'):
                                file_data.close()
                                os.remove(f'{self.page_save}/{hash_id}.html')
                                print('Remove wrong page save')
                            else:
                                pass
                        update_id = link_id
                        data_dict = json.loads(response.xpath('//script[@type="text/template"]//text()').getall()[-1])
                        try:
                            page_title_id = data_dict['props']['state']['pageTitleId']
                            final_data = data_dict['props']['state']
                        except:
                            page_title_id = data_dict['props']['body'][0]['props']['btf']['state']['pageTitleId']
                            final_data = data_dict['props']['body'][0]['props']['btf']['state']

                        # props.body[0].props.btf.state.pageTitleId
                        season = final_data['detail']['btfMoreDetails'][f'{page_title_id}']['seasonNumber']

                        name = html.unescape(final_data['detail']['btfMoreDetails'][f'{page_title_id}']['parentTitle'])
                        totalCardSize = final_data['collections'][f'{page_title_id}'][0]['totalCardSize']
                        description = html.unescape(final_data['detail']['btfMoreDetails'][f'{page_title_id}']['synopsis'])
                        timing = final_data['detail']['btfMoreDetails'][f'{page_title_id}']['runtime']
                        try:
                            release_year = final_data['detail']['btfMoreDetails'][f'{page_title_id}']['releaseYear']
                        except:
                            release_year = ''
                        category = "Series>" + ">".join([data['text'] for data in final_data['detail']['btfMoreDetails'][f'{page_title_id}']['genres']])
                        try:
                            images = final_data['detail']['btfMoreDetails'][f'{page_title_id}']['images'].get('heroshot')
                        except:
                            images = ''

                        try:
                            Rating = final_data['detail']['btfMoreDetails'][f'{page_title_id}']['amazonRating']['countFormatted']
                        except Exception as e:
                            try:
                                Rating = final_data['reviews'][f'{page_title_id}']['reviewsAnalysisModel']['reviewRatingInfo']['totalReviewCount']
                            except:
                                Rating = ''
                        starring = json.dumps([name['name'] for name in final_data['detail']['btfMoreDetails'][f'{page_title_id}']['contributors']['starringActors']])
                        try:
                            dir_list = []
                            director_list = final_data['detail']['btfMoreDetails'][f'{page_title_id}']['contributors']['directors']
                            for dir in director_list:
                                dir_name = dir.get('name')
                                dir_list.append(dir_name)
                            if dir_list:
                                director = ', '.join(dir_list)
                            else:
                                director = ''
                        except Exception as e:
                            director = ''
                        country = 'US'
                        series = dict()
                        series['hash_id'] = hash_id
                        series['url'] = new_url
                        series['new_url'] = url
                        series['Category'] = category
                        series['name'] = name
                        series['Image_URL'] = images
                        series['Video_URL'] = ''
                        series['Trailer_URL'] = ''
                        try:
                            series['genre'] = [data['text'] for data in final_data['detail']['btfMoreDetails'][f'{page_title_id}']['genres']][0]
                        except Exception as e:
                            series['genre'] = ''
                        series['Description'] = description
                        series['Rating'] = Rating
                        series['rating_value'] = final_data['detail']['btfMoreDetails'][f'{page_title_id}']['ratingBadge']['displayText'].strip()
                        try:
                            series['review'] = final_data['reviews'][f'{page_title_id}']['reviewsAnalysisModel']['reviewRatingInfo'].get('averageRatingLabel').split('out')[0].strip()
                        except:
                            series['review'] = ''
                        try:
                            series['`Content advisory`'] = ','.join(final_data['metadata'][f'{page_title_id}'].get('contentDescriptors'))
                        except:
                            series['`Content advisory`'] = ''
                        series['`Audio languages`'] = ','.join(final_data['detail']['btfMoreDetails'][f'{page_title_id}'].get('audioTracks'))
                        series['Subtitles'] = ','.join([ii.get('text') for ii in final_data['detail']['btfMoreDetails'][f'{page_title_id}'].get('enhancedSubtitles')])
                        series['Producers'] = ','.join([ii.get('name') for ii in final_data['detail']['btfMoreDetails'][f'{page_title_id}'].get('contributors').get('producers')])
                        series['Studio'] = ','.join([ii for ii in final_data['detail']['btfMoreDetails'][f'{page_title_id}'].get('studios')])
                        if response.xpath('//div[@id="dv-action-box"]//button[@name="more-purchase-options"]'):
                            try:
                                buy_rent1 = json.loads(response.xpath('//script[@type="text/template"]//text()').getall()[-2])
                                buy_rent = buy_rent1.get('props').get('state')
                            except:
                                buy_rent1 = json.loads(response.xpath('//script[@type="text/template"]//text()').getall()[-1])
                                buy_rent = buy_rent1.get('props').get('body')[0].get('props').get('atf').get('state')
                            sub_list = []
                            series['Episode_price'] = ''
                            series['Seasion_price'] = ''
                            for by in buy_rent.get("action").get("atf").get(f'{page_title_id}').get("acquisitionActions").get("moreWaysToWatch").get('children'):
                                rent_list = []
                                buy_list = []
                                for jj in by.get('children'):
                                    if jj.get('purchaseData'):
                                        purchase = jj.get('purchaseData').get('text')
                                        offerType = jj.get('purchaseData').get('offerType')
                                        videoQuality = jj.get('purchaseData').get('videoQuality')
                                        if 'isode' in purchase.lower():
                                            rent_list.append(
                                                videoQuality + ' ' + purchase.split(videoQuality)[1].strip())
                                            # movies['Rent'] = purchase
                                        else:
                                            buy_list.append(
                                                videoQuality + ' ' + purchase.split(videoQuality)[1].strip())
                                    if jj.get('message'):
                                        sub_list.append(jj.get('message'))
                                if rent_list:
                                    series['Episode_price'] = ','.join(rent_list)
                                if buy_list:
                                    series['Seasion_price'] = ','.join(buy_list)
                            if series['Episode_price'] and series['Seasion_price']:
                                series['`Option`'] = 'Stream/Rent/Buy'
                            elif series['Episode_price']:
                                series['`Option`'] = 'Rent'
                            elif series['Seasion_price']:
                                series['`Option`'] = 'Buy'
                            else:
                                series['`Option`'] = 'Stream'
                            if sub_list:
                                series['subcription'] = " | ".join(sub_list)
                            else:
                                subcription = response.xpath('//div[@class="dv-node-dp-action-box"]//span[contains(@data-testid,"entit")]//following-sibling::*//text()').get('').strip()
                                if not subcription:
                                    subcription = response.xpath('//div[@class="dv-node-dp-action-box"]//*[@class="_36qUej"]//following-sibling::*//text()').get('').strip()
                                    if len(subcription)<4:
                                        subcription = response.xpath('//div[@class="dv-node-dp-action-box"]//*[@class="_36qUej"]//text()').get('').strip()
                                series['subcription'] = subcription
                        else:
                            series['Episode_price'] = ''.join([ii for ii in response.xpath('//div[@id="dv-action-box"]//span[contains(text(),"Episode")]//text()').getall() if '$' in ii]).strip()
                            series['Seasion_price'] = ''.join([ii for ii in response.xpath('//div[@id="dv-action-box"]//span[contains(text(),"Season")]//text()').getall() if '$' in ii]).strip()
                            if series['Episode_price'] and series['Seasion_price']:
                                series['`Option`'] = 'Stream/Rent/Buy'
                            elif series['Episode_price']:
                                series['`Option`'] = 'Rent'
                            elif series['Seasion_price']:
                                series['`Option`'] = 'Buy'
                            else:
                                series['`Option`'] = 'Stream'
                            subcription = response.xpath('//div[@class="dv-node-dp-action-box"]//span[contains(@data-testid,"entit")]//following-sibling::*//text()').get('').strip()
                            if not subcription:
                                subcription = response.xpath('//div[@class="dv-node-dp-action-box"]//*[class="_36qUej"]//following-sibling::*//text()').get('').strip()
                                if len(subcription) < 4:
                                    subcription = response.xpath('//div[@class="dv-node-dp-action-box"]//*[@class="_36qUej"]//text()').get('').strip()
                            series['subcription'] = subcription
                            series['`Option`'] = 'Stream'
                        series['Release_date'] = release_year
                        series['Hour_Duration'] = timing
                        series['Starring'] = starring
                        series['Director'] = director
                        series['season'] = season
                        if response.xpath('//div[@id="tab-content-episodes"]//a[contains(@aria-label,"Show all")]'):
                            hash_id = int(hashlib.md5(bytes(url, 'utf-8')).hexdigest(), 16) % (10 ** 8)
                            episode_file_name = f'{self.page_save}/{hash_id}_total_episode.html'
                            try:
                                if os.path.exists(episode_file_name):
                                    with open(episode_file_name, 'r', encoding='utf-8') as epi_data:
                                        data = epi_data.read()
                                        data_dict = json.loads(data)
                                else:


                                    params = {
                                        'titleID': f"{page_title_id}",
                                        # 'isElcano': '0',
                                        'sections': 'Btf',
                                        'widgets': '{"btf":["Episodes","Bonus"]}',
                                        'widgetsConfig': '{"episodes":{"startIndex":0,"pageSize":' + str(totalCardSize) + '},"bonus":{"startIndex":0,"pageSize":200}}',
                                    }

                                    response_episode = requests.get('https://www.amazon.com/gp/video/api/getDetailPage',params=params, headers=headers_episode)
                                    with open(f'{self.page_save}/{hash_id}_total_episode.html','w', encoding='utf=8') as file:
                                        file.write(response_episode.text)
                                    data_dict = json.loads(response_episode.text)
                            except:
                                os.remove(episode_file_name)

                                params = {
                                    'titleID': f"{page_title_id}",
                                    # 'isElcano': '0',
                                    'sections': 'Btf',
                                    'widgets': '{"btf":["Episodes","Bonus"]}',
                                    'widgetsConfig': '{"episodes":{"startIndex":0,"pageSize":' + str(
                                        totalCardSize) + '},"bonus":{"startIndex":0,"pageSize":200}}',
                                }

                                response_episode = requests.get('https://www.amazon.com/gp/video/api/getDetailPage',params=params, headers=headers_episode)
                                with open(f'{self.page_save}/{hash_id}_total_episode.html','w', encoding='utf=8') as file:
                                    file.write(response_episode.text)
                                data_dict = json.loads(response_episode.text)
                            cards = data_dict.get('widgets').get('titleContent')[0].get('cards')
                            for id in cards:
                                try:
                                    series["episode_name"] = id.get('detail').get('title')
                                    series["episode_number"] = id.get('detail').get('episodeNumber')
                                    if id.get('detail').get('images').get('packshot'):
                                        series["episode_image_url"] = id.get('detail').get('images').get('packshot')
                                    else:
                                        series["episode_image_url"] = id.get('detail').get('images').get('covershot')
                                    series["epsiode_description"] = id.get('detail').get('synopsis')
                                    series["episode_hour_duration"] = id.get('detail').get('runtime')
                                    series["episode_releaseDate"] = id.get('detail').get('releaseDate')
                                    coloumns = ",".join(series.keys())
                                    print(series)

                                    print(self.insert_table(table_name=db.series_data_usa, columns=coloumns,
                                                            values=tuple(series.values())))
                                except Exception as error:
                                    print(error)

                            print(self.update_table(table_name=db.series_link_us, url_id=update_id))

                        else:
                            episode_id = final_data['collections'][f'{page_title_id}'][0]['titleIds']
                            for id in episode_id:
                                try:
                                    series["episode_name"] = final_data['detail']['detail'][f'{id}']['title'].replace('xa0','').strip()
                                    try:
                                        series["episode_number"] = final_data['detail']['detail'][f'{id}']['episodeNumber']
                                    except:
                                        series["episode_number"] = ''
                                    try:
                                        if final_data['detail']['detail'][f'{id}']['images']['packshot']:
                                            series["episode_image_url"] = final_data['detail']['detail'][f'{id}']['images']['packshot']
                                        else:
                                            series["episode_image_url"] = final_data['detail']['detail'][f'{id}']['images']['covershot']
                                    except:
                                        series["episode_image_url"] = ''
                                    try:
                                        series["epsiode_description"] = final_data['detail']['detail'][f'{id}']['synopsis']
                                    except:
                                        series["epsiode_description"] = ''
                                    try:
                                        series["episode_hour_duration"] = final_data['detail']['detail'][f'{id}']['runtime']
                                    except:
                                        series["episode_hour_duration"] = ''
                                    try:
                                        series["episode_releaseDate"] = final_data['detail']['detail'][f'{id}']['releaseDate']
                                    except:
                                        series["episode_releaseDate"] = ''


                                    coloumns = ",".join(series.keys())
                                    print(coloumns)

                                    print(self.insert_table(table_name=db.series_data_usa, columns=coloumns,values=tuple(series.values())))
                                except Exception as error:
                                    print(error)

                            print(self.update_table(table_name=db.series_link_us, url_id=update_id))


                else:
                    description = html.unescape(final_data['detail']['btfMoreDetails'][f'{page_title_id}']['synopsis'])
                    timing = final_data['detail']['btfMoreDetails'][f'{page_title_id}']['runtime']
                    totalCardSize = final_data['collections'][f'{page_title_id}'][0]['totalCardSize']
                    try:
                        release_year = final_data['detail']['btfMoreDetails'][f'{page_title_id}']['releaseYear']
                    except:
                        release_year = ''
                    category = "Series>" + ">".join([data['text'] for data in final_data['detail']['btfMoreDetails'][f'{page_title_id}']['genres']])
                    try:
                        images = final_data['detail']['btfMoreDetails'][f'{page_title_id}']['images'].get('heroshot')
                    except:
                        images = ''
                    try:
                        Rating = final_data['detail']['btfMoreDetails'][f'{page_title_id}']['amazonRating']['countFormatted']
                    except Exception as e:
                        try:
                            Rating = final_data['reviews'][f'{page_title_id}']['reviewsAnalysisModel']['reviewRatingInfo']['totalReviewCount']
                        except:
                            Rating = ''

                    starring = json.dumps([name['name'] for name in final_data['detail']['btfMoreDetails'][f'{page_title_id}']['contributors']['starringActors']])
                    try:
                        dir_list = []
                        director_list = final_data['detail']['btfMoreDetails'][f'{page_title_id}']['contributors']['directors']
                        for dir in director_list:
                            dir_name = dir.get('name')
                            dir_list.append(dir_name)
                        if dir_list:
                            director = ', '.join(dir_list)
                        else:
                            director = ''
                    except Exception as e:
                        director = ''

                    series = dict()
                    series['hash_id'] = hash_id
                    series['url'] = url
                    series['new_url'] = url
                    series['Category'] = category
                    series['name'] = name
                    series['Image_URL'] = images
                    series['Video_URL'] = ''
                    series['Trailer_URL'] = ''
                    try:
                        series['genre'] = [data['text'] for data in final_data['detail']['btfMoreDetails'][f'{page_title_id}']['genres']][0]
                    except Exception as e:
                        series['genre'] = ''
                    series['Description'] = description
                    series['Rating'] = Rating
                    series['rating_value'] = final_data['detail']['btfMoreDetails'][f'{page_title_id}']['ratingBadge']['displayText'].strip()
                    try:
                        series['review'] = final_data['reviews'][f'{page_title_id}']['reviewsAnalysisModel']['reviewRatingInfo'].get('averageRatingLabel').split('out')[0].strip()
                    except:
                        series['review'] = ''
                    try:
                        series['`Content advisory`'] = ','.join(final_data['metadata'][f'{page_title_id}'].get('contentDescriptors'))
                    except:
                        series['`Content advisory`'] = ''
                    series['`Audio languages`'] = ','.join(final_data['detail']['btfMoreDetails'][f'{page_title_id}'].get('audioTracks'))
                    series['Subtitles'] = ','.join([ii.get('text') for ii in final_data['detail']['btfMoreDetails'][f'{page_title_id}'].get('enhancedSubtitles')])
                    series['Producers'] = ','.join([ii.get('name') for ii in final_data['detail']['btfMoreDetails'][f'{page_title_id}'].get('contributors').get('producers')])
                    series['Studio'] = ','.join([ii for ii in final_data['detail']['btfMoreDetails'][f'{page_title_id}'].get('studios')])
                    if response.xpath('//div[@id="dv-action-box"]//button[@name="more-purchase-options"]'):
                        try:
                            buy_rent1 = json.loads(response.xpath('//script[@type="text/template"]//text()').getall()[-2])
                            buy_rent = buy_rent1.get('props').get('state')
                        except:
                            buy_rent1 = json.loads(response.xpath('//script[@type="text/template"]//text()').getall()[-1])
                            buy_rent = buy_rent1.get('props').get('body')[0].get('props').get('atf').get('state')
                        sub_list = []
                        series['Episode_price'] = ''
                        series['Seasion_price'] = ''
                        for by in buy_rent.get("action").get("atf").get(f'{page_title_id}').get("acquisitionActions").get("moreWaysToWatch").get('children'):
                            rent_list = []
                            buy_list = []
                            for jj in by.get('children'):
                                if jj.get('purchaseData'):
                                    purchase = jj.get('purchaseData').get('text')
                                    offerType = jj.get('purchaseData').get('offerType')
                                    videoQuality = jj.get('purchaseData').get('videoQuality')
                                    if 'isode' in purchase.lower():
                                        rent_list.append(videoQuality + ' ' + purchase.split(videoQuality)[1].strip())
                                        # movies['Rent'] = purchase
                                    else:
                                        buy_list.append(videoQuality + ' ' + purchase.split(videoQuality)[1].strip())
                                if jj.get('message'):
                                    sub_list.append(jj.get('message'))
                            if rent_list:
                                series['Episode_price'] = ','.join(rent_list)
                            if buy_list:
                                series['Seasion_price'] = ','.join(buy_list)
                        if series['Episode_price'] and series['Seasion_price']:
                            series['`Option`'] = 'Stream/Rent/Buy'
                        elif series['Episode_price']:
                            series['`Option`'] = 'Rent'
                        elif series['Seasion_price']:
                            series['`Option`'] = 'Buy'
                        else:
                            series['`Option`'] = 'Stream'
                        if sub_list:
                            series['subcription'] = " | ".join(sub_list)
                        else:
                            subcription = response.xpath('//div[@class="dv-node-dp-action-box"]//span[contains(@data-testid,"entit")]//following-sibling::*//text()').get('').strip()
                            if not subcription:
                                # subcription = response.xpath('//div[@class="dv-node-dp-action-box"]//*[class="_36qUej"]//following-sibling::*//text()').get('').strip()
                                subcription = response.xpath('//div[@class="dv-node-dp-action-box"]//*[@class="_36qUej"]//text()').get('').strip()
                                if len(subcription) < 4:
                                    subcription = response.xpath('//div[@class="dv-node-dp-action-box"]//*[@class="_36qUej"]//text()').get('').strip()
                            series['subcription'] = subcription
                    else:
                        series['Episode_price'] = ''.join([ii for ii in response.xpath('//div[@id="dv-action-box"]//span[contains(text(),"Episode")]//text()').getall() if'$' in ii]).strip()
                        series['Seasion_price'] = ''.join([ii for ii in response.xpath('//div[@id="dv-action-box"]//span[contains(text(),"Season")]//text()').getall() if '$' in ii]).strip()
                        if series['Episode_price'] and series['Seasion_price']:
                            series['`Option`'] = 'Stream/Rent/Buy'
                        elif series['Episode_price']:
                            series['`Option`'] = 'Rent'
                        elif series['Seasion_price']:
                            series['`Option`'] = 'Buy'
                        else:
                            series['`Option`'] = 'Stream'
                        subcription = response.xpath('//div[@class="dv-node-dp-action-box"]//span[contains(@data-testid,"entit")]//following-sibling::*//text()').get('').strip()
                        if not subcription:
                            # subcription = response.xpath('//div[@class="dv-node-dp-action-box"]//*[class="_36qUej"]//following-sibling::*//text()').get('').strip()
                            subcription = response.xpath('//div[@class="dv-node-dp-action-box"]//*[@class="_36qUej"]//text()').get('').strip()
                            if len(subcription) < 4:
                                subcription = response.xpath('//div[@class="dv-node-dp-action-box"]//*[@class="_36qUej"]//text()').get('').strip()
                        series['subcription'] = subcription
                    # series['Buy'] = ''.join([ii for ii in response.xpath('//div[@id="dv-action-box"]//span[contains(text(),"Buy")]//text()').getall() if'$' in ii]).strip()
                    series['Release_date'] = release_year
                    series['Hour_Duration'] = timing
                    series['Starring'] = starring
                    series['Director'] = director
                    series['season'] = season
                    if response.xpath('//div[@id="tab-content-episodes"]//a[contains(@aria-label,"Show all")]'):
                        hash_id = int(hashlib.md5(bytes(url, 'utf-8')).hexdigest(), 16) % (10 ** 8)
                        episode_file_name = f'{self.page_save}/{hash_id}_total_episode.html'
                        try:
                            if os.path.exists(episode_file_name):
                                with open(episode_file_name, 'r') as epi_data:
                                    data = epi_data.read()
                                    data_dict = json.loads(data)

                            else:



                                params = {
                                    'titleID': f"{page_title_id}",
                                    # 'isElcano': '0',
                                    'sections': 'Btf',
                                    'widgets': '{"btf":["Episodes","Bonus"]}',
                                    'widgetsConfig': '{"episodes":{"startIndex":0,"pageSize":' + str(totalCardSize) + '},"bonus":{"startIndex":0,"pageSize":200}}',
                                }
                                response_episode = requests.get('https://www.amazon.com/gp/video/api/getDetailPage',params=params, headers=headers_episode)
                                with open(f'{self.page_save}/{hash_id}_total_episode.html','w', encoding='utf=8') as file:
                                    file.write(response_episode.text)
                                data_dict = json.loads(response_episode.text)
                        except:
                            os.remove(episode_file_name)
                            params = {
                                'titleID': f"{page_title_id}",
                                # 'isElcano': '0',
                                'sections': 'Btf',
                                'widgets': '{"btf":["Episodes","Bonus"]}',
                                'widgetsConfig': '{"episodes":{"startIndex":0,"pageSize":' + str(
                                    totalCardSize) + '},"bonus":{"startIndex":0,"pageSize":200}}',
                            }

                            response_episode = requests.get('https://www.amazon.com/gp/video/api/getDetailPage',params=params, headers=headers_episode)
                            with open(f'{self.page_save}/{hash_id}_total_episode.html','w', encoding='utf=8') as file:
                                file.write(response_episode.text)
                            data_dict = json.loads(response_episode.text)

                        cards = data_dict.get('widgets').get('titleContent')[0].get('cards')
                        for id in cards:
                            try:
                                series["episode_name"] = id.get('detail').get('title')
                                series["episode_number"] = id.get('detail').get('episodeNumber')
                                if id.get('detail').get('images').get('packshot'):
                                    series["episode_image_url"] = id.get('detail').get('images').get('packshot')
                                else:
                                    series["episode_image_url"] = id.get('detail').get('images').get('covershot')
                                series["epsiode_description"] = id.get('detail').get('synopsis')
                                series["episode_hour_duration"] = id.get('detail').get('runtime')
                                series["episode_releaseDate"] = id.get('detail').get('releaseDate')
                                coloumns = ",".join(series.keys())
                                print(series)

                                print(self.insert_table(table_name=db.series_data_usa, columns=coloumns,
                                                        values=tuple(series.values())))
                            except Exception as error:
                                print(error)

                        print(self.update_table(table_name=db.series_link_us, url_id=update_id))

                    else:
                        episode_id = final_data['collections'][f'{page_title_id}'][0]['titleIds']
                        for id in episode_id:
                            try:
                                series["episode_name"] = final_data['detail']['detail'][f'{id}']['title'].replace('xa0','').strip()
                                try:
                                    series["episode_number"] = final_data['detail']['detail'][f'{id}']['episodeNumber']
                                except:
                                    series["episode_number"] = ''
                                try:
                                    if final_data['detail']['detail'][f'{id}']['images']['packshot']:series["episode_image_url"] = final_data['detail']['detail'][f'{id}']['images']['packshot']
                                    else:
                                        series["episode_image_url"] = final_data['detail']['detail'][f'{id}']['images']['covershot']
                                except:
                                    series["episode_image_url"] = ''
                                try:
                                    series["epsiode_description"] = final_data['detail']['detail'][f'{id}']['synopsis']
                                except:
                                    series["epsiode_description"] = ''
                                try:
                                    series["episode_hour_duration"] = final_data['detail']['detail'][f'{id}']['runtime']
                                except:
                                    series["episode_hour_duration"] = ''
                                try:
                                    series["episode_releaseDate"] = final_data['detail']['detail'][f'{id}']['releaseDate']
                                except:
                                    series["episode_releaseDate"] = ''

                                coloumns = ",".join(series.keys())
                                print(coloumns)

                                print(self.insert_table(table_name=db.series_data_usa, columns=coloumns,
                                                        values=tuple(series.values())))
                            except Exception as error:
                                print(error)

                        print(self.update_table(table_name=db.series_link_us, url_id=update_id))

            else:
                update_query = f"""UPDATE {db.series_link_us} SET status='{entityType}' WHERE id={link_id}"""
                self.cursor.execute(update_query)
                self.con.commit()
                print('its not a Series')
        except Exception as e:
            print('Total',e)
            with self.con.cursor() as cursor:
                update_query = f"""UPDATE {db.series_link_us} SET status1='Error' WHERE id={link_id}"""
                cursor.execute(update_query)
            self.con.commit()
            return colorama.Fore.BLUE + f"Status Updated at {link_id}" + colorama.Style.RESET_ALL



    def update_table(self, table_name, url_id):
        with self.con.cursor() as cursor:
            update_query = f"""UPDATE {table_name} SET status='Done' WHERE id={url_id}"""
            cursor.execute(update_query)
        self.con.commit()
        return colorama.Fore.BLUE + f"Status Updated at {url_id}" + colorama.Style.RESET_ALL


    def insert_table(self, table_name, columns, values):
        try:
            with self.con.cursor() as cursor:
                insert_query = f"""INSERT IGNORE INTO {table_name} ({columns}) VALUES {values}"""
                cursor.execute(insert_query)
            self.con.commit()
        except Exception as e:
            print(e)
        return colorama.Fore.GREEN + f"Movie Data Inserted" + colorama.Style.RESET_ALL


if __name__ == '__main__':
    execute("scrapy crawl second_page_read_series -a start=1 -a end=295880".split())
