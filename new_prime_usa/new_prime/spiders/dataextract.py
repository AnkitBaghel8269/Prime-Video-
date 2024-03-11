import json
import pymysql
import scrapy
from scrapy.cmdline import execute
import new_prime.db_config as db
from new_prime.items import RestaurantgurudataItem

# here i am Define class "DataextractSpider"
class DataextractSpider(scrapy.Spider):
    # Define Spider name
    name = "dataextract"
    allowed_domains = ["api.graphql.imdb.com"]


    def __init__(self, name=None, start=0, end=0, **kwargs):
        super().__init__(name, **kwargs)
        # DATABASE SPECIFIC VALUES
        self.start = int(start)
        self.end = int(end)

        # DATABASE CONNECTION
        self.con = pymysql.connect(host=db.db_host, user=db.db_user, password=db.db_password,database=db.db_name)
        self.cursor = self.con.cursor()
    # here i am Define start_requests function
    def start_requests(self):
        us_series_tid = db.us_series_tid
        us_movie_tid = db.us_movie_tid

        # Create a dictionary to associate names with variables
        variables = {"1": us_movie_tid, "2": us_series_tid}

        # Get user input for variable selection
        selected_variable = input("Please Enter 1 For movies Or 2 for Series (movies or series): ")


        # Check if the selected variable exists in the dictionary
        if selected_variable in variables:
            # Access the selected variable's value
            selected_variable_value = variables[selected_variable]
            print("Selected variable's value:", selected_variable_value)
        else:
            print("Invalid variable name entered.")
        # making query for fetch data from links table
        query = f"select * FROM {selected_variable_value} where status='Done' and id between {self.start} and {self.end}"
        self.cursor.execute(query)
        query_results = self.cursor.fetchall()
        self.logger.info(f"\n\n\nTotal Results ...{len(query_results)}\n\n\n", )
        if selected_variable_value == 'us_movie_tid':
            inserted_table_name = db.movies_link_us
        else:
            inserted_table_name = db.series_link_us
        for query_result in query_results:
            meta = {
                "Id": query_result[0],
                "res_id": query_result[1],
                "table_name": inserted_table_name,
                "data_fetch_table_name": selected_variable_value,
            }
            url = f"https://api.graphql.imdb.com/?operationName=HERO_WATCH_BOX&variables=%7B%22heroNowDateDay%22%3A23%2C%22heroNowDateMonth%22%3A1%2C%22heroNowDateYear%22%3A2024%2C%22heroYesterdayDateDay%22%3A22%2C%22heroYesterdayDateMonth%22%3A1%2C%22heroYesterdayDateYear%22%3A2024%2C%22id%22%3A%22{query_result[1]}%22%2C%22locale%22%3A%22en-US%22%2C%22location%22%3A%7B%22latLong%22%3A%7B%22lat%22%3A%2240.75%22%2C%22long%22%3A%22-73.99%22%7D%7D%2C%22providerId%22%3A%22amzn1.imdb.w2w.provider.prime_video.starzSub%22%7D&extensions=%7B%22persistedQuery%22%3A%7B%22sha256Hash%22%3A%2245952ca14ab0e7da9aec09b4ae7fa7733e04b01ba2900eb58f9320f18fcb7ec8%22%2C%22version%22%3A1%7D%7D"
            url = f'https://api.graphql.imdb.com/?operationName=HERO_WATCH_BOX&variables=%7B%22heroNowDateDay%22%3A1%2C%22heroNowDateMonth%22%3A3%2C%22heroNowDateYear%22%3A2024%2C%22heroYesterdayDateDay%22%3A29%2C%22heroYesterdayDateMonth%22%3A2%2C%22heroYesterdayDateYear%22%3A2024%2C%22id%22%3A%22{query_result[1]}%22%2C%22locale%22%3A%22en-US%22%2C%22location%22%3A%7B%22latLong%22%3A%7B%22lat%22%3A%2247.61%22%2C%22long%22%3A%22-122.33%22%7D%7D%2C%22providerId%22%3Anull%7D&extensions=%7B%22persistedQuery%22%3A%7B%22sha256Hash%22%3A%222345bd946351d79388dd2a7966d6136bfac3578ab1248a89b2c20285307c88c0%22%2C%22version%22%3A1%7D%7D'
            payload = {}
            headers = {
                'sec-ch-ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
                'sec-ch-ua-mobile': '?0',
                # 'x-amzn-sessionid': '141-6699834-8346653',
                'x-imdb-weblab-treatment-overrides': '{"IMDB_DESKTOP_SEARCH_ALGORITHM_UPDATES_577300":"T1","IMDB_NAV_PRO_FLY_OUT_Q1_REFRESH_848923":"T2"}',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
                'content-type': 'application/json',
                # 'x-imdb-client-rid': 'R7MNET26514E5WEC13QV',
                'accept': 'application/graphql+json, application/json',
                'x-imdb-client-name': 'imdb-web-next-localized',
                'x-imdb-user-country': 'US',
                'Referer': 'https://www.imdb.com/',
                'x-imdb-user-language': 'en-US',
                'sec-ch-ua-platform': '"Windows"',
                # 'Cookie': 'session-id=147-6608231-1583754; session-id-time=2082787201l'
            }
            yield scrapy.Request(
                url=url,
                headers=headers,
                # cookies=cookies,
                dont_filter=True,
                meta=meta,
            )

    def parse(self, response, **kwargs):
        item = RestaurantgurudataItem()
        item['id'] = response.meta['Id']
        item['show_id'] = response.meta['res_id']
        data = json.loads(response.text)
        try:
            for i in data['data']['title']['watchOptionsByCategory']['categorizedWatchOptionsList'][0]['watchOptions']:
                if i['title']['value']== 'Watch on Prime Video':

                    item['platform_name'] = i['title']['value']
                    item['platform_url'] = i['link']
                    item['table_name'] = response.meta['table_name']
                    item['data_fetch_table_name'] = response.meta['data_fetch_table_name']
                    yield item
        except:pass

if __name__ == '__main__':
    execute("scrapy crawl dataextract -a start=1 -a end=1000000".split())
