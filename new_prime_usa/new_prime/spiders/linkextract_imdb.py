import json
import pymysql

from scrapy.cmdline import execute
import new_prime.db_config as db
from new_prime.items import *
# here Define class which is scraped links from imdb.com

class LinkextractSpider(scrapy.Spider):
    # Define Spider name
    name = "linkextract_imdb"
    # here i am set domain which is i want ot allow
    allowed_domains = ["caching.graphql.imdb.com"]
    # start_urls = ["https://restaurantguru.com"]

    def __init__(self, name=None, start=0, end=0, **kwargs):
        super().__init__(name, **kwargs)
        # DATABASE SPECIFIC VALUES
        self.start = int(start)
        self.end = int(end)

        # DATABASE CONNECTION
        self.con = pymysql.connect(host=db.db_host, user=db.db_user, password=db.db_password,database=db.db_name)
        self.cursor = self.con.cursor()
    # Here i am define start_requests function which is sent request in first page of imdb website
    def start_requests(self):
        # set post url
        url = "https://caching.graphql.imdb.com/"
        # set first page key which is need in payload
        # Define two variables
        moives_after_paylod = 'eyJlc1Rva2VuIjpbIjIyMiIsIjIyMiIsInR0MTU2NzEwMjgiXSwiZmlsdGVyIjoie1wiY29uc3RyYWludHNcIjp7XCJjZXJ0aWZpY2F0ZUNvbnN0cmFpbnRcIjp7fSxcImNvbG9yYXRpb25Db25zdHJhaW50XCI6e30sXCJjcmVkaXRlZENvbXBhbnlDb25zdHJhaW50XCI6e30sXCJnZW5yZUNvbnN0cmFpbnRcIjp7fSxcImxpc3RDb25zdHJhaW50XCI6e1wiaW5BbGxMaXN0c1wiOltdLFwiaW5BbGxQcmVkZWZpbmVkTGlzdHNcIjpbXSxcIm5vdEluQW55TGlzdFwiOltdLFwibm90SW5BbnlQcmVkZWZpbmVkTGlzdFwiOltdfSxcInJlbGVhc2VEYXRlQ29uc3RyYWludFwiOntcInJlbGVhc2VEYXRlUmFuZ2VcIjp7fX0sXCJydW50aW1lQ29uc3RyYWludFwiOntcInJ1bnRpbWVSYW5nZU1pbnV0ZXNcIjp7fX0sXCJ0aXRsZVR5cGVDb25zdHJhaW50XCI6e1wiYW55VGl0bGVUeXBlSWRzXCI6W1wibW92aWVcIl19LFwidXNlclJhdGluZ3NDb25zdHJhaW50XCI6e1wiYWdncmVnYXRlUmF0aW5nUmFuZ2VcIjp7fSxcInJhdGluZ3NDb3VudFJhbmdlXCI6e319LFwid2F0Y2hPcHRpb25zQ29uc3RyYWludFwiOntcImFueVdhdGNoUHJvdmlkZXJJZHNcIjpbXCJhbXpuMS5pbWRiLncydy5wcm92aWRlci5wcmltZV92aWRlby5QUklNRVwiLFwiYW16bjEuaW1kYi53MncucHJvdmlkZXIucHJpbWVfdmlkZW9cIl0sXCJhbnlXYXRjaFJlZ2lvbnNcIjpbXCJVU1wiLFwiVVNcIl0sXCJleGNsdWRlV2F0Y2hQcm92aWRlcklkc1wiOltdLFwiZXhjbHVkZVdhdGNoUmVnaW9uc1wiOltdfX0sXCJsYW5ndWFnZVwiOlwiZW4tVVNcIixcInNvcnRcIjp7XCJzb3J0QnlcIjpcIlBPUFVMQVJJVFlcIixcInNvcnRPcmRlclwiOlwiQVNDXCJ9LFwicmVzdWx0SW5kZXhcIjo0OX0ifQ=='
        series_after_paylod = 'eyJlc1Rva2VuIjpbIjEyNiIsIjEyNiIsInR0MzU4MTkyMCJdLCJmaWx0ZXIiOiJ7XCJjb25zdHJhaW50c1wiOntcImNlcnRpZmljYXRlQ29uc3RyYWludFwiOnt9LFwiY29sb3JhdGlvbkNvbnN0cmFpbnRcIjp7fSxcImNyZWRpdGVkQ29tcGFueUNvbnN0cmFpbnRcIjp7fSxcImdlbnJlQ29uc3RyYWludFwiOnt9LFwibGlzdENvbnN0cmFpbnRcIjp7XCJpbkFsbExpc3RzXCI6W10sXCJpbkFsbFByZWRlZmluZWRMaXN0c1wiOltdLFwibm90SW5BbnlMaXN0XCI6W10sXCJub3RJbkFueVByZWRlZmluZWRMaXN0XCI6W119LFwicmVsZWFzZURhdGVDb25zdHJhaW50XCI6e1wicmVsZWFzZURhdGVSYW5nZVwiOnt9fSxcInJ1bnRpbWVDb25zdHJhaW50XCI6e1wicnVudGltZVJhbmdlTWludXRlc1wiOnt9fSxcInRpdGxlVHlwZUNvbnN0cmFpbnRcIjp7XCJhbnlUaXRsZVR5cGVJZHNcIjpbXCJ0dlNlcmllc1wiXX0sXCJ1c2VyUmF0aW5nc0NvbnN0cmFpbnRcIjp7XCJhZ2dyZWdhdGVSYXRpbmdSYW5nZVwiOnt9LFwicmF0aW5nc0NvdW50UmFuZ2VcIjp7fX0sXCJ3YXRjaE9wdGlvbnNDb25zdHJhaW50XCI6e1wiYW55V2F0Y2hQcm92aWRlcklkc1wiOltcImFtem4xLmltZGIudzJ3LnByb3ZpZGVyLnByaW1lX3ZpZGVvLlBSSU1FXCIsXCJhbXpuMS5pbWRiLncydy5wcm92aWRlci5wcmltZV92aWRlb1wiXSxcImFueVdhdGNoUmVnaW9uc1wiOltcIlVTXCIsXCJVU1wiXSxcImV4Y2x1ZGVXYXRjaFByb3ZpZGVySWRzXCI6W10sXCJleGNsdWRlV2F0Y2hSZWdpb25zXCI6W119fSxcImxhbmd1YWdlXCI6XCJlbi1VU1wiLFwic29ydFwiOntcInNvcnRCeVwiOlwiUE9QVUxBUklUWVwiLFwic29ydE9yZGVyXCI6XCJBU0NcIn0sXCJyZXN1bHRJbmRleFwiOjQ5fSJ9'

        # Create a dictionary to associate names with variables
        variables = {"1": moives_after_paylod, "2": series_after_paylod}

        # Get user input for variable selection
        selected_variable = input("Please Enter 1 For movies Or 2 for Series (movies or series): ")

        # Check if the selected variable exists in the dictionary
        if selected_variable in variables:
            # Access the selected variable's value
            selected_variable_value = variables[selected_variable]
            print("Selected variable's value:", selected_variable_value)
        else:
            print("Invalid variable name entered.")
        payload = json.dumps({
            "operationName": "AdvancedTitleSearch",
            "variables": {
                "locale": "en-US",
                "first": 500,
                "titleTypeConstraint": {
                    "anyTitleTypeIds": [
                        "tvSeries"
                    ]
                },
                "genreConstraint": {},
                "certificateConstraint": {},
                "userRatingsConstraint": {
                    "aggregateRatingRange": {},
                    "ratingsCountRange": {}
                },
                "creditedCompanyConstraint": {},
                "sortBy": "POPULARITY",
                "sortOrder": "ASC",
                "releaseDateConstraint": {
                    "releaseDateRange": {}
                },
                "colorationConstraint": {},
                "runtimeConstraint": {
                    "runtimeRangeMinutes": {}
                },
                "watchOptionsConstraint": {
                    "anyWatchProviderIds": [
                        "amzn1.imdb.w2w.provider.prime_video.PRIME",
                        "amzn1.imdb.w2w.provider.prime_video"
                    ],
                    "anyWatchRegions": [
                        "US",
                        "US"
                    ],
                    "excludeWatchProviderIds": [],
                    "excludeWatchRegions": []
                },
                "listConstraint": {
                    "inAllLists": [],
                    "notInAnyList": [],
                    "inAllPredefinedLists": [],
                    "notInAnyPredefinedList": []
                },
                "after": selected_variable_value
            },
            "extensions": {
                "persistedQuery": {
                    "version": 1,
                    "sha256Hash": "e7a1c7b10a7a9765731e5c874cef0342dfbd0dd7a87fa796e828778e54a07a20"
                }
            }
        })
        headers = {
            'authority': 'caching.graphql.imdb.com',
            'accept': 'application/graphql+json, application/json',
            'accept-language': 'en-US,en;q=0.9',
            'content-type': 'application/json',
            'cookie': 'session-id=141-6699834-8346653; session-id-time=2082787201l; ad-oo=0; ubid-main=130-3507256-0250445; ci=e30; session-token=YUIeie0Uh5AsF1R7GzBeZYzkixVDVgj6y7cGvwm9CAWSAxtcz7ULyRdKiHHQSURxjLNwEzSLjOMOe6Bxvz2T1SWQ9Q57at1E6SjM12AxudC69UqDzFbaEnmp+/IR/MvY3z6Q0ykWj3u7h1hQb0XeE0MQBWLHIHB0K7LxMyNls1BcoT10RRsbuC9LOhPb9Bk7GZvHd5X/pzg85CBeh08GROO35K34nvcdqFLPQQEmqBVscHV9ARg1H7zUUcmrxNvxqb5iUfhLtrG97r5Au63w5yxNSLfu1cLw+TEtFSoJ4l1roRBo4sGEZlzXCOKNytKXOUoxFdPUlk9CrEIq+c1VQrCnLHxKPAup; session-id=147-6608231-1583754; session-id-time=2082787201l',
            'origin': 'https://www.imdb.com',
            'referer': 'https://www.imdb.com/',
            'sec-ch-ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-site',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            'x-amzn-sessionid': '141-6699834-8346653',
            'x-imdb-client-name': 'imdb-web-next-localized',
            'x-imdb-client-rid': 'GTWY7MTRH7T5XKZG1F8J',
            'x-imdb-user-country': 'US',
            'x-imdb-user-language': 'en-US',
            'x-imdb-weblab-treatment-overrides': '{"IMDB_DESKTOP_SEARCH_ALGORITHM_UPDATES_577300":"T1","IMDB_NAV_PRO_FLY_OUT_Q1_REFRESH_848923":"T2"}'
        }
        yield scrapy.Request(url=url,method="POST",headers=headers,body=payload,dont_filter=True)

    # Here I am Define parse function
    def parse(self, response, **kwargs):
        # set item name
        item = RestaurantguruItem()
        # here i am loading json from response.text
        data = json.loads(response.text)
        all_link = []
        # data inserting one by one
        for i in data['data']['advancedTitleSearch']['edges']:
            item['sid'] = i['node']['title']['id']
            try:
                item['name'] = i['node']['title']['titleText']['text']
            except:
                item['name'] = 'N/A'
            try:
                item['ryear'] = i['node']['title']['releaseYear']['year']
            except:
                item['ryear'] = 'N/A'
            yield item
        # here i am getting next page key which is needable for check next page is available or not
        next_page = data['data']['advancedTitleSearch']['pageInfo']['hasNextPage']
        if next_page == True:
            url = "https://caching.graphql.imdb.com/"
            # here i am getting next page key which is needable in payload
            after_paylod = data['data']['advancedTitleSearch']['pageInfo']['endCursor']
            payload = json.dumps({
                "operationName": "AdvancedTitleSearch",
                "variables": {
                    "locale": "en-US",
                    "first": 500,
                    "titleTypeConstraint": {
                        "anyTitleTypeIds": [
                            "tvSeries"
                        ]
                    },
                    "genreConstraint": {},
                    "certificateConstraint": {},
                    "userRatingsConstraint": {
                        "aggregateRatingRange": {},
                        "ratingsCountRange": {}
                    },
                    "creditedCompanyConstraint": {},
                    "sortBy": "POPULARITY",
                    "sortOrder": "ASC",
                    "releaseDateConstraint": {
                        "releaseDateRange": {}
                    },
                    "colorationConstraint": {},
                    "runtimeConstraint": {
                        "runtimeRangeMinutes": {}
                    },
                    "watchOptionsConstraint": {
                        "anyWatchProviderIds": [
                            "amzn1.imdb.w2w.provider.prime_video.PRIME",
                            "amzn1.imdb.w2w.provider.prime_video"
                        ],
                        "anyWatchRegions": [
                            "US",
                            "US"
                        ],
                        "excludeWatchProviderIds": [],
                        "excludeWatchRegions": []
                    },
                    "listConstraint": {
                        "inAllLists": [],
                        "notInAnyList": [],
                        "inAllPredefinedLists": [],
                        "notInAnyPredefinedList": []
                    },
                    "after": after_paylod
                },
                "extensions": {
                    "persistedQuery": {
                        "version": 1,
                        "sha256Hash": "e7a1c7b10a7a9765731e5c874cef0342dfbd0dd7a87fa796e828778e54a07a20"
                    }
                }
            })
            headers = {
                'authority': 'caching.graphql.imdb.com',
                'accept': 'application/graphql+json, application/json',
                'accept-language': 'en-US,en;q=0.9',
                'content-type': 'application/json',
                'cookie': 'session-id=141-6699834-8346653; session-id-time=2082787201l; ad-oo=0; ubid-main=130-3507256-0250445; ci=e30; session-token=YUIeie0Uh5AsF1R7GzBeZYzkixVDVgj6y7cGvwm9CAWSAxtcz7ULyRdKiHHQSURxjLNwEzSLjOMOe6Bxvz2T1SWQ9Q57at1E6SjM12AxudC69UqDzFbaEnmp+/IR/MvY3z6Q0ykWj3u7h1hQb0XeE0MQBWLHIHB0K7LxMyNls1BcoT10RRsbuC9LOhPb9Bk7GZvHd5X/pzg85CBeh08GROO35K34nvcdqFLPQQEmqBVscHV9ARg1H7zUUcmrxNvxqb5iUfhLtrG97r5Au63w5yxNSLfu1cLw+TEtFSoJ4l1roRBo4sGEZlzXCOKNytKXOUoxFdPUlk9CrEIq+c1VQrCnLHxKPAup; session-id=147-6608231-1583754; session-id-time=2082787201l',
                'origin': 'https://www.imdb.com',
                'referer': 'https://www.imdb.com/',
                'sec-ch-ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
                'sec-fetch-dest': 'empty',
                'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'same-site',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
                'x-amzn-sessionid': '141-6699834-8346653',
                'x-imdb-client-name': 'imdb-web-next-localized',
                'x-imdb-client-rid': 'GTWY7MTRH7T5XKZG1F8J',
                'x-imdb-user-country': 'US',
                'x-imdb-user-language': 'en-US',
                'x-imdb-weblab-treatment-overrides': '{"IMDB_DESKTOP_SEARCH_ALGORITHM_UPDATES_577300":"T1","IMDB_NAV_PRO_FLY_OUT_Q1_REFRESH_848923":"T2"}'
            }
            yield scrapy.Request(url=url, method="POST", headers=headers, body=payload, dont_filter=True, callback=self.parse)

if __name__ == '__main__':
    execute("scrapy crawl linkextract_imdb".split())
