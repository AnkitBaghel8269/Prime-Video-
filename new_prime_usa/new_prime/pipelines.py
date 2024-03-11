# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from new_prime import db_config as db
from new_prime.items import OTTPlatformsLinksItem,OTTMoviedataItem,RestaurantguruItem,RestaurantgurudataItem
class NewPrimePipeline:
    def insert_item(self, item, spider, table_name, product_table=0):
        try:
            table_name = item.pop('table_name')
            data_fetch_table_name = item.pop('data_fetch_table_name')
            if product_table:
                id = item.pop('id')
            field_list = []
            value_list = []
            for field in item:
                field_list.append(str(field))
                value_list.append('%s')
            fields = ','.join(field_list)
            values = ", ".join(value_list)
            insert_db = f"insert into {table_name}( " + fields + " ) values ( " + values + " )"
            spider.cursor.execute(insert_db, tuple(item.values()))
            if product_table:
                update = f'update {data_fetch_table_name} set status="Done" where Id=%s'
                spider.cursor.execute(update, id)
            spider.con.commit()
            spider.logger.info(f'{table_name} Inserted...')
            spider.con.commit()
        except Exception as e:
            spider.logger.error(e)

    def process_item(self, item, spider):

        if isinstance(item, OTTPlatformsLinksItem):
            self.insert_item(item, spider, db.new_series_link)
        # if isinstance(item, IcsV1AssetItem):
        #     self.insert_item(item, spider, db.asset_table)
        if isinstance(item, RestaurantgurudataItem):
            self.insert_item(item, spider, item.get('table_name'),1)
        if isinstance(item, OTTMoviedataItem):
            self.insert_item(item, spider, item.get('table_name'),1)
        #
        if isinstance(item, RestaurantguruItem):
            self.insert_item(item, spider, db.us_series_tid)

        return item
