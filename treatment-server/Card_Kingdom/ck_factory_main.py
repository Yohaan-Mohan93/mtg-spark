from datetime import date
from datetime import datetime
from helper import create_directory
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType,StructField,StringType,IntegerType
from website import website_functions
import logging
import os

class ck_factory(website_functions):

    def load_non_foil_data(self,session, input_date):

        date_today = date.today().strftime('%Y%m%d')
        path_to_card_logs='Logs/Card_Kingdom_Logs'
        result = create_directory(path_to_card_logs)
        logs_filename = path_to_card_logs+'/Load_Non_Foil_Data_'+date_today+'.log'
        logging.basicConfig(filename=logs_filename, level=logging.INFO,format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
        
        input_file='input/CK_PRICES_'+ input_date + '.txt'
        logging.info(f'Reading from file: {input_file}')
        
        csv_file = session.read.option("header","true").option("sep","|").option("inferSchema", "true").csv(input_file)
        scrape_date = datetime.strptime(input_date,'%Y%m%d').strftime('%Y-%m-%d')

        csv_file = csv_file.select('ID',lit(scrape_date).alias('Date'),'Name','Type','Set','Rarity','NM(USD)','EX(USD)','VG(USD)','G(USD)','Card Text')
        input_data = csv_file.orderBy("Name")
        input_data_size = input_data.count()

        if input_data_size == 0:
            logging.info(f'No data in the file')
            return
        else: 
            logging.info(f'Size of csv_file for {input_date}: {input_data_size}')
            logging.info(f'writing data to table: mtgcards.card_kingdom')

            mysql_url = os.getenv('MYSQL_URL')
            mysql_user = os.getenv('MYSQL_USER')
            mysql_pwd = os.getenv('MYSQL_PASSWORD')

            input_data.write.format("jdbc")\
                            .option("url", mysql_url)\
                            .option("driver", "com.mysql.cj.jdbc.Driver")\
                            .option("dbtable","mtgcards.card_kingdom")\
                            .option("user", mysql_user)\
                            .mode("append")\
                            .option("password", mysql_pwd).save()

            scraper_history_schema = StructType([
                StructField('Date',StringType(),False),
                StructField('website',StringType(),False),
                StructField('insert_count',IntegerType(),False),
                StructField('foil_non_foil',StringType(),False)
            ])
            emptyRdd = session.sparkContext.emptyRDD()
            logging.info(f'Scrape Date: {scrape_date}')
            scraper_history = session.createDataFrame(data=[(scrape_date,'Card Kingdom',input_data_size,'Non Foil')],schema=scraper_history_schema)

            scraper_history.write.format("jdbc")\
                .option("url", mysql_url)\
                .option("driver", "com.mysql.cj.jdbc.Driver")\
                .option("dbtable","mtgcards.scraper_history")\
                .option("user", mysql_user)\
                .mode("append")\
                .option("password", mysql_pwd).save()

    def load_foil_data(self,session, input_date):

        date_today = date.today().strftime('%Y%m%d')
        path_to_card_logs='Logs/Card_Kingdom_Logs'
        result = create_directory(path_to_card_logs)
        logs_filename = path_to_card_logs+'/Load_Foil_Data_'+date_today+'.log'
        logging.basicConfig(filename=logs_filename, level=logging.INFO,format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
        
        input_file='input/CK_FOIL_PRICES_'+ input_date + '.txt'
        logging.info(f'Reading from file: {input_file}')
        
        csv_file = session.read.option("header","true").option("sep","|").option("inferSchema", "true").csv(input_file)
        scrape_date = datetime.strptime(input_date,'%Y%m%d').strftime('%Y-%m-%d')

        csv_file = csv_file.select('ID',lit(scrape_date).alias('Date'),'Name','Type','Set','Rarity','NM(USD)','EX(USD)','VG(USD)','G(USD)','Card Text')
        input_data = csv_file.orderBy("Name")
        input_data_size = input_data.count()

        if input_data_size == 0:
            logging.info(f'No data in the file')
            return
        else: 
            logging.info(f'Size of csv_file for {input_date}: {input_data_size}')
            logging.info(f'writing data to table: mtgcards.card_kingdom_foil')

            mysql_url = os.getenv('MYSQL_URL')
            mysql_user = os.getenv('MYSQL_USER')
            mysql_pwd = os.getenv('MYSQL_PASSWORD')

            input_data.write.format("jdbc")\
                            .option("url", mysql_url)\
                            .option("driver", "com.mysql.cj.jdbc.Driver")\
                            .option("dbtable","mtgcards.card_kingdom_foil")\
                            .option("user", mysql_user)\
                            .mode("append")\
                            .option("password", mysql_pwd).save()

            scraper_history_schema = StructType([
                StructField('Date',StringType(),False),
                StructField('website',StringType(),False),
                StructField('insert_count',IntegerType(),False),
                StructField('foil_non_foil',StringType(),False)
            ])
            emptyRdd = session.sparkContext.emptyRDD()
            logging.info(f'Scrape Date: {scrape_date}')
            scraper_history = session.createDataFrame(data=[(scrape_date,'Card Kingdom',input_data_size,'Foil')],schema=scraper_history_schema)

            scraper_history.write.format("jdbc")\
                .option("url", mysql_url)\
                .option("driver", "com.mysql.cj.jdbc.Driver")\
                .option("dbtable","mtgcards.scraper_history")\
                .option("user", mysql_user)\
                .mode("append")\
                .option("password", mysql_pwd).save()    


        
