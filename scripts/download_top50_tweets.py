import logging
import json
import collect_and_store_tweets

logging.root.handlers = []
logging.basicConfig(format='%(asctime)s|%(name)s|%(levelname)s| %(message)s',
                    level=logging.INFO,
                    filename="top50_tweets_app.log")

# set up logging to console
console = logging.StreamHandler()
console.setLevel(logging.INFO)
# set a format which is simpler for console use
formatter = logging.Formatter(fmt='%(asctime)s|%(name)s|%(levelname)s| %(message)s',
                              datefmt="%d-%m-%Y %H:%M:%S")
console.setFormatter(formatter)
logging.getLogger("").addHandler(console)

logging.info('Application started')
with open("download_top50_config.json", "r") as filehandler:
    config = json.load(filehandler)
logging.info('Loaded config file')
with open(config['top_50_games_fn'], "r") as filehandler:
    top50 = json.load(filehandler)
logging.info('Loaded top_50_games file')
games_list = top50['selected_top_50_games']['name']
collect_and_store_tweets.download_and_store_tweets_on_default_mongodb_instance(keys_filename=config['keys_fn'],
                                                      list_of_queries=games_list,
                                                      since_date=config['start_date'],
                                                      until_date=config['end_date'],
                                                      ip_mongo=config['ip_mongo'],
                                                      mongo_port=config['mongo_port'])
logging.info('Finished, good bye :) !')
