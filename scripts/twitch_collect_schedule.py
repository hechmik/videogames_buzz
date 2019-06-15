from apscheduler.schedulers.blocking import BlockingScheduler
import datetime
import json
from pandas.io.json import json_normalize
from pymongo import MongoClient
import os
import requests

# module with functions for uniforming game names, possibly to use later for integration
#import uniformer


def get_all_top_games_v5(header, print_progress=False):
    """
    Collects data about games streamed on Twitch

    :param header: header for twitch APIs requests, including client ID
    :param print_progress: if set to True it prints the progress
    :returns list of all games streamed on Twitch, sorted by viewers
    """
    responses = []  # list of games

    # first request
    res = requests.get('https://api.twitch.tv/kraken/games/top?limit=100', headers=header)
    responses.extend(res.json()['top'])

    games_processed = len(res.json()['top'])

    # ask for games until there are no games left (in which case, the twitch api returns an error)
    # essentially, it loops requests until twitch returns an error, in which case the loops breaks
    # not the best solution, but I do not know if there is a better way to do this
    while True:
        try:
            res = requests.get('https://api.twitch.tv/kraken/games/top?limit=100&offset={}'.format(len(responses)),
                               headers=header)
            responses.extend(res.json()['top'])

            games_processed += len(res.json()['top'])

            if print_progress:
                print('Games processed: {}'.format(games_processed))

        except Exception:
            try:
                if res.json()['error'] == 'Bad Request':
                    print('Done collecting!')
                    break
            except Exception:
                pass

    return responses


def collect_from_twitch_once(twitch_header, mongocoll, output_data_filename,
                             save_local=True, send_mongo=True,
                             lightweight=True, flatten = True, print_progress=False):
    """
    Main job to be passed to the scheduler function, not much sense in running it on its own.
    The function collects data from Twitch, removes duplicates, adds a field with a uniformed name,
    adds a filed with a 'silly' uniformed name, deletes some fields if lightweight = True,
    adds a timestamp and updates the files with paths output_data_filename and output_names_filename.

    The file saved at output_data_filename will contain the data collected from twitch, processed as described
    The file saved at output_names_filename will contain the names of the games collected (without duplicates)

    :param twitch_header: header for the Twitch APIs requests
    :param mongocoll: the mongo collection where the data should be sent, if send mongo=True
    :param output_data_filename: path to an already existing empty file
    :param save_local: if Rrue, saves a copy of the data collected on twitch on disk
    :param send_mongo: if True, sends data collected from twitch to mongo collection
    :param lightweight: if true, some fields from the collected data are deleted
    :param print_progress: if True, prints the progress
    :return: nothing
    """

    now = datetime.datetime.utcnow()
    if print_progress:
        print('Job started at {}\n'.format(now))

    # collect games
    games = get_all_top_games_v5(twitch_header, print_progress)

    # remove duplicates, i.e. items with the same name (same game collected twice)
    # keeps the later element
    games_no_dup = {elem['game']['name']: elem for elem in games}.values()
    games_no_dup = list(games_no_dup)

    # add fields for uniformed names
    for i in range(len(games_no_dup)):
        norm_name = games_no_dup[i]['game']['name'].lower()
        games_no_dup[i]['game']['norm_name'] = norm_name

    # removes some fields if lightweight is set to True
    if lightweight:
        for item in games_no_dup:
            try:
                del item['game']['locale']
                del item['game']['localized_name']
                del item['game']['box']['small']
                del item['game']['box']['medium']
                del item['game']['box']['template']
                del item['game']['logo']['small']
                del item['game']['logo']['medium']
                del item['game']['logo']['template']
            except KeyError:
                pass

    # create dict with timestamp and collected data
    d = dict()
    d['timestamp'] = str(now)
    d['data'] = games_no_dup

    if flatten:
        d['data'] = json_normalize(d['data'], sep='_').to_dict(orient='records')

    if save_local:
        # store dict
        # since the file can get big, this way of updating it should be efficient memory-wise (or so I have read)
        with open(output_data_filename, "a") as json_file:
            json_file.write("{}\n".format(json.dumps(d)))

    if send_mongo:
        post_id = mongocoll.insert_one(d).inserted_id

    if print_progress:
        now = datetime.datetime.utcnow()
        if send_mongo:
            print('Sent to Mongo: {}'.format(post_id))
        print('Job completed at {}\n'.format(now))


def twitch_collector_scheduler(twitch_header, mongocoll, save_local=True, send_mongo=True,
                               trigger='interval', seconds=30, lightweight=True, flatten=True,
                               print_progress=False):
    """
    1. Creates the empty files where the data will be stored;
    2. Schedules how often the function collect_and_store_once should be run.

    TODO: implement better using more options given by Apscheduler, now it only uses the interval and seconds parameters

    :param twitch_header: header for the Twitch APIs requests
    :param mongocoll: the mongo collection where the data should be sent, if send mongo=True
    :param save_local: if Rrue, saves a copy of the data collected on twitch on disk
    :param send_mongo: if True, sends data collected from twitch to mongo collection
    :param trigger: type of trigger, defaults to interval
    :param seconds: wait between each function call
    :param lightweight: if true, some fields from the collected data are deleted
    :param print_progress: if True, prints the progress
    """

    utcnow = str(datetime.datetime.utcnow().strftime("%Y%m%d_%H%M"))
    output_data_filename = '{}_data.json'.format(utcnow)
    if save_local:
        # create empty file to update later
        open(output_data_filename, 'a').close()

    if print_progress:
        print('Script started at: {}\n'.format(datetime.datetime.utcnow()))

    scheduler = BlockingScheduler()
    scheduler.add_job(collect_from_twitch_once,
                      args=[twitch_header, mongocoll, output_data_filename,
                            save_local, send_mongo, lightweight, flatten, print_progress],
                      trigger=trigger,
                      seconds=seconds,
                      max_instances=10,
                      next_run_time=datetime.datetime.now())
    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass


if __name__ == "__main__":

    # the 'keys.json' file should be in the same folder as this script.
    # otherwise change the following line with the correct location
    filename = "keys.json"
    with open(filename) as file:
        keys = json.loads(file.read())
        twitch_client_ID = keys['Twitch']['Client-ID']

    header_v5 = {
        'Accept': 'application/vnd.twitchtv.v5+json',
        'Client-ID': twitch_client_ID,
    }

    client = MongoClient('localhost', 27017) #Insert the IP of the VM
    db = client.twitch
    games_coll = db.games

    twitch_collector_scheduler(header_v5, games_coll, seconds=180,
                               save_local=True, send_mongo=True, flatten=True, print_progress=True)
