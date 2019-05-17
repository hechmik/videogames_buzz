from apscheduler.schedulers.blocking import BlockingScheduler
from ast import literal_eval
import datetime
import json
import os

# module with function for collecting raw game data
import twitch
# module with functions for uniforming game names, possibly to use later for integration
import uniformer


def collect_and_store_once(twitch_header, output_data_filename,
                           output_names_filename, lightweight=True, print_progress=False):
    """
    Main job to be passed to the scheduler function, not much sense in running it on its own.
    The function collects data from Twitch, removes duplicates, adds a field with a uniformed name,
    adds a filed with a 'silly' uniformed name, deletes some fields if lightweight = True,
    adds a timestamp and updates the files with paths output_data_filename and output_names_filename.

    The file saved at output_data_filename will contain the data collected from twitch, processed as described
    The file saved at output_names_filename will contain the names of the games collected (without duplicates)

    :param twitch_header: header for the Twitch APIs requests
    :param output_data_filename: path to an already existing empty file
    :param output_names_filename: path to an already existing empty file
    :param lightweight: if true, some fields from the collected data are deleted
    :param print_progress: if True, prints the progress
    :return: nothing
    """

    now = datetime.datetime.utcnow()
    if print_progress:
        print('Job started at {}\n'.format(now))

    # collect games
    games = twitch.get_all_top_games_v5(twitch_header, print_progress)

    # remove duplicates, i.e. items with the same name (same game collected twice)
    # keeps the later element
    games_no_dup = {elem['game']['name']: elem for elem in games}.values()
    games_no_dup = list(games_no_dup)

    # add fields for uniformed names
    for i in range(len(games_no_dup)):
        norm_name = uniformer.uniform(games_no_dup[i]['game']['name'])
        norm_unary_name = uniformer.silly_uniform(games_no_dup[i]['game']['name'])
        games_no_dup[i]['game']['norm_name'] = norm_name
        games_no_dup[i]['game']['unary_name'] = norm_unary_name

    # removes some fields if lightweight is set to True
    if lightweight:
        for item in games_no_dup:
            try:
                del item['game']['locale']
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

    # store dict
    # since the file can get big, this way of updating it should be efficient memory-wise (or so I have read)
    with open(output_data_filename, "a") as json_file:
        json_file.write("{}\n".format(json.dumps(d)))

    # store names of games
    # the file containing the names of games is not very big (less then 100KB), so loading it should not be a problem
    # TODO: is there a better way?
    games_set = {games_no_dup[i]['game']['norm_name'] for i in range(len(games_no_dup))}

    with open(output_names_filename, 'r', encoding='utf8') as f:
        content = f.read()

    if content == '':
        content = set(content)
    else:
        content = set(literal_eval(content))

    content = games_set | content

    with open(output_names_filename, 'w', encoding='utf8') as f:
        f.write(str(list(content)))

    if print_progress:
        now = datetime.datetime.utcnow()
        print('Job completed at {}\n'.format(now))


def twitch_collector_scheduler(twitch_header, trigger='interval', seconds=30, lightweight=True, print_progress=False):
    """
    1. Creates the empty files where the data will be stored;
    2. Schedules how often the function collect_and_store_once should be run.

    TODO: implement better using more options given by Apscheduler, now it only uses the interval and seconds parameters

    :param twitch_header: header for the Twitch APIs requests
    :param trigger: type of trigger, defaults to interval
    :param seconds: wait between each function call
    :param lightweight: if true, some fields from the collected data are deleted
    :param print_progress: if True, prints the progress
    """
    utcnow = str(datetime.datetime.utcnow().strftime("%Y%m%d_%H%M"))
    output_data_filename = '{}_data.json'.format(utcnow)
    output_names_filename = '{}_names.txt'.format(utcnow)

    # create empty files to update later
    open(output_data_filename, 'a').close()
    open(output_names_filename, 'a').close()


    if print_progress:
        print('Script started at: {}\n'.format(datetime.datetime.utcnow()))

    scheduler = BlockingScheduler()
    scheduler.add_job(collect_and_store_once,
                      args=[twitch_header, output_data_filename,
                            output_names_filename, lightweight, print_progress],
                      trigger=trigger,
                      seconds=seconds,
                      next_run_time=datetime.datetime.now())
    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass


if __name__ == "__main__":

    filename = "keys.json"
    with open(filename) as file:
        keys = json.loads(file.read())
        twitch_client_ID = keys['Twitch']['Client-ID']

    header_v5 = {
        'Accept': 'application/vnd.twitchtv.v5+json',
        'Client-ID': twitch_client_ID,
    }

    twitch_collector_scheduler(header_v5, print_progress=True)
