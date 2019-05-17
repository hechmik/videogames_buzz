import requests


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

    # ask for games until there are no games left

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


if __name__ == "__main__":
    import json

    filename = "keys.json"
    with open(filename) as file:
        keys = json.loads(file.read())
        twitch_client_ID = keys['Twitch']['Client-ID']

    header_v5 = {
        'Accept': 'application/vnd.twitchtv.v5+json',
        'Client-ID': twitch_client_ID,
    }

    games = get_all_top_games_v5(header_v5, print_progress=True)
    print(games[0])
