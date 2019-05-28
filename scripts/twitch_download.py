import logging
# create logger with 'spam_application'
logger = logging.getLogger('spam_application')
logger.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
fh = logging.FileHandler('app.log')
fh.setLevel(logging.DEBUG)

def get_n_top_games_from_py(n, header):
    logger.info("get_n_top_games_OLD >>>")
    '''
    returns the ordered list of top games (dicts), with length between n and (n + 99)
    headers is the header of the request (specifying the Client-ID)
    '''
    responses = [] # list of games
    
    res = requests.get('https://api.twitch.tv/helix/games/top?first=100', headers=header)
    responses.extend(res.json()['data'])
           
    while len(responses) < n:
        logger.info("Length responses: {}; Content of responses: {}".format(len(responses), responses))
        res = requests.get('https://api.twitch.tv/helix/games/top?after={}&first=100'.format(res_pag(res)), headers=header)
        responses.extend(res.json()['data'])
    logger.info("get_n_top_games_OLD <<<")

    return responses
