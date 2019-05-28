from bs4 import BeautifulSoup, element
import urllib
import pandas as pd
import numpy as np
import time
from random import randint

rank = []
gname = []
platform = []
year = []
genre = []
critic_score = []
user_score = []
publisher = []
developer = []
sales_na = []
sales_pal = []
sales_jp = []
sales_ot = []
sales_gl = []
href_genre = []

urlhead = 'http://www.vgchartz.com/gamedb/?page='
urltail = '&console=&region=All&developer=&publisher=&genre=&boxart=Both&ownership=Both'
urltail += '&results=1000&order=Sales&showtotalsales=0&showtotalsales=1&showpublisher=0'
urltail += '&showpublisher=1&showvgchartzscore=0&shownasales=1&showdeveloper=1&showcriticscore=1'
urltail += '&showpalsales=0&showpalsales=1&showreleasedate=1&showuserscore=1&showjapansales=1'
urltail += '&showlastupdate=0&showothersales=1&showgenre=1&sort=GL'

def download_page(url):
    user_agent = 'Mozilla/{}.0 (Windows NT 6.1; Win64; x64)'.format(randint(1,56))
    hdr = { 'User-Agent' :  user_agent}
    print(hdr)
    req = urllib.request.Request(url, headers=hdr)
    r = urllib.request.urlopen(req).read()
    time.sleep(randint(6,15))
    return r
    
def download_games_data():
    rec_count = 0
    try:
        
        for page in range(1, 60):

            surl = urlhead + str(page) + urltail
            r = download_page(surl)
            soup = BeautifulSoup(r)
            print(f"Page: {page}")

            # vgchartz website is really weird so we have to search for
            # <a> tags with game urls
            game_tags = list(filter(
                lambda x: x.attrs['href'].startswith('http://www.vgchartz.com/game/'),
                # discard the first 10 elements because those
                # links are in the navigation bar
                soup.find_all("a")
            ))[10:]

            for tag in game_tags:

                gname.append(" ".join(tag.string.split()))
                print(f"Page {page}-{rec_count + 1} Fetch data for game {gname[-1]}")

                # get different attributes
                # traverse up the DOM tree
                data = tag.parent.parent.find_all("td")
                rank.append(np.int32(data[0].string))
                platform.append(data[3].find('img').attrs['alt'])
                publisher.append(data[4].string)
                developer.append(data[5].string)
                critic_score.append(
                    float(data[6].string) if
                    not data[6].string.startswith("N/A") else np.nan)
                user_score.append(
                    float(data[7].string) if
                    not data[7].string.startswith("N/A") else np.nan)
                sales_na.append(
                    float(data[9].string[:-1]) if
                    not data[9].string.startswith("N/A") else np.nan)
                sales_pal.append(
                    float(data[10].string[:-1]) if
                    not data[10].string.startswith("N/A") else np.nan)
                sales_jp.append(
                    float(data[11].string[:-1]) if
                    not data[11].string.startswith("N/A") else np.nan)
                sales_ot.append(
                    float(data[12].string[:-1]) if
                    not data[12].string.startswith("N/A") else np.nan)
                sales_gl.append(
                    float(data[8].string[:-1]) if
                    not data[8].string.startswith("N/A") else np.nan)
                release_year = data[13].string.split()[-1]
                # different format for year
                if release_year.startswith('N/A'):
                    year.append('N/A')
                else:
                    if int(release_year) >= 80:
                        year_to_add = np.int32("19" + release_year)
                    else:
                        year_to_add = np.int32("20" + release_year)
                    year.append(year_to_add)

                # go to every individual website to get genre info
                url_to_game = tag.attrs['href']
                href_genre.append(url_to_game)

                #req_game = urllib.request.Request(url_to_game, headers=hdr)
                #time.sleep(randint(5,15))
                #site_raw = urllib.request.urlopen(req_game).read()
                #sub_soup = BeautifulSoup(site_raw, "html.parser")
                # again, the info box is inconsistent among games so we
                # have to find all the h2 and traverse from that to the genre name
                #h2s = sub_soup.find("div", {"id": "gameGenInfoBox"}).find_all('h2')
                # make a temporary tag here to search for the one that contains
                # the word "Genre"
                #temp_tag = element.Tag
                #for h2 in h2s:
                #    if h2.string == 'Genre':
                #        temp_tag = h2
                #genre.append(temp_tag.next_sibling.string)

                rec_count += 1
    except:
        print("Something went wrong")
    columns = {
        'Rank': rank,
        'Name': gname,
        'Platform': platform,
        'Year': year,
        'href_Genre': href_genre,
        'Critic_Score': critic_score,
        'User_Score': user_score,
        'Publisher': publisher,
        'Developer': developer,
        'NA_Sales': sales_na,
        'PAL_Sales': sales_pal,
        'JP_Sales': sales_jp,
        'Other_Sales': sales_ot,
        'Global_Sales': sales_gl
    }
    print(rec_count)
    df = pd.DataFrame(columns)
    print(df.columns)
    df = df[[
        'Rank', 'Name', 'Platform', 'Year', 'href_Genre',
        'Publisher', 'Developer', 'Critic_Score', 'User_Score',
        'NA_Sales', 'PAL_Sales', 'JP_Sales', 'Other_Sales', 'Global_Sales']]
    df.to_csv("vgsales.csv", sep=",", encoding='utf-8', index=False)

if __name__ == "__main__":
    download_games_data()