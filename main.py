import requests
import json


def store_results(web_page, file_name):
    content = requests.get(web_page).json()
    with open(file_name, 'w') as outfile:
        json.dump(content, outfile, indent=4, sort_keys=True)


if __name__ == "__main__":

    steam_spy_all = "http://steamspy.com/api.php?request=all"
    filename = "steam_spy_all.json"
    store_results(steam_spy_all, filename)

    single_app_detail = "http://steamspy.com/api.php?request=appdetails&appid=730"
    filename = "appid_730.json"
    store_results(single_app_detail, filename)