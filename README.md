# Data Management Project
## Popularity of videogames on Twitch and Twitter
The gaming sector is no longer confined to local and online matches: social networks such as Twitch and Twitter are benefiting from the ever-growing success of the industry as they are the de facto virtual square where people from all around the world share their progress and discuss outstanding gameplay moves or the latest rumors.
In this project we will try to measure and quantify the "buzz" around the most popular games of the past and present on these two social media, so that it will be possible to analyze in a data-driven way the popularity of the selected games. 

## Running the code
The scripts for collecting data from Twitch, Twitter and VGChartz can be found in the "scripts" folder, while the collected data can be found in the "dataset" folder.
In the "notebooks" folder there are four notebooks:
- Notebook 00 is a guide on how to upload the data from the "dataset" folder on your own local MongoDB instance;
- Notebook 01 demonstrates how to use the scripts for collecting data from Twitch and Twitter;
- Notebook 02 shows the steps that led to the identification of 50 games to monitor;
- Notebook 03 explores the data by querying the MongoDB.

In order to run the scripts for downloading data from Twitch and Twitter, keys for the respective API's are needed. The "keys.json" file can be used to store them: the scripts and the notebooks will access this file in order to retrieve them.

The script used for downloading the files, *download_top50_tweets.py*, needs the following files in the same directory:
- *download_top50_config.json*, where parameters such as the path of Twitter credentials, the file containing the games that needs to be downloaded, time range of the tweets that you want to download and MongoDB's IP and Port needs to be specified
- *collect_and_store_tweets.py*, that is the custom library for downloading and storing tweets