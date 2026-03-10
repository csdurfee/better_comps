"""
Scrapes all draft related data from nbadraft.net
"""

import requests
import re
import glob
import time
import os
import json
import bs4 as bs
import pandas as pd
import numpy as np

import career_stats

yearly_url = "https://www.nbadraft.net/actual-draft/?year-mock=%s"
CACHE_DIR = 'scrape_cache/nbadraft_net'
PLAYER_LIST_FILE = 'player_list.json'
SLEEP_TIME = 2 # seconds

NUMERIC_FIELDS = ['Athleticism', 'Size', 'Defense', 'Strength', 'Quickness', 
                  'Leadership', 'Jump Shot', 'NBA Ready', 'Rebounding', 'Potential',
                  'Post Skills', 'Intangibles', 'mock', 'big_board', 'overall', 
                  'Ball Handling', 'Passing']

def get_players_for_year(year):
    """
    gets info on all players drafted in specified year, 
    including link to their full profile.
    """
    url = yearly_url  % year
    r = requests.get(url)
    soup = bs.BeautifulSoup(r.content, 'html.parser')
    table_soup = soup.find(id='nba-mock-draft-content')

    round_one = soup.find(id="nba_consensus_mock1")
    round_two = soup.find(id="nba_consensus_mock2")

    players = []
    for round in [round_one, round_two]:
        # first row is a header
        player_rows = round.find_all("tr")[1:]

        for row in player_rows:
            cells = row.find_all('td')

            player_data = {}

            for cell in cells:
                attr = cell.attrs['class'][0]

                if attr == 'team':
                    ## need to strip non-ascii in team names
                    player_data[attr] = cell.text.encode('ascii', 'ignore').decode().replace('*', '')
                else:
                    player_data[attr] = cell.text
                # pick up URL to full player comp
                if attr == 'player':
                    player_data['player_url'] = cell.find('a')['href']

            player_data['year'] = year
            players.append(player_data)
    return players

def get_player_list_file():
    return f"{CACHE_DIR}/{PLAYER_LIST_FILE}"

def save_players(player_data):
    players_df = pd.DataFrame(player_data)
    players_df.to_json(get_player_list_file())

def load_players():
    return pd.read_json(get_player_list_file())

def scrape_player_lists(start=2009, end=2025):
    all_players = []
    year_range = range(start, end+1)
    for year in year_range:
        print(f"getting year {year}")
        year_players = get_players_for_year(year)
        all_players.extend(year_players)
        save_players(all_players)
        time.sleep(SLEEP_TIME)

### player page related functions
### eg https://www.nbadraft.net/players/nikola-jokic/
def get_player_filename(player_name):
    f_name = '-'.join(player_name.split(' '))
    return f"{CACHE_DIR}/players/{f_name}.json"

def get_name_from_filename(filename):
    return filename.replace(f"{CACHE_DIR}/players", "").replace("\\", "").replace(".json", "").replace("-", " ").strip()

def save_player(player_name, player_data):
    with open(get_player_filename(player_name), 'w') as f:
        json.dump(player_data, f)
    
def scrape_player_page(player_url):
    soup = fetch_player_page(player_url)
    player = extract_prose(soup)
    player.update(extract_numerics(soup))
    player.update(extract_ranks(soup))
    return player

def fetch_player_page(player_url):
    r = requests.get(player_url)
    soup = bs.BeautifulSoup(r.content, 'html.parser')
    return soup

def extract_numerics(soup):
    attrs = soup.find(class_="player-attributes").find_all(class_ = 'div-table-row')
    parsed_attrs = {}
    for attr in attrs:
        attr_name = attr.find(class_='attribute-name').text.strip()
        attr_value = attr.find(class_='attribute-value').text.strip()
        parsed_attrs[attr_name] = attr_value
    return parsed_attrs

def extract_ranks(soup):
    ranks = {}
    possible_mock = soup.find(class_='attribute-mock').find(class_='value')
    if possible_mock is not None:
        ranks['mock'] = possible_mock.text

    possible_big_board = soup.find(class_='attribute-big-board').find(class_='value')

    if possible_big_board is not None:
        ranks['big_board'] = possible_big_board.text

    possible_overall = soup.find(class_='attribute-overall').find(class_='value')
    if possible_overall is not None:
        ranks['overall'] = possible_overall.text

    return ranks

def extract_prose(soup):
    analysis = soup.find(id='analysis')

    player = {}
    descr_strengths = []
    descr_weaknesses = []
    descr_other = []

    paras = analysis.find(class_='vc_tta-panel-body').find_all(['p', 'h3'])

    got_comp = False

    for p in paras:
        # this is usually in its own paragraph

        if 'omparison:' in p.text:
            comp_chunk = p.text.split('omparison:')[1].strip()
            if "/" in comp_chunk:
                comps = comp_chunk.split("/")
            else:
                comps = [comp_chunk]

            player['nbadraft_net_comps'] = comps

            continue

        if len(p.text) < 100:
            pass
        else:
            # this HTML is an unholy mess on some pages, for instance https://www.nbadraft.net/players/ty-lawson/
            # the 'Strengths' parts aren't wrapped in <p> tags, so they don't get picked up.
            # it would be easy enough to do sentiment analysis, or have an LLM summarize
            if 'Strengths' in p.text:
                if 'Weaknesses' in p.text:
                    strength, weakness = p.text.split('Weaknesses')
                    descr_strengths.append(strength[len('Strengths:'):].strip())
                    descr_weaknesses.append(weakness[1:].strip())
                else:
                    descr_strengths.append(p.text[len('Strengths:'):].strip())

            elif 'Weaknesses' in p.text:
                descr_weaknesses.append(p.text[len('Weaknesses:'):].strip())
            else:
                descr_other.append(p.text)

    player['descr_other'] = descr_other
    player['descr_strengths'] = descr_strengths
    player['descr_weaknesses'] = descr_weaknesses
    player['descr_raw'] = str(analysis)

    return player


def scrape_all_players():
    data = load_players()
    for idx, row in data.iterrows():
        # see if we already have the player file
        player_filename = get_player_filename(row.player)
        if os.path.exists(player_filename):
            print("already got 'em")
        else:
            try:
                scraped = scrape_player_page(row.player_url)
                save_player(row.player, scraped)
                print(f"did {row.player} from {row.year}")
            except:
                print("error on %s" % row.player_url)
            # sleep between server requests.
            time.sleep(SLEEP_TIME)

def build_base_df():
    """
    Loop through all player files in the players/ directory, and build a pandas 
    dataframe from the data.
    """
    player_files = glob.glob("scrape_cache/nbadraft_net/players/*.json")

    players_extracted = []
    for filename in player_files:
        player_name = get_name_from_filename(filename)
        with open(filename, "r") as f:
            json_data = json.load(f)
            # these are lists, which make pandas mad if they are different lengths.
            # I am preserving the raw HTML. but this is primarily for numerical analysis
            del json_data['descr_other']
            del json_data['descr_strengths']
            del json_data['descr_weaknesses']
            json_data['Name'] = player_name
            players_extracted.append(json_data)
    df = pd.DataFrame(players_extracted)

    return df

def build_df():
    df = build_base_df()
    stats = career_stats.get_career_stats()

    df = df.merge(stats, how='left', left_on="Name", right_on="player")

    # fix columns that should be numeric.
    # these appear to be all integer values.
    # however, downcast='integer' doesn't work if there are na's.
    df[NUMERIC_FIELDS] = df[NUMERIC_FIELDS].apply(lambda x: pd.to_numeric(x, errors='coerce', downcast='integer'))

    return df

if __name__ == '__main__':
    print("main")
    #scrape_all_players()

    #scrape_player_lists()
    # import pprint
    # player = scrape_player_page("https://www.nbadraft.net/players/marvin-bagley/")
    # pprint.pprint(player)