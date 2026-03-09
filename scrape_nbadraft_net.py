"""
Scrapes all draft related data from nbadraft.net
"""

import requests
import re
import time
import bs4 as bs
import pandas as pd


yearly_url = "https://www.nbadraft.net/actual-draft/?year-mock=%s"
CACHE_DIR = 'scrape_cache'
PLAYER_CACHE_FILE = 'players.parquet'
SLEEP_TIME = 2 # seconds

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
                # pickup url to full player comp
                if attr == 'player':
                    player_data['player_url'] = cell.find('a')['href']

            player_data['year'] = year
            players.append(player_data)
    return players

def get_player_cache_file():
    return f"{CACHE_DIR}/{PLAYER_CACHE_FILE}"

def save_players(player_data):
    players_df = pd.DataFrame(player_data)
    players_df.to_parquet(get_player_cache_file())

def load_players():
    return pd.read_parquet(get_player_cache_file())

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
def scrape_player_page(player_url):
    soup = fetch_player_page(player_url)
    player = extract_prose(soup)
    numerics = extract_numerics(soup)
    player.update(numerics)
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

def extract_prose(soup):
    analysis = soup.find(id='analysis')

    player = {}
    descr_strengths = []
    descr_weaknesses = []
    descr_other = []

    paras = analysis.find(class_='vc_tta-panel-body').find_all(['p', 'h3'])

    got_comp = False
    ## sometimes comp is not in a paragraph

    for p in paras:
        if 'NBA Comparison:' in p.text:
            comp_chunks = p.text.split('NBA Comparison:')
            player['nbadraft_net_comp'] = comp_chunks[1].strip()
            continue

        if len(p.text) < 100:
            pass
        else:
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
    player['descr_raw'] = analysis

    return player

if __name__ == '__main__':
    import pprint
    nik = scrape_player_page("https://www.nbadraft.net/players/nikola-jokic/")
    pprint.pprint(nik)