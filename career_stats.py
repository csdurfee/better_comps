import kagglehub
import pandas as pd


"""
Using stats from here: https://www.kaggle.com/datasets/sumitrodatta/nba-aba-baa-stats?select=Advanced.csv

you can then do eg

>>> ws_per_48["Reggie Jackson"]
np.float64(0.07780751686960455)

there is a chance there is a name collision, would be better to group by player_id

however there will be small sample sizes. players who didn't play very much but had a high WS/48 in those minutes.
do I toss those out, or what? exclude to only players with > 500 minutes played or something

"""
def get_career_stats():
    stats_path = kagglehub.dataset_download("sumitrodatta/nba-aba-baa-stats")
    advanced = pd.read_csv(stats_path + "/Advanced.csv")
    #return advanced.groupby('player')['ws'].sum() / (advanced.groupby('player')['mp'].sum() / 48)
    medians = advanced.groupby('player')[['ows', 'dws', 'vorp', 'per']].median()
    sums = advanced.groupby('player')['g'].sum()

    return medians.merge(sums, left_on='player', right_on='player')