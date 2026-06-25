import chromadb

import scrape_nbadraft_net

import time

import pandas as pd

import chromadb

summaries = pd.read_json("data/player_summaries-v2.json")
players = scrape_nbadraft_net.load_players()

def get_collection():
    client = chromadb.Client()
    collection = client.get_or_create_collection(name="my_collection")
    return collection

def query(collection, description):
    """
    given a summary description of a player, return a list of 
    similar players and whether they match strengths or weaknesses
    """
    search_results = collection.query(query_texts=[description])
    return search_results['ids']


def index_clean(collection):
    # TODO: try different embedding function (eg Qwen3)
    
    for idx, row in summaries.iterrows():
        _start = time.time()
        
        # FIXME: need to match on the ASCii-fied version of the name, because diacritics
        #player = players[players.player == row.Name]

        base_metadata = {'player_name': row.player_name}

        # TODO: add position on the court for sharper comps?
        # TODO: replace bullet points with sentences?

        metadata = base_metadata.copy()
        metadata['feedback_type'] = 'strengths'

        ids = []
        documents = []
        metadatas = []
    
        # try splitting up by bullet point
        bullets = row.strengths.split("\n")
        for counter, bullet in enumerate(bullets):
            if len(bullet.strip()) > 0:
                ids.append(f"{row.player_name}-strengths-{counter}")
                documents.append(bullet)
                metadatas.append(metadata)
                # collection.add(
                #     ids=[f"{row.player_name}-strengths-{counter}"],
                #     documents=[bullet],
                #     metadatas=[metadata]
                # )

        metadata = base_metadata.copy()
        metadata['feedback_type'] = 'weaknesses'
        
        bullets = row.strengths.split("\n")
        for counter, bullet in enumerate(bullets):
            if len(bullet.strip()) > 0:
                ids.append(f"{row.player_name}-weaknesses-{counter}")
                documents.append(bullet)
                metadatas.append(metadata)
                # collection.add(
                #     ids=[f"{row.player_name}-weaknesses-{counter}"],
                #     documents=[bullet],
                #     metadatas=[metadata]
                # )
        if len(ids) > 0:
            collection.add(ids=ids, documents=documents, metadatas=metadatas)
        print(f"did {row.player_name}, elapsed {time.time() - _start}")