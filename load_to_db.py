from pymongo import MongoClient
from pymongo.errors import OperationFailure
import logging as log
import time
import pathlib as p
import os
import json
log.getLogger().setLevel(log.INFO)
log.basicConfig(format="%(asctime)s - [%(levelname)s]: %(message)s", datefmt="%H:%M:%S")


def insert_to_collection(host: str, port: int, file:list, collection: str):
    start = time.time()
    log.info(f"Connecting to the database")
    connection = MongoClient(host, port)
    try:
        connection.server_info()
        log.info(f"Connected successfully")
    except OperationFailure:
        log.error(f"Could not connect to db")

    with connection:
        log.info(f"Inserting data to {collection} collection")
        db = connection.Poland_spatial_data
        current_collection = db[collection]
        #current_collection.delete_many({})
        current_collection.insert_many(file)
        log.info(f"Data inserted successfully")
        time.sleep(1)
        end = time.time()
        log.info(f"Process of inserting data took {end - start} seconds")

def get_files(folder_name: str):
    folder_path: p.Path = p.Path.cwd().joinpath(folder_name)
    if not folder_path.exists():
        log.error(f"The folder {folder_name} does not exist")
    else:
        files = os.listdir(folder_path)
        node_files = [x for x in files if "_nodes.json" in x]
        way_files = [x for x in files if "_ways.json" in x]

        nodes_to_db = get_file_data(folder_path, node_files)
        insert_to_collection('localhost', 27017, nodes_to_db, 'nodes')
        ways_to_db = get_file_data(folder_path, way_files)
        insert_to_collection('localhost', 27017, ways_to_db, 'ways')

def get_file_data(folder_path: p.Path, file_list: list):
    all_nodal_data = []
    start = time.time()
    log.info(f"Reading file data")
    for element in file_list:
        file_path: p.Path = folder_path.joinpath(element)

        with file_path.open(mode="r", encoding="utf-8") as read_file:
            data = json.load(read_file)

        if not all_nodal_data:
            all_nodal_data = data
        else:
            #log.info(f"Length before inserting: {len(all_nodal_data)}")
            all_nodal_data = all_nodal_data + data
            #log.info(f"Length after inserting: {len(all_nodal_data)}")
    log.info(f"Data from files read successfully ")
    time.sleep(1)
    end = time.time()
    log.info(f"Process of reading data took {end - start} seconds")
    return all_nodal_data
if __name__ == "__main__":
    get_files("region_data")